// Copyright (c) YugabyteDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.
//

#include <gtest/gtest.h>

#include "yb/client/client.h"
#include "yb/master/catalog_manager.h"
#include "yb/master/master_types.pb.h"
#include "yb/yql/pgwrapper/libpq_utils.h"
#include "yb/yql/pgwrapper/pg_mini_test_base.h"
#include "yb/server/server_base.pb.h"

#include "yb/util/backoff_waiter.h"
#include "yb/util/test_macros.h"

using namespace std::chrono_literals;

DECLARE_uint32(TEST_abort_create_table);

namespace yb {

class CreateAbortTest : public pgwrapper::PgMiniTestBase {

 public:
  virtual size_t NumTabletServers() override {
    return 1;
  }

  // Returns true iff the table/index appears in the client's ListTables() output.
  bool TableExistsInClientList(const std::string& name) {
    return GetTableIDFromTableName(name).ok();
  }

  // Asserts that the given table/index exists or does not exist in the catalog manager
  // (via fetching TableInfo) and is or is not queryable via PG, according to should_exist.
  void AssertTableOrIndex(
      pgwrapper::PGConn* conn, const std::string& name, bool should_exist,
      const std::string& namespace_name = "yugabyte") {
    auto* cm = ASSERT_RESULT(catalog_manager_impl());
    auto table_info = cm->GetTableInfoFromNamespaceNameAndTableName(
        YQL_DATABASE_PGSQL, namespace_name, name);
    const bool exists_in_catalog = table_info != nullptr;
    if (should_exist) {
      ASSERT_TRUE(exists_in_catalog)
          << "Table/index '" << name << "' should exist in catalog";
      ASSERT_OK(conn->Execute("SELECT 1 FROM " + name + " LIMIT 1"));
    } else {
      ASSERT_FALSE(exists_in_catalog)
          << "Table/index '" << name << "' should not exist in catalog";
      auto s = conn->Execute("SELECT 1 FROM " + name + " LIMIT 1");
      ASSERT_NOK(s) << "Query on '" << name << "' should fail";
      ASSERT_STR_CONTAINS(s.ToString(), "does not exist");
    }
  }

  void AssertTableOrIndexExists(
      pgwrapper::PGConn* conn, const std::string& name,
      const std::string& namespace_name = "yugabyte") {
    AssertTableOrIndex(conn, name, true /* should_exist */, namespace_name);
  }

  void AssertTableOrIndexDoesNotExist(
      pgwrapper::PGConn* conn, const std::string& name,
      const std::string& namespace_name = "yugabyte") {
    AssertTableOrIndex(conn, name, false /* should_exist */, namespace_name);
  }

};

TEST_F(CreateAbortTest, TestAbortTableCreation) {

  // Create a client to access the cluster.
  auto client = ASSERT_RESULT(cluster_->CreateClient());

  LOG(INFO) << "INFO_A: Initial cluster state:number of tablet servers: "
    << cluster_->num_tablet_servers()
    << ", test flag: " << FLAGS_TEST_abort_create_table;

  // test assumptions
  ASSERT_EQ(cluster_->num_tablet_servers(), 1);
  ASSERT_EQ(FLAGS_TEST_abort_create_table, 0);

  // Wait for the global transaction status table to be created.
  // The table is created during master init on first table creation.
  ASSERT_OK(WaitFor([&]() -> Result<bool> {
    auto result = client->GetTransactionStatusTablets(CloudInfoPB());
    if (!result.ok()) {
      return false;
    }
    auto txn_tablets = *result;
    return !txn_tablets.global_tablets.empty();
  }, MonoDelta::FromSeconds(30),
  "Waiting for transaction status table to be created"));

  LOG(INFO) << "INFO_A: DONE waiting for transaction status table to be created";

  auto conn = ASSERT_RESULT(Connect());

  // test flag is 0 by default => normal state, create table/index should not fail
  ASSERT_OK(conn.Execute("CREATE TABLE test_create_abort_t1 (k1 int primary key, k2 int)"));

  // create table should abort if the test flag is 1
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_abort_create_table) = 1;
  auto s = conn.Execute("CREATE TABLE test_create_abort_t2 (k1 int primary key, k2 int)");
  ASSERT_NOK(s);
  ASSERT_STR_CONTAINS(s.ToString(), "TEST: Aborting due to FLAGS_TEST_abort_create_table");
  AssertTableOrIndexDoesNotExist(&conn, "test_create_abort_t2");
  ASSERT_FALSE(TableExistsInClientList("test_create_abort_t2"));

  // create index should abort if the test flag is 1
  s = conn.Execute("CREATE INDEX test_create_abort_t1_idx1 ON test_create_abort_t1 (k2)");
  ASSERT_NOK(s);
  ASSERT_STR_CONTAINS(s.ToString(), "TEST: Aborting due to FLAGS_TEST_abort_create_table");
  AssertTableOrIndexDoesNotExist(&conn, "test_create_abort_t1_idx1");
  ASSERT_FALSE(TableExistsInClientList("test_create_abort_t1_idx1"));

  // the test flag only applies to table names that start with 'test_creare_abort_' prefix
  // and should not be impact any other table.
  ASSERT_OK(conn.Execute("CREATE TABLE rand_table (k1 int primary key, k2 int)"));
  ASSERT_OK(conn.Execute("CREATE INDEX rand_table_idx ON rand_table (k2)"));

  // create table should not abort if the test flag is 2
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_abort_create_table) = 2;
  ASSERT_OK(conn.Execute("CREATE TABLE test_create_abort_t3 (k1 int primary key, k2 int)"));
  // but create index should abort if the test flag is 2
  s = conn.Execute("CREATE INDEX test_create_abort_t3_idx1 ON test_create_abort_t3 (k2)");
  ASSERT_NOK(s);
  ASSERT_STR_CONTAINS(s.ToString(), "TEST: Aborting due to FLAGS_TEST_abort_create_table");
  AssertTableOrIndexDoesNotExist(&conn, "test_create_abort_t3_idx1");
  ASSERT_FALSE(TableExistsInClientList("test_create_abort_t3_idx1"));

  // create index should abort if the test flag is 3
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_abort_create_table) = 3;
  s = conn.Execute("CREATE INDEX test_create_abort_t3_idx1 ON test_create_abort_t3 (k2)");
  ASSERT_NOK(s);
  ASSERT_STR_CONTAINS(s.ToString(), "TEST: Aborting due to FLAGS_TEST_abort_create_table");
  AssertTableOrIndexDoesNotExist(&conn, "test_create_abort_t3_idx1");
  ASSERT_FALSE(TableExistsInClientList("test_create_abort_t3_idx1"));

  // restore things back
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_abort_create_table) = 0;
  // sanity-check create index works again since test flag 0
  ASSERT_OK(conn.Execute("CREATE INDEX test_create_abort_t3_idx1 ON test_create_abort_t3 (k2)"));
  ASSERT_OK(conn.Execute("DROP INDEX test_create_abort_t3_idx1"));
  ASSERT_OK(conn.Execute("DROP TABLE test_create_abort_t1"));
  ASSERT_OK(conn.Execute("DROP TABLE test_create_abort_t3"));
  ASSERT_OK(conn.Execute("DROP INDEX rand_table_idx"));
  ASSERT_OK(conn.Execute("DROP TABLE rand_table"));

  // sleep for 10?? seconds.
  // std::this_thread::sleep_for(10s);
  LOG(INFO) << "INFO_A: DONE";

}

}  // namespace yb
