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

#include <algorithm>

#include <gtest/gtest.h>

#include "yb/integration-tests/mini_cluster.h"
#include "yb/integration-tests/xcluster/xcluster_test_base.h"
#include "yb/integration-tests/yb_mini_cluster_test_base.h"

#include "yb/client/client.h"
#include "yb/yql/pgwrapper/pg_mini_test_base.h"
#include "yb/server/server_base.pb.h"
#include "yb/server/server_base.proxy.h"
#include "yb/rpc/rpc_controller.h"
#include "yb/rpc/proxy.h"
#include "yb/rpc/messenger.h"
#include "yb/gutil/strtoint.h"
#include "yb/common/entity_ids.h"

#include "yb/master/catalog_manager.h"
#include "yb/master/master.h"
#include "yb/master/master_ddl.proxy.h"
#include "yb/master/master_ddl_client.h"
#include "yb/master/master_cluster_client.h"
#include "yb/master/mini_master.h"
#include "yb/master/test_async_rpc_manager.h"

#include "yb/tserver/mini_tablet_server.h"
#include "yb/tserver/tablet_server.h"

#include "yb/util/backoff_waiter.h"
#include "yb/util/countdown_latch.h"
#include "yb/util/logging_test_util.h"
#include "yb/util/status_callback.h"
#include "yb/util/test_macros.h"
#include "yb/util/unique_lock.h"

using namespace std::chrono_literals;

DECLARE_int32(retrying_ts_rpc_max_delay_ms);
DECLARE_int32(retrying_rpc_max_jitter_ms);
DECLARE_int32(transaction_status_check_interval_sec);
DECLARE_int32(transaction_table_num_tablets);
DECLARE_int32(transaction_table_num_tablets_per_tserver);
DECLARE_int32(TEST_transaction_status_check_run_count);
DECLARE_bool(TEST_abort_create_pg_auto_analyze_table);
DECLARE_int32(tserver_unresponsive_timeout_ms);
DECLARE_bool(auto_create_local_transaction_tables);
DECLARE_bool(TEST_name_transaction_tables_with_tablespace_id);
DECLARE_int32(replication_factor);
DECLARE_bool(enable_load_balancing);
DECLARE_bool(autoscale_transaction_tables);

namespace yb {

class CreateAbortTest : public pgwrapper::PgMiniTestBase {

 public:
  virtual size_t NumTabletServers() override {
    return 1;
  }

  void SetUp() override {
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_abort_create_pg_auto_analyze_table) = true;
    pgwrapper::PgMiniTestBase::SetUp();
  }

};

TEST_F(CreateAbortTest, ReproAttempt1) {

  // Create a client to access the cluster.
  auto client = ASSERT_RESULT(cluster_->CreateClient());

  LOG(INFO) << "INFO_A: Initial cluster state:number of tablet servers: "
    << cluster_->num_tablet_servers()
    << ", test flag: " << FLAGS_auto_create_local_transaction_tables;

  // test assumptions
  ASSERT_EQ(cluster_->num_tablet_servers(), 1);

  // Wait for the global transaction status table to be created.
  // The table is created during master init.
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

  // wait until the pg auto analyze table creation is aborted
  // this is indicated by a log message
  auto log_waiter = RegexWaiterLogSink(
    R"#(.*TEST: Aborting due to FLAGS_TEST_abort_create_pg_auto_analyze_table.*)#");
  ASSERT_OK(log_waiter.WaitFor(30s));

  // sleep for 10?? seconds.
  // std::this_thread::sleep_for(10s);
  LOG(INFO) << "INFO_A: DONE";

}

}  // namespace yb
