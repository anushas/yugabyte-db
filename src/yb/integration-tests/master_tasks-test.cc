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

#include "yb/master/catalog_manager.h"
#include "yb/master/master.h"
#include "yb/master/master_ddl.proxy.h"
#include "yb/master/master_ddl_client.h"
#include "yb/master/mini_master.h"
#include "yb/master/test_async_rpc_manager.h"

#include "yb/tserver/mini_tablet_server.h"
#include "yb/tserver/tablet_server.h"

#include "yb/util/backoff_waiter.h"
#include "yb/util/status_callback.h"
#include "yb/util/test_macros.h"

using namespace std::chrono_literals;

DECLARE_int32(retrying_ts_rpc_max_delay_ms);
DECLARE_int32(retrying_rpc_max_jitter_ms);
DECLARE_int32(transaction_status_check_interval_sec);
DECLARE_int32(transaction_table_num_tablets);
DECLARE_int32(transaction_table_num_tablets_per_tserver);
DECLARE_int32(TEST_transaction_status_check_run_count);
DECLARE_bool(auto_create_local_transaction_tables);
DECLARE_bool(TEST_name_transaction_tables_with_tablespace_id);
DECLARE_int32(replication_factor);
DECLARE_bool(enable_load_balancing);
DECLARE_bool(autoscale_transaction_tables);

namespace yb {

class MasterTasksTest : public YBMiniClusterTestBase<MiniCluster> {
 public:
  MasterTasksTest() {}

  void SetUp();

  virtual MiniClusterOptions GetMiniClusterOptions();

  Result<TableId> CreateTable(
      master::MasterDDLClient& client, const NamespaceName& namespace_name,
      const TableName& table_name, const Schema& schema);
};

// Test that retrying master and tserver rpc tasks retry properly and that the delay before retrying
// is capped by FLAGS_retrying_ts_rpc_max_delay_ms + up to 50ms random jitter per retry.
TEST_F(MasterTasksTest, RetryingMasterRpcTaskMaxDelay) {
  constexpr auto kNumRetries = 10;

  auto* leader_master = ASSERT_RESULT(cluster_->GetLeaderMiniMaster());
  std::vector<consensus::RaftPeerPB> master_peers;
  ASSERT_OK(leader_master->master()->ListRaftConfigMasters(&master_peers));

  // Send the RPC to a non-leader master.
  auto non_leader_master = std::find_if(
      master_peers.begin(), master_peers.end(),
      [&](auto& peer) { return peer.permanent_uuid() != leader_master->permanent_uuid(); });
  ASSERT_NE(non_leader_master, master_peers.end()) << "Failed to find non-leader master";
  std::promise<Status> promise;
  std::future<Status> future = promise.get_future();
  ASSERT_OK(leader_master->master()->test_async_rpc_manager()->SendMasterTestRetryRequest(
      std::move(*non_leader_master), kNumRetries, [&promise](const Status& s) {
        LOG(INFO) << "Done: " << s;
        promise.set_value(s);
      }));

  LOG(INFO) << "Task scheduled";

  auto status = future.wait_for(
      (FLAGS_retrying_ts_rpc_max_delay_ms + FLAGS_retrying_rpc_max_jitter_ms) *
      kNumRetries * RegularBuildVsSanitizers(1.1, 1.2) * 1ms);
  ASSERT_EQ(status, std::future_status::ready);
  ASSERT_OK(future.get());
}

TEST_F(MasterTasksTest, RetryingTSRpcTaskMaxDelay) {
  constexpr auto kNumRetries = 10;

  ANNOTATE_UNPROTECTED_WRITE(FLAGS_retrying_ts_rpc_max_delay_ms) = 100;

  auto* ts = cluster_->mini_tablet_server(0);

  auto* leader_master = ASSERT_RESULT(cluster_->GetLeaderMiniMaster());

  std::promise<Status> promise;
  std::future<Status> future = promise.get_future();
  ASSERT_OK(leader_master->master()->test_async_rpc_manager()->SendTsTestRetryRequest(
    ts->server()->permanent_uuid(), kNumRetries, [&promise](const Status& s) {
      LOG(INFO) << "Done: " << s;
      promise.set_value(s);
  }));

  LOG(INFO) << "Task scheduled";

  auto status = future.wait_for(
      (FLAGS_retrying_ts_rpc_max_delay_ms + FLAGS_retrying_rpc_max_jitter_ms) *
      kNumRetries * RegularBuildVsSanitizers(1.1, 1.2) * 1ms);
  ASSERT_EQ(status, std::future_status::ready);
  ASSERT_OK(future.get());
}

class SingleMasterTasksTest : public MasterTasksTest {
 public:
  MiniClusterOptions GetMiniClusterOptions() override;
};

class DummyTask : public master::RetryingTSRpcTaskWithTable {
 public:
  DummyTask(
      master::Master* master, ThreadPool* callback_pool,
      AsyncTaskThrottlerBase* async_task_throttler, scoped_refptr<master::TableInfo> table,
      const std::string& uuid, const TabletId& tablet_id);

 public:
  server::MonitoredTaskType type() const override;

  std::string type_name() const override;

  std::string description() const override;

  bool finish_called_ = false;

 protected:
  bool SendRequest(int attempt) override;

  void HandleResponse(int attempt) override;

  Status ResetProxies() override;

  void DoRpcCallback() override;

  void Finished(const Status& status) override;

  TabletId tablet_id() const override;

 private:
  master::Master* master_;
  TableId table_id_;
  TabletId tablet_id_;
};

TEST_F(SingleMasterTasksTest, SkipCallbacksWhenReloadingSysCatalog) {
  const std::string kTableName("test_table");
  const std::string kNamespaceName("yugabyte");
  const Schema kTableSchema({ColumnSchema("key", DataType::INT32, ColumnKind::HASH)});

  auto ddl_client = master::MasterDDLClient(
      ASSERT_RESULT(cluster_->GetLeaderMasterProxy<master::MasterDdlProxy>()));
  auto ns_id =
      ASSERT_RESULT(ddl_client.CreateNamespace(kNamespaceName, YQLDatabase::YQL_DATABASE_CQL));
  ASSERT_OK(ddl_client.WaitForCreateNamespaceDone(ns_id, MonoDelta::FromSeconds(60)));
  auto table_id = ASSERT_RESULT(CreateTable(ddl_client, kNamespaceName, kTableName, kTableSchema));
  ASSERT_OK(ddl_client.WaitForCreateTableDone(table_id, MonoDelta::FromSeconds(60)));
  auto leader_mini_master = ASSERT_RESULT(cluster_->GetLeaderMiniMaster());
  auto original_leader_uuid = leader_mini_master->permanent_uuid();
  auto& catalog_mgr = leader_mini_master->catalog_manager_impl();
  auto table = ASSERT_RESULT(catalog_mgr.FindTableById(table_id));
  auto tablets = ASSERT_RESULT(table->GetTablets());
  ASSERT_GT(tablets.size(), 0);

  auto task = std::make_shared<DummyTask>(
      leader_mini_master->master(), catalog_mgr.AsyncTaskPool(), nullptr, table,
      cluster_->mini_tablet_server(0)->server()->permanent_uuid(), tablets[0]->id());
  table->AddTask(task);
  {
    // Step down the master leader to trigger a catalog reload.
    auto leader_epoch = catalog_mgr.GetLeaderEpochInternal();
    auto new_leader_id = ASSERT_RESULT(cluster_->StepDownMasterLeader(original_leader_uuid));
    // Wait until the term has been incremented to be sure the catalog reload has finished and all
    // synchronous logic executed during task aborts has been executed.
    ASSERT_OK(WaitFor(
        [&]() -> Result<bool> {
          auto maybe_leader = VERIFY_RESULT(cluster_->GetLeaderMiniMaster());
          if (maybe_leader == nullptr) {
            return false;
          }
          return maybe_leader->catalog_manager_impl().GetLeaderEpochInternal().leader_term >
                 leader_epoch.leader_term;
        },
        MonoDelta::FromSeconds(60), "Waiting for new leader"));
  }
  ASSERT_FALSE(task->finish_called_);
}

void MasterTasksTest::SetUp() {
  YBMiniClusterTestBase::SetUp();
  auto opts = GetMiniClusterOptions();
  cluster_ = std::make_unique<MiniCluster>(opts);
  ASSERT_OK(cluster_->Start());

  ASSERT_OK(cluster_->WaitForTabletServerCount(opts.num_tablet_servers));
}

MiniClusterOptions MasterTasksTest::GetMiniClusterOptions() {
  MiniClusterOptions opts;
  opts.num_tablet_servers = 1;
  opts.num_masters = 3;
  return opts;
}

Result<TableId> MasterTasksTest::CreateTable(
    master::MasterDDLClient& client, const NamespaceName& namespace_name,
    const TableName& table_name, const Schema& schema) {
  master::CreateTableRequestPB request;
  request.set_name(table_name);
  SchemaToPB(schema, request.mutable_schema());
  if (!namespace_name.empty()) {
    request.mutable_namespace_()->set_name(namespace_name);
  }
  request.mutable_partition_schema()->set_hash_schema(PartitionSchemaPB::MULTI_COLUMN_HASH_SCHEMA);
  request.mutable_schema()->mutable_table_properties()->set_num_tablets(1);
  return client.CreateTable(request);
}

MiniClusterOptions SingleMasterTasksTest::GetMiniClusterOptions() {
  MiniClusterOptions opts;
  opts.num_tablet_servers = 1;
  opts.num_masters = 1;
  return opts;
}

DummyTask::DummyTask(
    master::Master* master, ThreadPool* callback_pool, AsyncTaskThrottlerBase* async_task_throttler,
    scoped_refptr<master::TableInfo> table, const std::string& uuid, const TabletId& tablet_id)
    : master::RetryingTSRpcTaskWithTable(
          master, callback_pool, std::make_unique<master::PickSpecificUUID>(master, uuid), table,
          master::LeaderEpoch(0, 1), async_task_throttler),
      master_(master),
      table_id_(table->id()),
      tablet_id_(tablet_id) {}

server::MonitoredTaskType DummyTask::type() const {
  return server::MonitoredTaskType::kTestRetryTs;
}

std::string DummyTask::type_name() const { return "DummyTask"; }

std::string DummyTask::description() const { return "DummyTask"; }

bool DummyTask::SendRequest(int attempt) { return true; }

void DummyTask::HandleResponse(int attempt) { TransitionToCompleteState(); }

Status DummyTask::ResetProxies() { return Status::OK(); }

void DummyTask::DoRpcCallback() {}

void DummyTask::Finished(const Status& status) {
  // We call FindTableById because it acquires the catalog manager mutex.
  auto _unused = master_->catalog_manager_impl()->FindTableById(table_id_);
  finish_called_ = true;
}

TabletId DummyTask::tablet_id() const { return tablet_id_; }

class MasterTasksTest2 : public pgwrapper::PgMiniTestBase {

 public:
  virtual size_t NumTabletServers() override {
    return 1;
  }

  void SetUp() override {
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_check_broadcast_address) = false;
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_enable_load_balancing) = false;
    pgwrapper::PgMiniTestBase::SetUp();
  }

 protected:

  // Helper to get tablespace OID from PostgreSQL
  Result<uint32_t> GetTablespaceOid(const std::string& tablespace_name) {
    auto conn = VERIFY_RESULT(Connect());
    auto result = VERIFY_RESULT(conn.FetchFormat(
        "SELECT oid FROM pg_tablespace WHERE spcname = '$0'", tablespace_name));
    if (PQntuples(result.get()) == 0) {
      return STATUS_FORMAT(NotFound, "Tablespace $0 not found", tablespace_name);
    }
    return VERIFY_RESULT(pgwrapper::GetValue<pgwrapper::PGOid>(result.get(), 0, 0));
  }

  // Helper to create a tablespace and return its OID
  Result<uint32_t> CreateTablespaceAndGetOid(
      const std::string& tablespace_name,
      const std::string& cloud = "test_cloud",
      const std::string& region = "test_region",
      const std::string& zone = "zone2",
      int num_replicas = 1) {
    auto conn = VERIFY_RESULT(Connect());
    RETURN_NOT_OK(conn.ExecuteFormat(R"#(
        CREATE TABLESPACE $0 WITH (replica_placement='{
          "num_replicas": $1,
          "placement_blocks": [{
            "cloud": "$2",
            "region": "$3",
            "zone": "$4",
            "min_num_replicas": $1
          }]
        }')
      )#", tablespace_name, num_replicas, cloud, region, zone));
    return GetTablespaceOid(tablespace_name);
  }

  // Helper to get current transaction status table version
  Result<uint64_t> GetCurrentTransactionTablesVersion() {
    auto* leader_master = VERIFY_RESULT(cluster_->GetLeaderMiniMaster());
    return leader_master->catalog_manager_impl().GetTransactionTablesVersion();
  }

  // Helper to verify number of tablets for global and local transaction status tables
  // using their table info.
  Status VerifyTransactionTableTablets(
      const std::string& local_txnstatus_name,
      size_t expected_global_tablets,
      size_t expected_local_tablets) {
    auto cm = VERIFY_RESULT(catalog_manager());

    // Get global transactions table info
    auto table_id = VERIFY_RESULT(GetTableIDFromTableName("transactions"));
    auto global_table_info = VERIFY_RESULT(cm->FindTableById(table_id));
    LOG(INFO) << "INFO_A: Global table info: " << global_table_info->name()
              << ", table ID: " << global_table_info->id()
              << ", tablets: " << global_table_info->TabletCount();

    // Get local transactions table info
    table_id = VERIFY_RESULT(GetTableIDFromTableName(local_txnstatus_name));
    auto local_table_info = VERIFY_RESULT(cm->FindTableById(table_id));
    LOG(INFO) << "INFO_A: Local Table info: " << local_table_info->name()
              << ", table ID: " << local_table_info->id()
              << ", tablets: " << local_table_info->TabletCount();

    // Verify tablet counts
    SCHECK_EQ(global_table_info->TabletCount(), expected_global_tablets, IllegalState,
              Format("Global transaction table tablet count mismatch. Expected: $0, Got: $1",
                     expected_global_tablets, global_table_info->TabletCount()));
    SCHECK_EQ(local_table_info->TabletCount(), expected_local_tablets, IllegalState,
              Format("Local transaction table tablet count mismatch. Expected: $0, Got: $1",
                     expected_local_tablets, local_table_info->TabletCount()));

    return Status::OK();
  }

  // Helper to verify the tablets available for a placement using the client API
  Status VerifyTransactionStatusTabletsFromClient(
      const CloudInfoPB& cloud_info,
      size_t expected_global_tablets,
      size_t expected_local_tablets,
      const std::string& log_prefix) {
    auto client = VERIFY_RESULT(cluster_->CreateClient());
    auto txn_tablets = VERIFY_RESULT(client->GetTransactionStatusTablets(cloud_info));
    size_t global_tablet_count = txn_tablets.global_tablets.size();
    size_t local_tablet_count = txn_tablets.region_local_tablets.size();

    LOG(INFO) << "INFO_A: " << log_prefix << " Global tablet count: " << global_tablet_count
              << ", " << log_prefix << " Local tablet count: " << local_tablet_count;

    SCHECK_EQ(global_tablet_count, expected_global_tablets, IllegalState,
              Format("Global transaction tablet count mismatch. Expected: $0, Got: $1",
                     expected_global_tablets, global_tablet_count));
    SCHECK_EQ(local_tablet_count, expected_local_tablets, IllegalState,
              Format("Local transaction tablet count mismatch. Expected: $0, Got: $1",
                     expected_local_tablets, local_tablet_count));

    return Status::OK();
  }

  // Helper to wait for transaction status tablets to reach expected counts
  Status WaitForTransactionStatusTablets(
      const CloudInfoPB& cloud_info,
      size_t expected_global_tablets,
      size_t expected_local_tablets,
      MonoDelta timeout = MonoDelta::FromSeconds(30)) {
    auto client = VERIFY_RESULT(cluster_->CreateClient());
    return WaitFor([&]() -> Result<bool> {
      auto result = client->GetTransactionStatusTablets(cloud_info);
      if (!result.ok()) {
        return false;
      }
      auto txn_tablets = *result;
      LOG(INFO) << "INFO_A: global tablet count: " << txn_tablets.global_tablets.size()
                << ", local tablet count: " << txn_tablets.region_local_tablets.size();
      return txn_tablets.global_tablets.size() == expected_global_tablets &&
             txn_tablets.region_local_tablets.size() == expected_local_tablets;
    }, timeout, "INFO_A: Waiting for tablet server to finish creating tablets");
  }

  // Helper to sleep and verify background task run count (test flag)
  void WaitAndVerifyBackgroundTaskRuns(
      int32_t expected_min_run_count,
      const std::string& tag = "") {
    SleepFor(MonoDelta::FromSeconds(FLAGS_transaction_status_check_interval_sec * 3 + 2));
    ASSERT_GE(FLAGS_TEST_transaction_status_check_run_count, expected_min_run_count)
      << tag << ": Background task should have run at least " << expected_min_run_count
      << " times but got " << FLAGS_TEST_transaction_status_check_run_count;
  }
};

// Test that the transaction status check background task runs
// periodically and logs expected info for local transaction status tables.
TEST_F(MasterTasksTest2, TransactionStatusCheckBackgroundTask) {
  ASSERT_TRUE(FLAGS_autoscale_transaction_tables)
    << "autoscale_transaction_tables must be enabled (default is true) for this test";

  // Set a short interval for the transaction status check (2 seconds)
  // to make the test faster.
  int32_t original_transaction_status_check_interval_sec
    = FLAGS_transaction_status_check_interval_sec;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_transaction_status_check_interval_sec) = 2;

  // Reset transaction_table_num_tablets to test auto scaling (up) of
  // transaction status tables.
  int32_t original_transaction_table_num_tablets = FLAGS_transaction_table_num_tablets;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_transaction_table_num_tablets) = 0;

  // Set transaction_table_num_tablets_per_tserver to a known value
  // to make this test deterministic.
  int32_t original_transaction_table_num_tablets_per_tserver
    = FLAGS_transaction_table_num_tablets_per_tserver;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_transaction_table_num_tablets_per_tserver) = 2;

  // Enable auto_create_local_transaction_tables.
  bool original_auto_create_local_transaction_tables
    = FLAGS_auto_create_local_transaction_tables;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_auto_create_local_transaction_tables) = true;

  // Enable name_transaction_tables_with_tablespace_id to make this
  // test deterministic.
  bool original_name_transaction_tables_with_tablespace_id
    = FLAGS_TEST_name_transaction_tables_with_tablespace_id;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_name_transaction_tables_with_tablespace_id) = true;

  // Create a client to access the cluster.
  auto client = ASSERT_RESULT(cluster_->CreateClient());

  LOG(INFO) << "INFO_A: Initial cluster state:number of tablet servers: "
    << cluster_->num_tablet_servers()
    << ", replication factor: " << FLAGS_replication_factor
    << ", " << FLAGS_TEST_check_broadcast_address
    << ", " << FLAGS_enable_load_balancing;

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

  // background task runs once each time tserver joins
  WaitAndVerifyBackgroundTaskRuns(1, "init");
  // Note placement info from the initial tserver (default placement)
  auto* initial_tserver = cluster_->mini_tablet_server(0);
  std::string initial_cloud = initial_tserver->options()->placement_cloud();
  std::string initial_region = initial_tserver->options()->placement_region();
  std::string initial_zone = initial_tserver->options()->placement_zone();

  CloudInfoPB initial_cloud_info;
  initial_cloud_info.set_placement_cloud(initial_cloud);
  initial_cloud_info.set_placement_region(initial_region);
  initial_cloud_info.set_placement_zone(initial_zone);

  LOG(INFO) << "INFO_A: Initial cloud: " << initial_cloud
            << ", Initial region: " << initial_region
            << ", Initial zone: " << initial_zone
            << ", Global transactions Table ID: " << GetTableIDFromTableName("transactions");

  // (1) Create a local transaction status table

  // A new placement (zone2)
  CloudInfoPB zone2_cloud_info;
  zone2_cloud_info.set_placement_cloud("test_cloud");
  zone2_cloud_info.set_placement_region("test_region");
  zone2_cloud_info.set_placement_zone("zone2");

  // 1.1 Create a tablespace for the new placement (zone2) using SQL
  std::string tablespace_name = "tablespace_zone2";
  auto tablespace_oid = ASSERT_RESULT(CreateTablespaceAndGetOid(tablespace_name));

  // 1.2 Add a new tablet server in the new placement (zone2).
  // tserver must already be started before we create the
  // local transaction status table (else creation will fail).
  auto ts_opts_new = ASSERT_RESULT(tserver::TabletServerOptions::CreateTabletServerOptions());
  ts_opts_new.SetPlacement("test_cloud", "test_region", "zone2");
  ASSERT_OK(cluster_->AddTabletServer(ts_opts_new, true));
  ASSERT_OK(cluster_->WaitForTabletServerCount(2));

  auto initial_version = ASSERT_RESULT(GetCurrentTransactionTablesVersion());
  // 1.3 create a test table in the tablespace, this will trigger the creation
  // of the local transaction status table transactions_<tablespace_oid>
  auto conn = ASSERT_RESULT(Connect());
  ASSERT_OK(conn.ExecuteFormat(
      "CREATE TABLE test_table_in_zone2 (key INT PRIMARY KEY) TABLESPACE $0",
      tablespace_name));

  // local transaction status table creation would bump the transaction table version
  auto current_version = ASSERT_RESULT(GetCurrentTransactionTablesVersion());
  ASSERT_GE(current_version, initial_version+1);

  std::string local_txnstatus_name  = "transactions_" + std::to_string(tablespace_oid);
  // find the name of the transaction status table that was created
  LOG(INFO) << "INFO_A: Local transaction status table created "
            << local_txnstatus_name << "[ "
            << GetTableIDFromTableName(local_txnstatus_name) << "]";

  // background task runs once more when tserver (zone2) joins
  WaitAndVerifyBackgroundTaskRuns(2, "after tserver1 (zone2) joins");

  // verify the tablets are as expected. This table info based check
  // is guaranteed to work since the background task has already added
  // the tablets by now. But the tablets may not be physcially created
  // yet.
  ASSERT_OK(VerifyTransactionTableTablets(
      local_txnstatus_name,
      cluster_->num_tablet_servers() * FLAGS_transaction_table_num_tablets_per_tserver,
      1 * FLAGS_transaction_table_num_tablets_per_tserver));

  // wait until the tablet server finishes creating the tablets.
  // GetTransactionStatusTablets will return service not available
  // until the tablets are created.
  // RESOLVE:Technically this is not the background task's work,
  // but nice to test. This is done asynchronously after tablet
  // is added. It could take indeterminate time to finish.
  // This sometimes runs into issues if there's a tablet
  // movement. So turned off the load balancer.
  // Need to ensure this check will not be flaky.
  ASSERT_OK(WaitForTransactionStatusTablets(
      zone2_cloud_info,
      2 * FLAGS_transaction_table_num_tablets_per_tserver,
      1 * FLAGS_transaction_table_num_tablets_per_tserver));

  // verify the number of tablets available for global and local transaction
  // status tables for all the 3 placements is as expected. This is the API
  // used by transaction coordinator.
  ASSERT_OK(VerifyTransactionStatusTabletsFromClient(
      initial_cloud_info,
      cluster_->num_tablet_servers() * FLAGS_transaction_table_num_tablets_per_tserver,
      0,
      "Original"));

  ASSERT_OK(VerifyTransactionStatusTabletsFromClient(
      zone2_cloud_info,
      cluster_->num_tablet_servers() * FLAGS_transaction_table_num_tablets_per_tserver,
      1 * FLAGS_transaction_table_num_tablets_per_tserver,
      "Zone2"));

  CloudInfoPB zone3_cloud_info;
  zone3_cloud_info.set_placement_cloud("test_cloud");
  zone3_cloud_info.set_placement_region("test_region");
  zone3_cloud_info.set_placement_zone("zone3");
  ASSERT_OK(VerifyTransactionStatusTabletsFromClient(
      zone3_cloud_info,
      cluster_->num_tablet_servers() * FLAGS_transaction_table_num_tablets_per_tserver,
      0,
      "Zone3"));

  // (2) Add another tablet server, but to a different placement (zone3)
  auto ts_opts_new3 = ASSERT_RESULT(tserver::TabletServerOptions::CreateTabletServerOptions());
  ts_opts_new3.SetPlacement("test_cloud", "test_region", "zone3");
  ASSERT_OK(cluster_->AddTabletServer(ts_opts_new3, true));
  ASSERT_OK(cluster_->WaitForTabletServerCount(3));

  // verify background task ran once more.
  WaitAndVerifyBackgroundTaskRuns(3, "after tserver3 (zone3) joins");

  ASSERT_OK(VerifyTransactionTableTablets(
      local_txnstatus_name,
      cluster_->num_tablet_servers() * FLAGS_transaction_table_num_tablets_per_tserver,
      1 * FLAGS_transaction_table_num_tablets_per_tserver));

  // wait until the tablet server finishes creating the tablets
  ASSERT_OK(WaitForTransactionStatusTablets(
      zone3_cloud_info,
      3 * FLAGS_transaction_table_num_tablets_per_tserver,
      0));

  // verify the number of tablets for all the 3 placements
  ASSERT_OK(VerifyTransactionStatusTabletsFromClient(
      initial_cloud_info,
      cluster_->num_tablet_servers() * FLAGS_transaction_table_num_tablets_per_tserver,
      0,
      "Original"));

  ASSERT_OK(VerifyTransactionStatusTabletsFromClient(
      zone2_cloud_info,
      cluster_->num_tablet_servers() * FLAGS_transaction_table_num_tablets_per_tserver,
      1 * FLAGS_transaction_table_num_tablets_per_tserver,
      "Zone2"));

  ASSERT_OK(VerifyTransactionStatusTabletsFromClient(
      zone3_cloud_info,
      cluster_->num_tablet_servers() * FLAGS_transaction_table_num_tablets_per_tserver,
      0,
      "Zone3"));

  // (3) Add another tablet server, but to the same placement (zone2)
  auto ts_opts_new4 = ASSERT_RESULT(tserver::TabletServerOptions::CreateTabletServerOptions());
  ts_opts_new4.SetPlacement("test_cloud", "test_region", "zone2");
  ASSERT_OK(cluster_->AddTabletServer(ts_opts_new4, true));
  ASSERT_OK(cluster_->WaitForTabletServerCount(4));

  // verify background task ran once more.
  WaitAndVerifyBackgroundTaskRuns(4, "after tserver4 (zone2) joins");

  ASSERT_OK(VerifyTransactionTableTablets(
      local_txnstatus_name,
      cluster_->num_tablet_servers() * FLAGS_transaction_table_num_tablets_per_tserver,
      2 * FLAGS_transaction_table_num_tablets_per_tserver));

  // wait until the tablet server finishes creating the tablets
  ASSERT_OK(WaitForTransactionStatusTablets(
      zone2_cloud_info,
      4 * FLAGS_transaction_table_num_tablets_per_tserver,
      2 * FLAGS_transaction_table_num_tablets_per_tserver));

  // verify the number of tablets for all the 3 placements
  ASSERT_OK(VerifyTransactionStatusTabletsFromClient(
      initial_cloud_info,
      cluster_->num_tablet_servers() * FLAGS_transaction_table_num_tablets_per_tserver,
      0,
      "Original"));

  ASSERT_OK(VerifyTransactionStatusTabletsFromClient(
      zone2_cloud_info,
      cluster_->num_tablet_servers() * FLAGS_transaction_table_num_tablets_per_tserver,
      2 * FLAGS_transaction_table_num_tablets_per_tserver,
      "Zone2"));

  ASSERT_OK(VerifyTransactionStatusTabletsFromClient(
      zone3_cloud_info,
      cluster_->num_tablet_servers() * FLAGS_transaction_table_num_tablets_per_tserver,
      0,
      "Zone3"));

  // Restore the original flags
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_transaction_status_check_interval_sec)
    = original_transaction_status_check_interval_sec;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_transaction_table_num_tablets)
    = original_transaction_table_num_tablets;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_transaction_table_num_tablets_per_tserver)
    = original_transaction_table_num_tablets_per_tserver;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_auto_create_local_transaction_tables)
    = original_auto_create_local_transaction_tables;
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_name_transaction_tables_with_tablespace_id)
    = original_name_transaction_tables_with_tablespace_id;

}

}  // namespace yb
