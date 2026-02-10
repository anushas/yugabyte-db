// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// The following only applies to changes made to this file as part of YugabyteDB development.
//
// Portions Copyright (c) YugabyteDB, Inc.
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

#include <future>
#include <limits>

#include "yb/util/flags.h"

#include "yb/common/wire_protocol.h"

#include "yb/consensus/log_anchor_registry.h"
#include "yb/consensus/log_reader.h"
#include "yb/consensus/log_util.h"
#include "yb/consensus/metadata.pb.h"
#include "yb/consensus/opid_util.h"

#include "yb/rpc/messenger.h"
#include "yb/rpc/rpc_controller.h"
#include "yb/rpc/rpc_header.pb.h"

#include "yb/tserver/mini_tablet_server.h"
#include "yb/tserver/remote_bootstrap-test-base.h"
#include "yb/tserver/remote_bootstrap.pb.h"
#include "yb/tserver/remote_bootstrap.proxy.h"
#include "yb/tserver/remote_bootstrap_session.h"

#include "yb/util/crc.h"
#include "yb/util/env_util.h"
#include "yb/util/monotime.h"
#include "yb/util/stopwatch.h"
#include "yb/util/test_util.h"

using std::string;
using std::vector;

#define ASSERT_REMOTE_ERROR(status, err, code, str) \
    ASSERT_NO_FATALS(AssertRemoteError(status, err, code, str))

DECLARE_uint64(remote_bootstrap_idle_timeout_ms);
DECLARE_uint64(remote_bootstrap_timeout_poll_period_ms);

// Flags for test output
DECLARE_int32(remote_bootstrap_begin_session_timeout_ms);
DECLARE_int32(rbs_init_max_number_of_retries);

DECLARE_int32(TEST_delay_create_checkpoint_sec);

namespace yb {
namespace tserver {

using env_util::ReadFully;
using log::ReadableLogSegment;
using rpc::ErrorStatusPB;
using rpc::RpcController;

class RemoteBootstrapServiceTest : public RemoteBootstrapTest {
 public:
  RemoteBootstrapServiceTest() {
    // Poll for session expiration every 10 ms for the session timeout test.
    ANNOTATE_UNPROTECTED_WRITE(FLAGS_remote_bootstrap_timeout_poll_period_ms) = 10;
  }

 protected:
  void SetUp() override {
    RemoteBootstrapTest::SetUp();
    remote_bootstrap_proxy_.reset(
        new RemoteBootstrapServiceProxy(
            proxy_cache_.get(), HostPort::FromBoundEndpoint(mini_server_->bound_rpc_addr())));
  }

  Status DoBeginRemoteBootstrapSession(const string& tablet_id,
                                       const string& requestor_uuid,
                                       BeginRemoteBootstrapSessionResponsePB* resp,
                                       RpcController* controller) {
    controller->set_timeout(MonoDelta::FromSeconds(1.0));
    BeginRemoteBootstrapSessionRequestPB req;
    req.set_tablet_id(tablet_id);
    req.set_requestor_uuid(requestor_uuid);
    return UnwindRemoteError(
        remote_bootstrap_proxy_->BeginRemoteBootstrapSession(req, resp, controller), controller);
  }

  Status DoBeginValidRemoteBootstrapSession(
      string* session_id,
      tablet::RaftGroupReplicaSuperBlockPB* superblock = nullptr,
      uint64_t* idle_timeout_millis = nullptr,
      uint64_t* first_sequence_number = nullptr) {
    BeginRemoteBootstrapSessionResponsePB resp;
    RpcController controller;
    RETURN_NOT_OK(DoBeginRemoteBootstrapSession(GetTabletId(), GetLocalUUID(), &resp, &controller));
    *session_id = resp.session_id();
    if (superblock) {
      *superblock = resp.superblock();
    }
    if (idle_timeout_millis) {
      *idle_timeout_millis = resp.session_idle_timeout_millis();
    }
    if (first_sequence_number) {
      *first_sequence_number = resp.first_wal_segment_seqno();
    }
    return Status::OK();
  }

  Status DoCheckSessionActive(const string& session_id,
                              CheckRemoteBootstrapSessionActiveResponsePB* resp,
                              RpcController* controller) {
    controller->set_timeout(MonoDelta::FromSeconds(1.0));
    CheckRemoteBootstrapSessionActiveRequestPB req;
    req.set_session_id(session_id);
    return UnwindRemoteError(
        remote_bootstrap_proxy_->CheckRemoteBootstrapSessionActive(req, resp, controller),
        controller);
  }

  Status DoFetchData(const string& session_id, const DataIdPB& data_id,
                     uint64_t* offset, int64_t* max_length,
                     FetchDataResponsePB* resp,
                     RpcController* controller) {
    controller->set_timeout(MonoDelta::FromSeconds(1.0));
    FetchDataRequestPB req;
    req.set_session_id(session_id);
    req.mutable_data_id()->CopyFrom(data_id);
    if (offset) {
      req.set_offset(*offset);
    }
    if (max_length) {
      req.set_max_length(*max_length);
    }
    return UnwindRemoteError(
        remote_bootstrap_proxy_->FetchData(req, resp, controller), controller);
  }

  Status DoEndRemoteBootstrapSession(const string& session_id, bool is_success,
                                     const Status* error_msg,
                                     EndRemoteBootstrapSessionResponsePB* resp,
                                     RpcController* controller) {
    controller->set_timeout(MonoDelta::FromSeconds(1.0));
    EndRemoteBootstrapSessionRequestPB req;
    req.set_session_id(session_id);
    req.set_is_success(is_success);
    if (error_msg) {
      StatusToPB(*error_msg, req.mutable_error());
    }
    return UnwindRemoteError(
        remote_bootstrap_proxy_->EndRemoteBootstrapSession(req, resp, controller), controller);
  }

  // Decode the remote error into a Status object.
  Status ExtractRemoteError(const ErrorStatusPB* remote_error) {
    const RemoteBootstrapErrorPB& error =
        remote_error->GetExtension(RemoteBootstrapErrorPB::remote_bootstrap_error_ext);
    return StatusFromPB(error.status());
  }

  // Enhance a RemoteError Status message with additional details from the remote.
  Status UnwindRemoteError(Status status, const RpcController* controller) {
    if (!status.IsRemoteError()) {
      return status;
    }
    Status remote_error = ExtractRemoteError(controller->error_response());
    return status.CloneAndPrepend(remote_error.ToString());
  }

  void AssertRemoteError(Status status, const ErrorStatusPB* remote_error,
                         const RemoteBootstrapErrorPB::Code app_code,
                         const string& status_code_string) {
    ASSERT_TRUE(status.IsRemoteError()) << "Unexpected status code: " << status.ToString()
                                        << ", app code: "
                                        << RemoteBootstrapErrorPB::Code_Name(app_code)
                                        << ", status code string: " << status_code_string;
    const Status app_status = ExtractRemoteError(remote_error);
    const RemoteBootstrapErrorPB& error =
        remote_error->GetExtension(RemoteBootstrapErrorPB::remote_bootstrap_error_ext);
    ASSERT_EQ(app_code, error.code()) << error.ShortDebugString();
    ASSERT_EQ(status_code_string, app_status.CodeAsString()) << app_status.ToString();
    LOG(INFO) << app_status.ToString();
  }

  // Wrap given file name in the protobuf format suitable for a FetchData() call.
  static DataIdPB AsDataTypeId(const string& file_name) {
    DataIdPB data_id;
    data_id.set_type(DataIdPB::ROCKSDB_FILE);
    data_id.set_file_name(file_name);
    return data_id;
  }

  std::unique_ptr<RemoteBootstrapServiceProxy> remote_bootstrap_proxy_;
};

// Test beginning and ending a remote bootstrap session.
TEST_F(RemoteBootstrapServiceTest, TestSimpleBeginEndSession) {
  string session_id;
  tablet::RaftGroupReplicaSuperBlockPB superblock;
  uint64_t idle_timeout_millis;
  uint64_t first_segment_seqno;
  ASSERT_OK(DoBeginValidRemoteBootstrapSession(&session_id,
                                               &superblock,
                                               &idle_timeout_millis,
                                               &first_segment_seqno));
  // Basic validation of returned params.
  ASSERT_FALSE(session_id.empty());
  ASSERT_EQ(FLAGS_remote_bootstrap_idle_timeout_ms, idle_timeout_millis);
  ASSERT_TRUE(superblock.IsInitialized());
  ASSERT_EQ(1, first_segment_seqno);

  EndRemoteBootstrapSessionResponsePB resp;
  RpcController controller;
  ASSERT_OK(DoEndRemoteBootstrapSession(session_id, true, nullptr, &resp, &controller));
}

// Test starting two sessions. The current implementation will silently only create one.
TEST_F(RemoteBootstrapServiceTest, TestBeginTwice) {
  // Second time through should silently succeed.
  for (int i = 0; i < 2; i++) {
    string session_id;
    ASSERT_OK(DoBeginValidRemoteBootstrapSession(&session_id));
    ASSERT_FALSE(session_id.empty());
  }
}

// Test bad session id error condition.
TEST_F(RemoteBootstrapServiceTest, TestInvalidSessionId) {
  vector<string> bad_session_ids;
  bad_session_ids.push_back("hodor");
  bad_session_ids.push_back(GetLocalUUID());

  // Fetch a block for a non-existent session.
  for (const string& session_id : bad_session_ids) {
    FetchDataResponsePB resp;
    RpcController controller;
    DataIdPB data_id;
    data_id.set_type(DataIdPB::LOG_SEGMENT);
    data_id.set_wal_segment_seqno(1);
    Status status = DoFetchData(session_id, data_id, nullptr, nullptr, &resp, &controller);
    ASSERT_REMOTE_ERROR(status, controller.error_response(), RemoteBootstrapErrorPB::NO_SESSION,
                        STATUS(NotFound, "").CodeAsString());
  }

  // End a non-existent session.
  for (const string& session_id : bad_session_ids) {
    EndRemoteBootstrapSessionResponsePB resp;
    RpcController controller;
    Status status = DoEndRemoteBootstrapSession(session_id, true, nullptr, &resp, &controller);
    ASSERT_REMOTE_ERROR(status, controller.error_response(), RemoteBootstrapErrorPB::NO_SESSION,
                        STATUS(NotFound, "").CodeAsString());
  }
}

// Test bad tablet id error condition.
TEST_F(RemoteBootstrapServiceTest, TestInvalidTabletId) {
  BeginRemoteBootstrapSessionResponsePB resp;
  RpcController controller;
  Status status =
      DoBeginRemoteBootstrapSession("some-unknown-tablet", GetLocalUUID(), &resp, &controller);
  ASSERT_REMOTE_ERROR(status, controller.error_response(), RemoteBootstrapErrorPB::TABLET_NOT_FOUND,
                      STATUS(NotFound, "").CodeAsString());
}

// Test DataIdPB validation.
TEST_F(RemoteBootstrapServiceTest, TestInvalidBlockOrOpId) {
  string session_id;
  ASSERT_OK(DoBeginValidRemoteBootstrapSession(&session_id));

  // Invalid Segment Sequence Number for log fetch.
  {
    FetchDataResponsePB resp;
    RpcController controller;
    DataIdPB data_id;
    data_id.set_type(DataIdPB::LOG_SEGMENT);
    data_id.set_wal_segment_seqno(31337);
    Status status = DoFetchData(session_id, data_id, nullptr, nullptr, &resp, &controller);
    ASSERT_REMOTE_ERROR(status, controller.error_response(),
                        RemoteBootstrapErrorPB::WAL_SEGMENT_NOT_FOUND,
                        STATUS(NotFound, "").CodeAsString());
  }

  // Invalid file name for rocksdb file fetch.
  {
    FetchDataResponsePB resp;
    RpcController controller;
    DataIdPB data_id;
    data_id.set_type(DataIdPB::ROCKSDB_FILE);
    data_id.set_file_name("random_file_name");
    Status status = DoFetchData(session_id, data_id, nullptr, nullptr, &resp, &controller);
    ASSERT_REMOTE_ERROR(status, controller.error_response(),
                        RemoteBootstrapErrorPB::ROCKSDB_FILE_NOT_FOUND,
                        STATUS(NotFound, "").CodeAsString());
  }

  // Empty data type id (no Segment Sequence Number and no RocksDB file);
  {
    FetchDataResponsePB resp;
    RpcController controller;
    DataIdPB data_id;
    data_id.set_type(DataIdPB::LOG_SEGMENT);
    Status status = DoFetchData(session_id, data_id, nullptr, nullptr, &resp, &controller);
    ASSERT_REMOTE_ERROR(status, controller.error_response(),
                        RemoteBootstrapErrorPB::INVALID_REMOTE_BOOTSTRAP_REQUEST,
                        STATUS(InvalidArgument, "").CodeAsString());
  }

  // Both RocksDB file and Segment Sequence Number in the same "union" PB (illegal).
  {
    FetchDataResponsePB resp;
    RpcController controller;
    DataIdPB data_id;
    data_id.set_type(DataIdPB::LOG_SEGMENT);
    data_id.set_wal_segment_seqno(0);
    data_id.set_file_name("dummy_file_name");
    Status status = DoFetchData(session_id, data_id, nullptr, nullptr, &resp, &controller);
    ASSERT_REMOTE_ERROR(status, controller.error_response(),
                        RemoteBootstrapErrorPB::INVALID_REMOTE_BOOTSTRAP_REQUEST,
                        STATUS(InvalidArgument, "").CodeAsString());
  }
}

// Test that we are able to fetch log segments.
TEST_F(RemoteBootstrapServiceTest, TestFetchLog) {
  string session_id;
  tablet::RaftGroupReplicaSuperBlockPB superblock;
  uint64_t idle_timeout_millis;
  uint64_t segment_seqno;
  ASSERT_OK(DoBeginValidRemoteBootstrapSession(&session_id,
                                               &superblock,
                                               &idle_timeout_millis,
                                               &segment_seqno));

  ASSERT_EQ(1, segment_seqno);

  // Fetch the remote data.
  FetchDataResponsePB resp;
  RpcController controller;
  DataIdPB data_id;
  data_id.set_type(DataIdPB::LOG_SEGMENT);
  data_id.set_wal_segment_seqno(segment_seqno);
  ASSERT_OK(DoFetchData(session_id, data_id, nullptr, nullptr, &resp, &controller));

  // Fetch the local data.
  log::SegmentSequence local_segments;
  ASSERT_OK(tablet_peer_->log()->GetLogReader()->GetSegmentsSnapshot(&local_segments));

  uint64_t first_seg_seqno = (*local_segments.begin())->header().sequence_number();


  ASSERT_EQ(segment_seqno, first_seg_seqno)
      << "Expected equal sequence numbers: " << segment_seqno
      << " and " << first_seg_seqno;
  const scoped_refptr<ReadableLogSegment>& segment = ASSERT_RESULT(local_segments.front());
  faststring scratch;
  int64_t size = ASSERT_RESULT(segment->readable_file_checkpoint()->Size());
  scratch.resize(size);
  Slice slice;
  ASSERT_OK(ReadFully(segment->readable_file_checkpoint().get(), 0, size, &slice, scratch.data()));

  AssertDataEqual(slice.data(), slice.size(), resp.chunk());
}

// Test that the remote bootstrap session timeout works properly.
TEST_F(RemoteBootstrapServiceTest, TestSessionTimeout) {
  // This flag should be seen by the service due to TSO.
  // We have also reduced the timeout polling frequency in SetUp().
  // Expire the session almost immediately.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_remote_bootstrap_idle_timeout_ms) = 1;

  // Start session.
  string session_id;
  ASSERT_OK(DoBeginValidRemoteBootstrapSession(&session_id));

  MonoTime start_time = MonoTime::Now();
  CheckRemoteBootstrapSessionActiveResponsePB resp;

  do {
    RpcController controller;
    ASSERT_OK(DoCheckSessionActive(session_id, &resp, &controller));
    if (!resp.session_is_active()) {
      break;
    }
    SleepFor(MonoDelta::FromMilliseconds(1)); // 1 ms
  } while (MonoTime::Now().GetDeltaSince(start_time).ToSeconds() < 10);

  ASSERT_FALSE(resp.session_is_active()) << "Remote bootstrap session did not time out!";
}

// Test that when a remote bootstrap session holds the checkpoint lock (via blocking lock),
// a subsequent remote bootstrap session using try_lock fails to acquire the lock and returns
// and error. This simulates the scenario where:
// 1. First RPC acquires checkpoint lock and takes a long time (sleeps)
// 2. Client times out on first RPC
// 3. Second RPC is sent, uses try_lock, fails to acquire mutex, returns error
TEST_F(RemoteBootstrapServiceTest, TestCheckpointLockTimeout) {
  // Set test flag to sleep for 5 seconds after acquiring checkpoint lock.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_delay_create_checkpoint_sec) = 5;

  LOG(INFO) << "Test configuration: remote_bootstrap_begin_session_timeout_ms="
            << FLAGS_remote_bootstrap_begin_session_timeout_ms
            << ", rbs_init_max_number_of_retries="
            << FLAGS_rbs_init_max_number_of_retries
            << ", TEST_delay_create_checkpoint_sec="
            << FLAGS_TEST_delay_create_checkpoint_sec;

  // Create two sessions that will try to acquire the lock.
  auto session1 = make_scoped_refptr<RemoteBootstrapSession>(
      tablet_peer_, "TestSession1", "FakeUUID1", /*nsessions=*/nullptr);
  auto session2 = make_scoped_refptr<RemoteBootstrapSession>(
      tablet_peer_, "TestSession2", "FakeUUID2", /*nsessions=*/nullptr);

  // Start first session in a thread - it will acquire the lock and sleep.
  auto* first_session_ptr = session1.get();
  auto first_session_future = std::async(std::launch::async, [first_session_ptr]() {
    return first_session_ptr->InitBootstrapSession();
  });

  // Wait for first session to start and acquire the lock.
  SleepFor(MonoDelta::FromMilliseconds(500));

  // Now try to start a second session - it should fail to acquire the lock with try_lock.
  Status second_session_status = session2->InitBootstrapSession();

  // Verify the second session failed with InternalError status.
  ASSERT_TRUE(second_session_status.IsInternalError())
      << "Expected InternalError status, got: " << second_session_status;
  ASSERT_STR_CONTAINS(second_session_status.ToString(),
      "Unable to acquire checkpoint lock");

  // Wait for first session to complete and verify it succeeded.
  Status first_session_status = first_session_future.get();
  ASSERT_OK(first_session_status);

  // Reset the test flag.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_delay_create_checkpoint_sec) = 0;
}

// Test that when a BeginRemoteBootstrapSession RPC holds the checkpoint lock,
// a subsequent BeginRemoteBootstrapSession RPC call fails to acquire the lock.
// This tests the RPC-level behavior when checkpoint lock contention occurs.
TEST_F(RemoteBootstrapServiceTest, TestBeginRBSCheckpointLockContention) {
  // Set test flag to sleep for 5 seconds after acquiring checkpoint lock.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_delay_create_checkpoint_sec) = 5;

  LOG(INFO) << "Test configuration: remote_bootstrap_begin_session_timeout_ms="
            << FLAGS_remote_bootstrap_begin_session_timeout_ms
            << ", rbs_init_max_number_of_retries="
            << FLAGS_rbs_init_max_number_of_retries
            << ", TEST_delay_create_checkpoint_sec="
            << FLAGS_TEST_delay_create_checkpoint_sec;

  // Start first RPC call in a separate thread - it will acquire the lock and sleep.
  // Use a longer timeout so the RPC doesn't timeout before the sleep completes.
  auto first_rpc_future = std::async(std::launch::async, [this]() {
    BeginRemoteBootstrapSessionResponsePB resp;
    RpcController controller;
    // Use a longer timeout (15 seconds) to allow the sleep to complete.
    controller.set_timeout(MonoDelta::FromSeconds(15.0));
    BeginRemoteBootstrapSessionRequestPB req;
    req.set_tablet_id(GetTabletId());
    req.set_requestor_uuid(GetLocalUUID());
    return UnwindRemoteError(
        remote_bootstrap_proxy_->BeginRemoteBootstrapSession(req, &resp, &controller), &controller);
  });

  // Wait for first RPC to start and acquire the checkpoint lock.
  SleepFor(MonoDelta::FromMilliseconds(100));

  // Now try to make a second RPC call - it should fail to acquire the lock with try_lock.
  BeginRemoteBootstrapSessionResponsePB resp;
  RpcController controller;
  Status second_rpc_status = DoBeginRemoteBootstrapSession(
      GetTabletId(), GetLocalUUID(), &resp, &controller);

  // Verify the second RPC failed with RemoteError status.
  ASSERT_TRUE(second_rpc_status.IsRemoteError())
      << "Expected RemoteError status, got: " << second_rpc_status;
  ASSERT_STR_CONTAINS(second_rpc_status.ToString(), "Internal error");
  ASSERT_STR_CONTAINS(second_rpc_status.ToString(),
                      "Unable to acquire checkpoint lock");

  // Wait for first RPC to complete and verify it succeeded.
  Status first_rpc_status = first_rpc_future.get();
  ASSERT_OK(first_rpc_status);

  // Reset the test flag.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_delay_create_checkpoint_sec) = 0;
}

// Test that when a BeginRemoteBootstrapSession RPC times out while holding
// the checkpoint lock, the RPC returns a timeout status.
// This simulates the scenario where:
// 1. First RPC acquires checkpoint lock and takes longer than RPC timeout (sleeps)
// 2. Client times out on first RPC
// 3. Second RPC sent now fails to acquire the lock and returns RemoteError
//    if sleep of step 1 is still in progress (lock is still held).
// 4. Once the sleep of step 1 completes, the second RPC should succeed.
TEST_F(RemoteBootstrapServiceTest, TestBeginRBSRPCTimeoutWithCheckpointLock) {
  // Set test flag to sleep for 10 seconds after acquiring checkpoint lock.
  // This is longer than the 5 seconds RPC timeout used in DoBeginRemoteBootstrapSession.
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_delay_create_checkpoint_sec) = 10;

  LOG(INFO) << "Test configuration: remote_bootstrap_begin_session_timeout_ms="
            << FLAGS_remote_bootstrap_begin_session_timeout_ms
            << ", rbs_init_max_number_of_retries="
            << FLAGS_rbs_init_max_number_of_retries
            << ", TEST_delay_create_checkpoint_sec="
            << FLAGS_TEST_delay_create_checkpoint_sec
            << ", RPC timeout in DoBeginRemoteBootstrapSession=1.0 seconds";

  // Start first RPC call - it will acquire the lock and sleep for 10 seconds,
  // but the RPC timeout is 5 seconds, so it should timeout.
  BeginRemoteBootstrapSessionResponsePB resp;
  RpcController controller;
  controller.set_timeout(MonoDelta::FromSeconds(5.0));
  auto first_rpc_start_time = MonoTime::Now();
  Status first_rpc_status = DoBeginRemoteBootstrapSession(
      GetTabletId(), GetLocalUUID(), &resp, &controller);

  // Verify the first RPC timed out.
  // The status should be TimedOut or RemoteError with timeout information.
  LOG(INFO) << "First RPC status: " << first_rpc_status.ToString();
  ASSERT_TRUE(first_rpc_status.IsTimedOut() || first_rpc_status.IsRemoteError())
      << "Expected TimedOut or RemoteError status, got: " << first_rpc_status;
  ASSERT_STR_CONTAINS(first_rpc_status.ToString(), "Timed out");

  // Reset the test flag so rest of the RPCs are regular (no sleep).
  ANNOTATE_UNPROTECTED_WRITE(FLAGS_TEST_delay_create_checkpoint_sec) = 0;

  // Now try to make a second RPC call - it should either succeed (if lock was released)
  // or fail to acquire the lock (if lock is still held).
  // Since the first RPC timed out on the client side, the server-side operation
  // may have completed or may still be running.
  BeginRemoteBootstrapSessionResponsePB resp2;
  RpcController controller2;
  Status second_rpc_status = DoBeginRemoteBootstrapSession(
      GetTabletId(), GetLocalUUID(), &resp2, &controller2);

  LOG(INFO) << "Second RPC status after first RPC timed out: " << second_rpc_status.ToString();
  if (first_rpc_start_time.GetDeltaSince(MonoTime::Now()).ToSeconds() < 10) {
    ASSERT_TRUE(second_rpc_status.IsRemoteError())
      << "Expected RemoteError status, got: " << second_rpc_status;
    ASSERT_STR_CONTAINS(second_rpc_status.ToString(), "Internal error");
    ASSERT_STR_CONTAINS(second_rpc_status.ToString(),
                        "Unable to acquire checkpoint lock");
  } else {
    ASSERT_OK(second_rpc_status);
  }

  LOG(INFO) << "Sleeping for 10 seconds ";
  // Even though the client timed out, the RPC processing on the
  // server side is still asleep and holding the lock.
  // Wait to ensure it wakes up and completes the checkpoint
  // releasing the lock.
  SleepFor(MonoDelta::FromSeconds(10));

  // Now the next RPC should succeed.
  BeginRemoteBootstrapSessionResponsePB resp3;
  RpcController controller3;
  Status third_rpc_status = DoBeginRemoteBootstrapSession(
      GetTabletId(), GetLocalUUID(), &resp3, &controller3);
  LOG(INFO) << "Third RPC status: " << third_rpc_status.ToString();
  ASSERT_OK(third_rpc_status);
}

} // namespace tserver
} // namespace yb
