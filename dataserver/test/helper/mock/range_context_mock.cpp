// Copyright 2019 The Chubao Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

#include "range_context_mock.h"

#include <unistd.h>

#include "base/fs_util.h"
#include "storage/meta_store.h"
#include "range/split_policy.h"
#include "range/range.h"

#include "../helper_util.h"
#include "master_client_mock.h"
#include "raft_mock.h"

namespace chubaodb {
namespace test {
namespace mock {

using namespace chubaodb::ds;
using namespace chubaodb::ds::range;

class DisableSplitPolicy : public SplitPolicy {
public:
    std::string Description() const override { return "DisableSplit"; }
    bool IsEnabled() const override { return false; }
    uint64_t CheckSize() const override { return 0; }
    uint64_t SplitSize() const override { return 0; }
    uint64_t MaxSize() const override { return 0; }
};

Status RangeContextMock::Init() {
    // init db
    char path[] = "/tmp/chubaodb_ds_range_mock_XXXXXX";
    char* tmp = mkdtemp(path);
    if (tmp == NULL) {
        return Status(Status::kIOError, "mkdtemp", "");
    }
    path_ = path;

    // open db
    auto s = helper::NewDBManager(JoinFilePath({path_, "data"}), db_manager_);
    if (!s.ok()) {
        return s;
    }

    // open meta db
    meta_store_.reset(new storage::MetaStore(JoinFilePath({path_, "meta"})));
    auto ret = meta_store_->Open();
    if (!ret.ok()) return ret;

    // master client
    master_client_.reset(new MasterClientMock);

    // raft_server
    raft_server_.reset(new RaftServerMock);

    // range stats
    range_stats_.reset(new RangeStats);

    // split policy
    split_policy_.reset(new DisableSplitPolicy);

    return Status::OK();
}

void RangeContextMock::Destroy() {
    db_manager_.reset();
    if (!path_.empty()) {
        RemoveDirAll(path_.c_str());
    }
}

void RangeContextMock::ScheduleCheckSize(uint64_t range_id) {
}

Status RangeContextMock::CreateRange(const basepb::Range& meta, uint64_t leader,
                   uint64_t index, std::shared_ptr<Range> *result) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = ranges_.find(meta.id());
    if (it != ranges_.end()) {
        return Status(Status::kExisted);
    }
    auto rng = std::make_shared<Range>(this, meta);
    auto s = rng->Initialize(nullptr, leader, index);
    if (!s.ok()) {
        return s;
    }
    ranges_.emplace(meta.id(), rng);
    if (result) {
        *result = rng;
    }
    return Status::OK();
}

std::shared_ptr<Range> RangeContextMock::FindRange(uint64_t range_id) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = ranges_.find(range_id);
    if (it == ranges_.end()) {
        return nullptr;
    } else {
        return it->second;
    }
}

Status RangeContextMock::SplitRange(uint64_t range_id, const dspb::SplitCommand &req, uint64_t raft_index) {
    CreateRange(req.new_range(), req.leader(), raft_index);
    return Status::OK();
}

}
}
}
