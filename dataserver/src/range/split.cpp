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

#include "range.h"

#include <sstream>

#include "master/client.h"
#include "base/util.h"

#include "range_logger.h"

namespace chubaodb {
namespace ds {
namespace range {

void Range::checkSplit(uint64_t size) {
    statis_size_ += size;

    // split disabled
    if (!context_->GetSplitPolicy()->IsEnabled()) {
        return;
    }

    auto check_size = context_->GetSplitPolicy()->CheckSize();
    if (!statis_flag_ && statis_size_ > check_size) {
        statis_flag_ = true;
        context_->ScheduleCheckSize(id_);
    }
}

void Range::ResetStatisSize() {
    auto policy = context_->GetSplitPolicy();
    ResetStatisSize(policy->SplitSize(), policy->MaxSize());
}

void Range::ResetStatisSize(uint64_t split_size, uint64_t max_size) {
    // split size is split size, not half of split size
    // amicable sequence writing and random writing
    // const static uint64_t split_size = ds_config.range_config.split_size >>
    // 1;
    auto meta = meta_.Get();

    uint64_t total_size = 0;
    std::string split_key;
    auto s = store_->StatSize(split_size, &total_size , &split_key);
    statis_flag_ = false;
    statis_size_ = 0;
    if (!s.ok()) {
        if (s.code() == Status::kUnexpected) {
            RLOG_INFO("StatSize failed: {}", s.ToString());
        } else {
            RLOG_ERROR("StatSize failed: {}", s.ToString());
        }
        return;
    }
    real_size_ = total_size;
    if (split_key <= meta.start_key() || split_key >= meta.end_key()) {
        RLOG_ERROR("StatSize invalid split key: {} vs scope[{}-{}]",
                EncodeToHex(split_key),
                EncodeToHex(meta.start_key()),
                EncodeToHex(meta.end_key()));
        return ;
    }

    RLOG_INFO("StatSize real size: {}, split key: {}", real_size_, EncodeToHex(split_key));

    if (!verifyEpoch(meta.range_epoch())) {
        RLOG_WARN("StatSize epoch is changed");
        return;
    }

    // when real size >= max size, we need split with split size
    if (real_size_ >= max_size) {
        return askSplit(std::move(split_key), std::move(meta));
    }
}

void Range::askSplit(std::string &&key, basepb::Range&& meta, bool force) {
    assert(!key.empty());
    assert(key >= meta.start_key());
    assert(key < meta.end_key());

    RLOG_INFO("AskSplit, version: {}, key: {}, policy: {}",
            meta.range_epoch().version(), EncodeToHex(key),
            context_->GetSplitPolicy()->Description());

    mspb::AskSplitRequest ask;
    ask.set_allocated_range(new basepb::Range(std::move(meta)));
    ask.set_split_key(std::move(key));
    ask.set_force(force);
    mspb::AskSplitResponse resp;
    auto s = context_->MasterClient()->AskSplit(ask, resp);
    if (!s.ok()) {
        RLOG_WARN("AskSplit failed: {}", s.ToString());
    } else {
        RLOG_INFO("AskSplit success: {}", resp.ShortDebugString());
        startSplit(resp);
    }
}

void Range::startSplit(mspb::AskSplitResponse &resp) {
    if (!valid_) {
        return;
    }

    if (!verifyEpoch(resp.range().range_epoch())) {
        RLOG_WARN("StartSplit epoch is changed");
        return;
    }

    auto &split_key = resp.split_key();

    RLOG_INFO("StartSplit new_range_id: {} split_key: {}",
            resp.new_range_id(), EncodeToHex(split_key));

    dspb::Command cmd;
    // set cmd_id
    cmd.mutable_cmd_id()->set_node_id(node_id_);
    cmd.mutable_cmd_id()->set_seq(kPassbySubmitQueueCmdSeq);
    // set cmd_type
    cmd.set_cmd_type(dspb::CmdType::AdminSplit);

    // set cmd admin_split_req
    auto split_req = cmd.mutable_split_cmd();

    split_req->set_split_key(split_key);

    auto range = resp.release_range();

    auto psize = range->peers_size();
    auto rsize = resp.new_peer_ids_size();
    if (psize != rsize) {
        RLOG_WARN("StartSplit peers_size no equal");
        return;
    }

    auto epoch = range->mutable_range_epoch();

    // set verify epoch
    cmd.set_allocated_verify_epoch(new basepb::RangeEpoch(*epoch));

    // set leader
    split_req->set_leader(node_id_);
    // set epoch
    split_req->mutable_epoch()->set_version(epoch->version() + 1);

    // no need set con_ver;con_ver is member change
    // split_req->mutable_epoch()->set_version(epoch->conf_ver());

    // set range id
    range->set_id(resp.new_range_id());
    // set range start_key
    range->set_start_key(split_key);
    // range end_key doesn't need to change.

    // set range_epoch
    epoch->set_conf_ver(1);
    epoch->set_version(1);

    auto p0 = range->mutable_peers(0);
    // set range peers
    for (int i = 0; i < psize; i++) {
        auto px = range->mutable_peers(i);
        px->set_id(resp.new_peer_ids(i));

        // Don't consider the role
        if (i > 0 && px->node_id() == node_id_) {
            p0->Swap(px);
        }
    }

    split_req->set_allocated_new_range(range);

    auto ret = submit(cmd);
    if (!ret.ok()) {
        RLOG_ERROR("Split raft submit error: {}", ret.ToString());
    }
}

Status Range::Split(const dspb::SplitCommand& req, uint64_t raft_index,
        std::shared_ptr<Range>& split_range) {
    split_range = std::make_shared<Range>(context_, req.new_range());
    std::unique_ptr<db::DB> split_db;
    auto ret = store_->Split(req.new_range().id(), req.split_key(), raft_index, split_db);
    if (!ret.ok()) {
        RLOG_ERROR("Split(new range: {}) split new db failed: {}",
                req.new_range().id(), ret.ToString());
        return ret;
    }
    return split_range->Initialize(std::move(split_db), req.leader(), raft_index + 1);
}

Status Range::applySplit(const dspb::Command &cmd, uint64_t index) {
    RLOG_INFO("ApplySplit Begin, version: {}, index: {}", meta_.GetVersion(), index);

    const auto& req = cmd.split_cmd();
    auto ret = meta_.CheckSplit(req.split_key(), cmd.verify_epoch().version());
    if (ret.code() == Status::kStaleEpoch) {
        RLOG_WARN("ApplySplit(new range: {}) check failed: {}",
                req.new_range().id(), ret.ToString());
        return Status::OK();
    } else if (ret.code() == Status::kOutOfBound) {
        // invalid split key, ignore split request
        RLOG_ERROR("ApplySplit(new range: {}) check failed: {}",
                req.new_range().id(), ret.ToString());
        return Status::OK();
    } else if (!ret.ok()) {
        RLOG_ERROR("ApplySplit(new range: {}) check failed: {}",
                req.new_range().id(), ret.ToString());
        return ret;
    }

    context_->Statistics()->IncrSplitCount();

    ret = context_->SplitRange(id_, req, index);
    if (!ret.ok()) {
        RLOG_ERROR("ApplySplit(new range: {}) create failed: {}",
                req.new_range().id(), ret.ToString());
        return ret;
    }

    meta_.Split(req.split_key(), req.epoch().version());
    ret = store_->ApplySplit(req.split_key(), index);
    if (!ret.ok()) {
        return Status(Status::kIOError, "apply split", ret.ToString());
    }

    scheduleHeartbeat(false);

    if (req.leader() == node_id_) {
        // new range report heartbeat
        split_range_id_ = req.new_range().id();

        uint64_t rsize = context_->GetSplitPolicy()->SplitSize() / 2;
        auto rng = context_->FindRange(split_range_id_);
        if (rng != nullptr) {
            rng->scheduleHeartbeat(false);
            rng->SetRealSize(real_size_ - rsize);
        }

        real_size_ = rsize;
    }

    context_->Statistics()->DecrSplitCount();

    RLOG_INFO("ApplySplit(new range: {}) End. version:{}",
            req.new_range().id(), meta_.GetVersion());

    return ret;
}

Status Range::ForceSplit(uint64_t version, std::string *result_split_key) {
    auto meta = meta_.Get();
    // check version when version ne zero
    if (version != 0 && meta.range_epoch().version() != version) {
        std::ostringstream ss;
        ss << "request version: " << version << ", ";
        ss << "current version: " << meta.range_epoch().version();
        return Status(Status::kStaleEpoch, "force split", ss.str());
    }

    std::string split_key;
    split_key = FindMiddle(meta.start_key(), meta.end_key());
    if (split_key.empty() || split_key <= meta.start_key() || split_key >= meta.end_key()) {
        std::ostringstream ss;
        ss << "begin key: " << EncodeToHex(meta.start_key()) << ", ";
        ss << "end key: " << EncodeToHex(meta.end_key()) << ", ";
        ss << "split key: " << EncodeToHex(split_key);
        return Status(Status::kUnknown, "could not find split key", ss.str());
    }

    RLOG_INFO("Force split, start AskSplit. split key: [{}-{}]-{}, version: {}",
            EncodeToHex(meta.start_key()), EncodeToHex(meta.end_key()),
            EncodeToHex(split_key), meta.range_epoch().version());

    *result_split_key = split_key;
    askSplit(std::move(split_key), std::move(meta), true);

    return Status::OK();
}

}  // namespace range
}  // namespace ds
}  // namespace chubaodb
