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

#include "base/util.h"
#include "range_logger.h"

namespace chubaodb {
namespace ds {
namespace range {

// handle kv get
void Range::kvGet(RPCRequestPtr rpc, dspb::RangeRequest &req) {
    RLOG_DEBUG("RawGet begin");

    ErrorPtr err;

    dspb::RangeResponse resp;
    do {
        auto& get_req = req.kv_get();

        if (!verifyKeyInBound(get_req.key(), &err)) {
            break;
        }
        if (ds_config.raft_config.read_option == chubaodb::raft::READ_UNSAFE) {
            auto get_resp = resp.mutable_kv_get();
            auto btime = NowMicros();
            auto ret = store_->Get(get_req.key(), get_resp->mutable_value());
            context_->Statistics()->PushTime(HistogramType::kStore, NowMicros() - btime);
            get_resp->set_code(static_cast<int>(ret.code()));
        } else {
            submitCmd(std::move(rpc), *req.mutable_header(), chubaodb::raft::READ_FLAG,
                  [&req](dspb::Command &cmd) {
                      cmd.set_cmd_type(dspb::CmdType::KvGet);
                      cmd.set_allocated_kv_get_req(req.release_kv_get());
                  });
            return;
        }
    } while (false);

    if (err != nullptr) {
        RLOG_WARN("RawGet error: {}", err->ShortDebugString());
    }
    sendResponse(rpc, req.header(), resp, std::move(err));
}

// handle kv put
void Range::kvPut(RPCRequestPtr rpc, dspb::RangeRequest &req) {
    RLOG_DEBUG("RawPut begin");

    ErrorPtr err;
    do {
        if (!verifyKeyInBound(req.kv_put().key(), &err)) {
            break;
        }

        if (!hasSpaceLeft(LimitType::kSoft, &err)) {
            break;
        }

        submitCmd(std::move(rpc), *req.mutable_header(), chubaodb::raft::WRITE_FLAG,
                  [&req](dspb::Command &cmd) {
                      cmd.set_cmd_type(dspb::CmdType::KvPut);
                      cmd.set_allocated_kv_put_req(req.release_kv_put());
                  });
    } while (false);

    if (err != nullptr) {
        RLOG_WARN("RawPut error: {}", err->ShortDebugString());
        dspb::RangeResponse resp;
        sendResponse(rpc, req.header(), resp, std::move(err));
    }
}

Status Range::applyKvPut(const dspb::Command &cmd, uint64_t raft_index) {
    Status ret;

    RLOG_DEBUG("ApplyRawPut begin");
    auto &req = cmd.kv_put_req();
    auto btime = NowMicros();

    ErrorPtr err;
    do {
        if (!verifyEpoch(cmd.verify_epoch(), &err)) {
            break;
        }

        if (!verifyKeyInBound(req.key(), &err)) {
            RLOG_WARN("Apply RawPut failed, epoch is changed");
            ret = Status(Status::kInvalidArgument, "key not int range", "");
            break;
        }

        ret = store_->Put(req.key(), req.value(), raft_index);
        context_->Statistics()->PushTime(HistogramType::kStore, NowMicros() - btime);
        if (!ret.ok()) {
            RLOG_ERROR("ApplyRawPut failed, code: {}, msg: {}", ret.code(), ret.ToString());
            break;
        }

        if (cmd.cmd_id().node_id() == node_id_) {
            auto len = req.key().size() + req.value().size();
            checkSplit(len);
        }
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        dspb::RangeResponse resp;
        resp.mutable_kv_put()->set_code(ret.code());
        replySubmit(cmd, resp, std::move(err), btime);
    }

    return ret;
}

void Range::kvDelete(RPCRequestPtr rpc, dspb::RangeRequest &req) {
    RLOG_DEBUG("RawDelete begin");

    ErrorPtr err;
    do {
        if (!verifyKeyInBound(req.kv_delete().key(), &err)) {
            break;
        }

        if (!hasSpaceLeft(LimitType::kHard, &err)) {
            break;
        }

        submitCmd(std::move(rpc), *req.mutable_header(), chubaodb::raft::WRITE_FLAG,
                  [&req](dspb::Command &cmd) {
                      cmd.set_cmd_type(dspb::CmdType::KvDelete);
                      cmd.set_allocated_kv_delete_req(req.release_kv_delete());
                  });
    } while (false);

    if (err != nullptr) {
        RLOG_WARN("RawDelete error: {}", err->ShortDebugString());

        dspb::RangeResponse resp;
        sendResponse(rpc, req.header(), resp, std::move(err));
    }
}

Status Range::applyKvDelete(const dspb::Command &cmd, uint64_t raft_index) {
    Status ret;
    ErrorPtr err;

    RLOG_DEBUG("ApplyRawDelete begin");

    auto btime = NowMicros();
    auto &req = cmd.kv_delete_req();

    do {
        if (!verifyEpoch(cmd.verify_epoch(), &err)) {
            break;
        }

        if (!verifyKeyInBound(req.key(), &err)) {
            RLOG_WARN("ApplyRawDelete failed, epoch is changed");
            break;
        }

        ret = store_->Delete(req.key(), raft_index);
        context_->Statistics()->PushTime(HistogramType::kStore,
                                         NowMicros() - btime);

        if (!ret.ok()) {
            RLOG_ERROR("ApplyRawDelete failed, code: {}, msg: {}",
                       ret.code(), ret.ToString());
            break;
        }
        // ignore delete CheckSplit
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        dspb::RangeResponse resp;
        resp.mutable_kv_delete()->set_code(ret.code());
        replySubmit(cmd, resp, std::move(err), btime);
    }

    return ret;
}

}  // namespace range
}  // namespace ds
}  // namespace chubaodb
