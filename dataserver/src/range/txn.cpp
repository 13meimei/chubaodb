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

namespace chubaodb {
namespace ds {
namespace range {

using namespace dspb;

static bool isAllDeleteOp(const dspb::PrepareRequest& req) {
    bool ret = true;
    for (const auto& intent: req.intents()) {
        if (intent.typ() != dspb::DELETE) {
            ret = false;
            break;
        }
    }
    return ret;
}

void Range::txnPrepare(RPCRequestPtr rpc, RangeRequest& req) {
    RLOG_DEBUG("TxnPrepare begin, req: {}", req.DebugString());

    ErrorPtr err;
    auto msg_id = rpc->MsgID();
    do {
        if (!verifyKeyInBound(req.prepare(), &err)) {
            break;
        }

        auto limit_type = isAllDeleteOp(req.prepare()) ? LimitType::kHard : LimitType::kSoft;
        if (!hasSpaceLeft(limit_type, &err)) {
            break;
        }

        // submit to raft
        submitCmd(std::move(rpc), *req.mutable_header(),
                [&req](dspb::Command &cmd) {
                cmd.set_cmd_type(dspb::CmdType::TxnPrepare);
                cmd.set_allocated_txn_prepare_req(req.release_prepare());
                });
    } while (false);

    if (err != nullptr) {
        RLOG_WARN("TxnPrepare {} error: {}", msg_id, err->ShortDebugString());
        RangeResponse resp;
        sendResponse(rpc, req.header(), resp, std::move(err));
    }
}

Status Range::applyTxnPrepare(const dspb::Command &cmd, uint64_t raft_index) {
    RLOG_DEBUG("ApplyTxnPrepare begin at {}, req: {}", raft_index, cmd.DebugString());

    Status ret;
    auto &req = cmd.txn_prepare_req();
    auto btime = NowMicros();
    ErrorPtr err;
    uint64_t bytes_written = 0;
    std::unique_ptr<PrepareResponse> prepare_resp(new PrepareResponse);
    do {
        if (!verifyEpoch(cmd.verify_epoch(), &err)) {
            break;
        }

        if (!verifyKeyInBound(req, &err)) {
            break;
        }

        bytes_written = store_->TxnPrepare(req, raft_index, prepare_resp.get());
        context_->Statistics()->PushTime(HistogramType::kStore, NowMicros() - btime);
    } while (false);

    if (!recovering_) {
        if (err != nullptr) {
            RLOG_ERROR("TxnPrepare {} failed: {}", req.txn_id(), err->ShortDebugString());
        } else if (prepare_resp->errors_size() > 0) {
            RLOG_ERROR("TxnPrepare {} failed: {}", req.txn_id(), prepare_resp->ShortDebugString());
        }
    }

    if (cmd.cmd_id().node_id() == node_id_) {
        RangeResponse resp;
        resp.set_allocated_prepare(prepare_resp.release());
        replySubmit(cmd, resp, std::move(err), btime);
        if (bytes_written > 0) {
            checkSplit(bytes_written);
        }
    }
    return ret;
}

void Range::txnDecide(RPCRequestPtr rpc, RangeRequest& req) {
    RLOG_DEBUG("TxnDecide begin, req: {}", req.DebugString());

    ErrorPtr err;
    auto msg_id = rpc->MsgID();
    do {
        if (!verifyKeyInBound(req.decide(), &err)) {
            break;
        }

        if (!hasSpaceLeft(LimitType::kHard, &err)) {
            break;
        }

        // submit to raft
        submitCmd(std::move(rpc), *req.mutable_header(),
                [&req](dspb::Command &cmd) {
                cmd.set_cmd_type(dspb::CmdType::TxnDecide);
                cmd.set_allocated_txn_decide_req(req.release_decide());
                });
    } while (false);

    if (err != nullptr) {
        RLOG_WARN("TxnDecide {} error: {}", msg_id, err->ShortDebugString());
        RangeResponse resp;
        sendResponse(rpc, req.header(), resp, std::move(err));
    }
}

Status Range::applyTxnDecide(const dspb::Command &cmd, uint64_t raft_index) {
    RLOG_DEBUG("ApplyTxnDecide begin, cmd: {}", cmd.DebugString());

    Status ret;
    auto &req = cmd.txn_decide_req();
    auto btime = NowMicros();
    ErrorPtr err;
    std::unique_ptr<DecideResponse> resp(new DecideResponse);
    uint64_t bytes_written = 0;

    do {
        if (!verifyEpoch(cmd.verify_epoch(), &err)) {
            break;
        }

        if (!verifyKeyInBound(req, &err)) {
            break;
        }

        bytes_written = store_->TxnDecide(req, raft_index, resp.get());
        context_->Statistics()->PushTime(HistogramType::kStore, NowMicros() - btime);
    } while (false);

    if (!recovering_) {
        if (err != nullptr) {
            RLOG_ERROR("TxnDecide {} failed: {}", req.txn_id(), err->ShortDebugString());
        } else if (resp->has_err()) {
            RLOG_ERROR("TxnDecide {} failed: {}", req.txn_id(), resp->err().ShortDebugString());
        }
    }

    if (cmd.cmd_id().node_id() == node_id_) {
        RangeResponse range_resp;
        range_resp.set_allocated_decide(resp.release());
        replySubmit(cmd, range_resp, std::move(err), btime);
        if (bytes_written > 0) {
            checkSplit(bytes_written);
        }
    }
    return ret;
}

void Range::txnClearup(RPCRequestPtr rpc, RangeRequest& req) {
    RLOG_DEBUG("TxnClearup  begin, req: {}", req.clear_up().DebugString());

    ErrorPtr err;
    auto msg_id = rpc->MsgID();
    do {
        if (!verifyKeyInBound(req.clear_up().primary_key(), &err)) {
            break;
        }

        if (!hasSpaceLeft(LimitType::kHard, &err)) {
            break;
        }

        submitCmd(std::move(rpc), *req.mutable_header(),
                [&req](dspb::Command &cmd) {
                cmd.set_cmd_type(dspb::CmdType::TxnClearup);
                cmd.set_allocated_txn_clearup_req(req.release_clear_up());
                });
    } while (false);

    if (err != nullptr) {
        RLOG_WARN("TxnClearup {} error: {}", msg_id, err->ShortDebugString());
        RangeResponse resp;
        sendResponse(rpc, req.header(), resp, std::move(err));
    }
}

Status Range::applyTxnClearup(const dspb::Command &cmd, uint64_t raft_index) {
    Status ret;
    auto &req = cmd.txn_clearup_req();
    auto btime = NowMicros();
    ErrorPtr err;
    std::unique_ptr<ClearupResponse> resp(new ClearupResponse);

    do {
        if (!verifyEpoch(cmd.verify_epoch(), &err)) {
            break;
        }

        if (!verifyKeyInBound(req.primary_key(), &err)) {
            break;
        }

        store_->TxnClearup(req, raft_index, resp.get());
        context_->Statistics()->PushTime(HistogramType::kStore, NowMicros() - btime);
    } while (false);

    if (!recovering_) {
        if (err != nullptr) {
            RLOG_ERROR("TxnClearup {} failed: {}", req.txn_id(), err->ShortDebugString());
        } else if (resp->has_err()) {
            RLOG_ERROR("TxnClearup {} failed: {}", req.txn_id(), resp->err().ShortDebugString());
        }
    }

    if (cmd.cmd_id().node_id() == node_id_) {
        RangeResponse range_resp;
        range_resp.set_allocated_clear_up(resp.release());
        replySubmit(cmd, range_resp, std::move(err), btime);
    }
    return ret;
}

void Range::txnGetLockInfo(RPCRequestPtr rpc, RangeRequest& req) {
    auto btime = NowMicros();
    ErrorPtr err;
    RangeResponse resp;
    do {
        if (!verifyKeyInBound(req.get_lock_info().key(), &err)) {
            break;
        }

        store_->TxnGetLockInfo(req.get_lock_info(), resp.mutable_get_lock_info());
        context_->Statistics()->PushTime(HistogramType::kStore, NowMicros() - btime);
    } while (false);

    if (err != nullptr) {
        RLOG_ERROR("TxnGetLockInfo {} failed: {}", req.get_lock_info().txn_id(), err->ShortDebugString());
    }

    sendResponse(rpc, req.header(), resp, std::move(err));
}

void Range::txnSelect(RPCRequestPtr rpc, RangeRequest& req) {
    RLOG_DEBUG("Select begin, req: {}", req.DebugString());

    auto btime = NowMicros();
    auto msg_id = rpc->MsgID();
    ErrorPtr err;
    RangeResponse resp;
    do {
        auto key = req.select().key();
        if (!key.empty() && !verifyKeyInBound(key, &err)) {
            break;
        }

        auto select_resp = resp.mutable_select();
        auto ret = store_->TxnSelect(req.select(), select_resp);
        context_->Statistics()->PushTime(HistogramType::kStore, NowMicros() - btime);

        if (!ret.ok()) {
            RLOG_ERROR("TxnSelect from store error: {}", ret.ToString());
            select_resp->set_code(static_cast<int>(ret.code()));
            break;
        }

        if (key.empty() && !verifyEpoch(req.header().range_epoch(), &err)) {
            resp.clear_select();
            RLOG_WARN("epoch change Select error: {}", err->ShortDebugString());
        }
    } while (false);

    if (err != nullptr) {
        RLOG_WARN("TxnSelect {} error: {}", msg_id, err->ShortDebugString());
    } else {
        RLOG_DEBUG("TxnSelect {} result: code={}, rows={}", msg_id,
                (resp.has_select() ? resp.select().code() : 0),
                (resp.has_select() ? resp.select().rows_size() : 0));
    }

    sendResponse(rpc, req.header(), resp, std::move(err));
}

void Range::txnScan(RPCRequestPtr rpc, RangeRequest& req) {
    RLOG_DEBUG("TxnScan begin, req: {}", req.DebugString());

    auto btime = NowMicros();
    auto msg_id = rpc->MsgID();
    ErrorPtr err;
    RangeResponse resp;
    do {
        auto scan_resp = resp.mutable_scan();
        auto ret = store_->TxnScan(req.scan(), scan_resp);
        context_->Statistics()->PushTime(HistogramType::kStore, NowMicros() - btime);

        if (!ret.ok()) {
            RLOG_ERROR("TxnScan from store error: {}", ret.ToString());
            scan_resp->set_code(static_cast<int>(ret.code()));
            break;
        }

        if (!verifyEpoch(req.header().range_epoch(), &err)) {
            resp.clear_scan();
            RLOG_WARN("epoch change Select error: {}", err->ShortDebugString());
        }
    } while (false);

    if (err != nullptr) {
        RLOG_WARN("TxnScan {} error: {}", msg_id, err->ShortDebugString());
    } else {
        RLOG_DEBUG("TxnScan {} result: code={}, size={}", msg_id,
                (resp.has_scan() ? resp.scan().code() : 0),
                (resp.has_scan() ? resp.scan().kvs_size() : 0));
    }

    sendResponse(rpc, req.header(), resp, std::move(err));
}

}  // namespace range
}  // namespace ds
}  // namespace chubaodb
