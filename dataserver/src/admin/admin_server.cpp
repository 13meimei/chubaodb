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

#include "admin_server.h"

#include "net/session.h"
#include "common/logger.h"
#include "server/range_server.h"
#include "server/worker.h"

#ifdef CHUBAO_USE_TCMALLOC
#include <gperftools/malloc_extension.h>
#endif

namespace chubaodb {
namespace ds {
namespace admin {

using namespace dspb;

AdminServer::AdminServer(server::ContextServer* context) :
    context_(context) {
}

AdminServer::~AdminServer() {
    Stop();
}

Status AdminServer::Start(uint16_t port) {
    net::ServerOptions sops;
    sops.io_threads_num = 0;
    sops.max_connections = 200;
    net_server_.reset(new net::Server(sops, "admin"));

    auto ret = net_server_->ListenAndServe("0.0.0.0", port,
            [this](const net::Context& ctx, const net::MessagePtr& msg) {
                onMessage(ctx, msg);
            });
    if (!ret.ok()) return ret;

    FLOG_INFO("[Admin] server listen on 0.0.0.0:{}", port);

    return Status::OK();
}

Status AdminServer::Stop() {
    net_server_->Stop();
    return Status::OK();
}

Status AdminServer::checkAuth(const AdminAuth& auth) {
    // TODO:
    return Status::OK();
}

Status AdminServer::execute(const AdminRequest& req, AdminResponse* resp) {
    switch (req.req_case()) {
        case AdminRequest::kSetCfg:
            return setConfig(req.set_cfg(), resp->mutable_set_cfg());
        case AdminRequest::kGetCfg:
            return getConfig(req.get_cfg(), resp->mutable_get_cfg());
        case AdminRequest::kGetInfo:
            return getInfo(req.get_info(), resp->mutable_get_info());
        case AdminRequest::kForceSplit:
            return forceSplit(req.force_split(), resp->mutable_force_split());
        case AdminRequest::kCompaction:
            return compaction(req.compaction(), resp->mutable_compaction());
        case AdminRequest::kClearQueue:
            return clearQueue(req.clear_queue(), resp->mutable_clear_queue());
        case AdminRequest::kGetPendings:
            return getPending(req.get_pendings(), resp->mutable_get_pendings());
        case AdminRequest::kFlushDb:
            return flushDB(req.flush_db(), resp->mutable_flush_db());
        case AdminRequest::kProfile:
            return profile(req.profile(), resp->mutable_profile());
        default:
            return Status(Status::kNotSupported, "admin type", std::to_string(req.req_case()));
    }
}

void AdminServer::onMessage(const net::Context& ctx, const net::MessagePtr& msg) {
    AdminRequest req;
    if (!req.ParseFromArray(msg->body.data(), static_cast<int>(msg->body.size()))) {
        FLOG_ERROR("[Admin] deserialize failed from {}, head: {}",
                ctx.remote_addr, msg->head.DebugString());
    }
    FLOG_INFO("[Admin] recv from {}, detail: {}", ctx.remote_addr, req.ShortDebugString());

    AdminResponse resp;
    Status ret = checkAuth(req.auth());
    if (ret.ok()) {
        ret = execute(req, &resp);
    }

    if (!ret.ok()) {
        FLOG_WARN("[Admin] handle cmd from {} error: {}, cmd: {}", ctx.remote_addr,
                ret.ToString(), req.ShortDebugString());
        resp.set_code(static_cast<uint32_t>(ret.code()));
        resp.set_error_msg(ret.ToString());
    }

    std::vector<uint8_t> resp_body;
    resp_body.resize(resp.ByteSizeLong());
    if (resp.SerializeToArray(resp_body.data(), static_cast<int>(resp_body.size()))) {
        ctx.Write(msg->head, std::move(resp_body));
    }
}

Status AdminServer::forceSplit(const ForceSplitRequest& req, ForceSplitResponse* resp) {
    auto rng = context_->range_server->Find(req.range_id());
    if (rng == nullptr) {
        return Status(Status::kNotFound, "range", std::to_string(req.range_id()));
    }
    FLOG_INFO("[Admin] force split range {}, version: {}", req.range_id(), req.version());
    return rng->ForceSplit(req.version(), resp->mutable_split_key());
}

Status AdminServer::compaction(const CompactionRequest& req, CompactionResponse* resp) {
    // TODO: FIXME
    return Status(Status::kNotSupported);
}

Status AdminServer::clearQueue(const ClearQueueRequest& req, ClearQueueResponse* resp) {
    bool clear_fast = false, clear_slow = false;
    switch (req.queue_type()) {
        case ClearQueueRequest_QueueType_FAST_WORKER:
            clear_fast = true;
            break;
        case ClearQueueRequest_QueueType_SLOW_WORKER:
            clear_slow = true;
            break;
        case ClearQueueRequest_QueueType_ALL:
            clear_fast = true;
            clear_slow = true;
            break;
        default:
            return Status(Status::kInvalidArgument, "queue type", std::to_string(req.queue_type()));
    }
    resp->set_cleared(context_->worker->ClearQueue(clear_fast, clear_slow));
    FLOG_WARN("[Admin] {} queue cleared: {}",
            ClearQueueRequest_QueueType_Name(req.queue_type()).c_str(), resp->cleared());
    return Status::OK();
}

Status AdminServer::getPending(const GetPendingsRequest& req, GetPendingsResponse* resp) {
    return Status(Status::kNotSupported);
}

Status AdminServer::flushDB(const FlushDBRequest& req, FlushDBResponse* resp) {
    return Status::OK();
}

} // namespace admin
} // namespace ds
} // namespace chubaodb
