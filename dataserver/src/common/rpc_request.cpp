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

#include "rpc_request.h"

#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include "base/util.h"
#include "logger.h"
#include "proto/gen/dspb/function.pb.h"

namespace chubaodb {

RPCRequest::RPCRequest(const net::Context& req_ctx, const net::MessagePtr& req_msg) :
    ctx(req_ctx),
    msg(req_msg),
    begin_time(NowMicros()) {
    expire_time = NowMilliSeconds();
    if (msg->head.timeout != 0) {
        expire_time += msg->head.timeout;
    } else {
        expire_time += kDefaultRPCRequestTimeoutMS;
    };
}

std::string RPCRequest::FuncName() const {
    return dspb::FunctionID_Name(static_cast<dspb::FunctionID>(msg->head.func_id));
}

bool RPCRequest::ParseTo(google::protobuf::Message& proto_req, bool zero_copy) {
    auto data = msg->body.data();
    auto len = static_cast<int>(msg->body.size());
    if (zero_copy) {
        google::protobuf::io::ArrayInputStream input(data, len);
        return proto_req.ParseFromZeroCopyStream(&input);
    } else {
        return proto_req.ParseFromArray(data, len);
    }
}

void RPCRequest::Reply(const google::protobuf::Message& proto_resp) {
    std::vector<uint8_t> resp_body;
    resp_body.resize(proto_resp.ByteSizeLong());
    auto ret = proto_resp.SerializeToArray(resp_body.data(), static_cast<int>(resp_body.size()));
    if (ret) {
        ret = ctx.Write(msg->head, std::move(resp_body));
        if (!ret) {
            FLOG_WARN("reply to {} failed: maybe connection is closed.", ctx.remote_addr);
        }
    } else {
        FLOG_ERROR("serialize response failed, msg: {}", proto_resp.ShortDebugString());
    }
}

}  // namespace chubaodb
