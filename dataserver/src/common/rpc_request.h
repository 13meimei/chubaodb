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

_Pragma("once");

#include <vector>
#include <google/protobuf/message.h>

#include "net/message.h"
#include "proto_utils.h"

namespace chubaodb {

// rpc default timeout
static const uint32_t kDefaultRPCRequestTimeoutMS = 10000;

struct RPCRequest {
    net::Context ctx;
    net::MessagePtr msg;
    int64_t expire_time = 0; // absolute time, unit: ms
    int64_t begin_time = 0;  // unit: us

    RPCRequest(const net::Context& req_ctx, const net::MessagePtr& req_msg);
    virtual ~RPCRequest() = default;

    uint64_t MsgID() const { return msg->head.msg_id; }
    std::string FuncName() const;

    // Parse to request proto msg
    bool ParseTo(google::protobuf::Message& proto_req, bool zero_copy = true);

    // Send response
    virtual void Reply(const google::protobuf::Message& proto_resp);
};

using RPCRequestPtr = std::unique_ptr<RPCRequest>;

}  // namespace chubaodb
