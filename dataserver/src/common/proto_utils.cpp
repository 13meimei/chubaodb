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

#include "proto_utils.h"

namespace chubaodb {

void SetResponseHeader(dspb::RangeResponse_Header* resp,
                       const dspb::RangeRequest_Header &req, ErrorPtr err) {
    resp->set_trace_id(req.trace_id());
    resp->set_cluster_id(req.cluster_id());
    if (err != nullptr) {
        resp->set_allocated_error(err.release());
    }
}

} // namespace chubaodb
