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

#include <stdint.h>
#include <string>

namespace chubaodb {
namespace raft {

class NodeResolver {
public:
    NodeResolver() = default;
    virtual ~NodeResolver() = default;

    virtual std::string GetNodeAddress(uint64_t node_id) = 0;
    virtual bool AddNodeAddress(uint64_t node_id, std::string address) = 0;
};

} /* namespace raft */
} /* namespace chubaodb */
