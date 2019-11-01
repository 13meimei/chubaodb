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

#include "options.h"
#include "status.h"

namespace chubaodb {
namespace raft {

class Raft {
public:
    Raft() = default;
    virtual ~Raft() = default;

    Raft(const Raft&) = delete;
    Raft& operator=(const Raft&) = delete;

    virtual bool IsStopped() const = 0;

    virtual void GetLeaderTerm(uint64_t* leader, uint64_t* term) const = 0;
    virtual bool IsLeader() const = 0;

    virtual Status TryToLeader() = 0;

    virtual Status Submit(std::string& cmd, uint64_t unique_seq, uint16_t rw_flag) = 0;
    virtual Status ChangeMemeber(const ConfChange& conf) = 0;

    virtual void GetStatus(RaftStatus* status) const = 0;

    virtual void Truncate(uint64_t index) = 0;
};

} /* namespace raft */
} /* namespace chubaodb */
