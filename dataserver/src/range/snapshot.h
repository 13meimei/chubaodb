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

#include "dspb/dspb.pb.h"
#include "raft/snapshot.h"
#include "storage/iterator_interface.h"

namespace chubaodb {
namespace ds {
namespace range {

class Snapshot : public raft::Snapshot {
public:
    Snapshot(uint64_t applied, dspb::SnapshotContext&& ctx,
             storage::IteratorInterface* iter);
    ~Snapshot();

    Status Next(std::string* data, bool* over) override;
    Status Context(std::string* context) override;
    uint64_t ApplyIndex() override;
    void Close() override;

private:
    uint64_t applied_ = 0;
    dspb::SnapshotContext context_;
    storage::IteratorInterface* iter_ = nullptr;
};

} /* namespace range */
} /* namespace ds */
} /* namespace chubaodb */
