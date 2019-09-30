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

#include "snapshot.h"

namespace chubaodb {
namespace ds {
namespace range {

Snapshot::Snapshot(uint64_t applied, dspb::SnapshotContext&& ctx,
                   storage::IteratorInterface* iter)
    : applied_(applied), context_(ctx), iter_(iter) {}

Snapshot::~Snapshot() { Close(); }

Status Snapshot::Next(std::string* data, bool* over) {
    if (iter_->Valid()) {
        dspb::SnapshotKVPair p;
        p.set_key(iter_->key());
        p.set_value(iter_->value());
        if (!p.SerializeToString(data)) {
            return Status(Status::kCorruption, "serialize snapshot data",
                          "pb return false");
        }
        *over = false;
        iter_->Next();
    } else {
        *over = true;
    }
    return iter_->status();
}

Status Snapshot::Context(std::string* context) {
    if (!context_.SerializeToString(context)) {
        return Status(Status::kCorruption, "serialize snapshot meta", "pb return false");
    }
    return Status::OK();
}

uint64_t Snapshot::ApplyIndex() { return applied_; }

void Snapshot::Close() {
    delete iter_;
    iter_ = nullptr;
}

} /* namespace range */
} /* namespace ds */
} /* namespace chubaodb */