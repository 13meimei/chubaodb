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

#include "iterator_impl.h"

namespace chubaodb {
namespace ds {
namespace db {

RocksIterator::RocksIterator(rocksdb::Iterator* it, const std::string& start,
                   const std::string& limit)
    : rit_(it), limit_(limit) {
    assert(!start.empty());
    assert(!limit.empty());
    rit_->Seek(start);
}

RocksIterator::~RocksIterator() {
    delete rit_;
}

bool RocksIterator::Valid() {
    return rit_->Valid() && (Key() < limit_);
}

void RocksIterator::Next() {
    rit_->Next();
}

Status RocksIterator::status() {
    if (!rit_->status().ok()) {
        return Status(Status::kIOError, rit_->status().ToString(), "");
    }
    return Status::OK();
}

std::string RocksIterator::Key() {
    return rit_->key().ToString();
}

std::string RocksIterator::Value() {
    return rit_->value().ToString();
}

uint64_t RocksIterator::KeySize() {
    return rit_->key().size();
}

uint64_t RocksIterator::ValueSize() {
    return rit_->value().size();
}

} // namespace db
} // namespace ds
} // namespace chubaodb
