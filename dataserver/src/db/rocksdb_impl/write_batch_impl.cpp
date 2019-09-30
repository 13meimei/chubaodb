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

#include "write_batch_impl.h"

#include <rocksdb/write_batch.h>

namespace chubaodb {
namespace ds {
namespace db {

RocksWriteBatch::RocksWriteBatch(rocksdb::ColumnFamilyHandle* data_cf, rocksdb::ColumnFamilyHandle* txn_cf) :
    data_cf_(data_cf),
    txn_cf_(txn_cf) {
}

Status RocksWriteBatch::Put(CFType cf, const std::string& key, const std::string& value) {
    auto s = batch_.Put(cf == CFType::kData? data_cf_ : txn_cf_, key, value);
    return Status(static_cast<Status::Code >(s.code()));
}

Status RocksWriteBatch::Delete(CFType cf, const std::string& key) {
    auto s = batch_.Delete(cf == CFType::kData? data_cf_ : txn_cf_, key);
    return Status(static_cast<Status::Code >(s.code()));
}

} // namespace db
} // namespace ds
} // namespace chubaodb

