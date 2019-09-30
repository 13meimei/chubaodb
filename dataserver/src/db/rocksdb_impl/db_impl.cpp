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

#include "db_impl.h"

#include <sstream>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#include <rocksdb/rate_limiter.h>

#include "base/util.h"

#include "manager_impl.h"
#include "write_batch_impl.h"

namespace chubaodb {
namespace ds {
namespace db {

RocksDBImpl::RocksDBImpl(const RocksDBOptions& ops, RocksDBManager *manager,
        rocksdb::ColumnFamilyHandle *data_cf, rocksdb::ColumnFamilyHandle *txn_cf) :
    id_(ops.id),
    wal_disabled_(ops.wal_disabled),
    replay_(ops.replay),
    start_key_(ops.start_key),
    end_key_(ops.end_key),
    db_(ops.db),
    manager_(manager),
    data_cf_(data_cf),
    txn_cf_(txn_cf) {

    if (ops.wal_disabled) {
        write_options_.disableWAL = true;
    }
    read_options_ = rocksdb::ReadOptions(ops.read_checksum, true);
}

RocksDBImpl::~RocksDBImpl() {
}

Status RocksDBImpl::Open() {
    return Status::OK();
}

Status RocksDBImpl::Close() {
    return Status::OK();
}

Status RocksDBImpl::Destroy() {
    db_->DeleteRange(write_options_, data_cf_, start_key_, end_key_);
    db_->DeleteRange(write_options_, txn_cf_, start_key_, end_key_);

    applied_index_ = 0;
    return Status::OK();
}

uint64_t RocksDBImpl::PersistApplied() {
    return applied_index_;
}

Status RocksDBImpl::Get(CFType cf, const std::string& key, std::string& value) {
    auto handle = GetColumnFamily(cf);
    auto s = db_->Get(read_options_, handle, key, &value);
    if (s.ok()) {
        return Status::OK();
    } else if (s.IsNotFound()) {
        return Status(Status::kNotFound);
    } else {
        return Status(Status::kIOError, "Get", s.ToString());
    }
}

Status RocksDBImpl::Put(CFType cf, const std::string& key, const std::string& value,
        uint64_t raft_index) {
    auto handle = GetColumnFamily(cf);
    auto s = db_->Put(write_options_, handle, key, value);
    if (s.ok()) {
        applied_index_ = raft_index;
        return Status::OK();
    } else {
        return Status(Status::kIOError, "PUT", s.ToString());
    }
}

WriteBatchPtr RocksDBImpl::NewWriteBatch() {
    return WriteBatchPtr(new RocksWriteBatch(data_cf_, txn_cf_));
}

Status RocksDBImpl::Write(WriteBatch* batch, uint64_t raft_index) {
    auto rwb = dynamic_cast<RocksWriteBatch*>(batch);
    auto s = db_->Write(write_options_, rwb->getBatch());
    if (s.ok()) {
        applied_index_ = raft_index;
        return Status::OK();
    } else {
        return Status(Status::kIOError, "Write", s.ToString());
    }
}

Status RocksDBImpl::Delete(CFType cf, const std::string& key, uint64_t raft_index) {
    auto handle = GetColumnFamily(cf);
    auto s = db_->Delete(write_options_, handle, key);
    if (s.ok()) {
        applied_index_ = raft_index;
        return Status::OK();
    } else {
        return Status(Status::kIOError, "Delete", s.ToString());
    }
}

Status RocksDBImpl::DeleteRange(CFType cf, const std::string& begin_key, const std::string& end_key) {
    auto handle = GetColumnFamily(cf);
    auto s = db_->DeleteRange(write_options_, handle, begin_key, end_key);
    if (s.ok()) {
        return Status::OK();
    } else {
        return Status(Status::kIOError, "DeleteRange", s.ToString());
    }
}

Status RocksDBImpl::ApplySnapshotStart(uint64_t raft_index) {
    db_->DeleteRange(write_options_, data_cf_, start_key_, end_key_);
    db_->DeleteRange(write_options_, txn_cf_, start_key_, end_key_);

    applied_index_ = 0;
    return Status::OK();
}

Status RocksDBImpl::ApplySnapshotData(WriteBatch* batch) {
    auto rwb = dynamic_cast<RocksWriteBatch*>(batch);
    auto s = db_->Write(write_options_, rwb->getBatch());
    if (s.ok()) {
        return Status::OK();
    } else {
        return Status(Status::kIOError, "Write", s.ToString());
    }
}

Status RocksDBImpl::ApplySnapshotFinish(uint64_t raft_index) {
    applied_index_ = raft_index;
    return Status::OK();
}

IteratorPtr RocksDBImpl::NewIterator(const std::string& start, const std::string& limit) {
    auto it = db_->NewIterator(read_options_);
    IteratorPtr iter(new RocksIterator(it, start, limit));
    return iter;
}

Status RocksDBImpl::NewIterators(const std::string& start, const std::string& limit,
        IteratorPtr& data_iter, IteratorPtr& txn_iter) {

    std::vector<rocksdb::ColumnFamilyHandle*> cf_handles;
    cf_handles.push_back(data_cf_);
    cf_handles.push_back(txn_cf_);

    std::vector<rocksdb::Iterator*> iterators;
    rocksdb::ReadOptions rops;
    rops.fill_cache = false;
    auto s = db_->NewIterators(rops, cf_handles, &iterators);
    if (!s.ok()) {
        return Status(Status::kIOError, "create iterators", s.ToString());
    }

    assert(iterators.size() == 2);
    data_iter.reset(new RocksIterator(iterators[0], start, limit));
    txn_iter.reset(new RocksIterator(iterators[1], start, limit));

    return Status::OK();
}

Status RocksDBImpl::SplitDB(uint64_t split_range_id, const std::string& split_key,
             uint64_t raft_index, std::unique_ptr<DB>& split_db) {

    auto s = manager_->CreatSplit(split_range_id, split_key, end_key_, split_db);
    if (s.ok()) {
        // set applied index
        dynamic_cast<RocksDBImpl*>(split_db.get())->applied_index_ = raft_index;
    }
    return s;
}

Status RocksDBImpl::ApplySplit(const std::string& split_key, uint64_t raft_index) {
    end_key_ = split_key;
    applied_index_ = raft_index;
    return Status::OK();
}

} // namespace db
} // namespace ds
} // namespace chubaodb
