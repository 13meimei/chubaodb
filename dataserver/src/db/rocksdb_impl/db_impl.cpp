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

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#include <rocksdb/rate_limiter.h>

#include "db/mass_tree_impl/iterator_impl.h"

#include "manager_impl.h"
#include "write_batch_impl.h"
#include "base/util.h"

namespace chubaodb {
namespace ds {
namespace db {

static std::string makeAppliedKey(uint64_t range_id) {
    return std::string("A_") + std::to_string(range_id);
}

RocksDBImpl::RocksDBImpl(const RocksDBOptions& ops, RocksDBManager *manager,
            rocksdb::ColumnFamilyHandle *data_cf, rocksdb::ColumnFamilyHandle *txn_cf,
            rocksdb::ColumnFamilyHandle *meta_cf, MasstreeWrapper* txn_cache) :
    id_(ops.id),
    applied_key_(makeAppliedKey(id_)),
    start_key_(ops.start_key),
    end_key_(ops.end_key),
    db_(ops.db),
    manager_(manager),
    data_cf_(data_cf),
    txn_cf_(txn_cf),
    meta_cf_(meta_cf),
    txn_cache_(txn_cache) {
    if (ops.wal_disabled) {
        write_options_.disableWAL = true;
    }
    read_options_ = rocksdb::ReadOptions(ops.read_checksum, true);
}

RocksDBImpl::~RocksDBImpl() {
}

Status RocksDBImpl::loadApplied() {
    std::string applied_str;
    auto s = db_->Get(rocksdb::ReadOptions(true, false), meta_cf_, applied_key_, &applied_str);
    if (s.IsNotFound()) {
        applied_index_ = 0;
        return Status::OK();
    } else if (!s.ok()) {
        return Status(Status::kIOError, "get applied index", s.ToString());
    }
    // now s is ok
    try {
        applied_index_ = std::stoull(applied_str);
    } catch (std::exception &e) {
        return Status(Status::kCorruption, "invalid applied", EncodeToHex(applied_str));
    }
    return Status::OK();
}

Status RocksDBImpl::deleteApplied() {
    auto s = db_->Delete(write_options_, meta_cf_, applied_key_);
    if (!s.ok()) {
        return Status(Status::kIOError, "delete applied key", s.ToString());
    }
    return Status::OK();
}

Status RocksDBImpl::saveApplied(uint64_t index) {
    auto s = db_->Put(write_options_, meta_cf_, applied_key_, std::to_string(index));
    if (!s.ok()) {
        return Status(Status::kIOError, "put applied", s.ToString());
    }
    applied_index_ = index;
    return Status::OK();
}

Status RocksDBImpl::writeBatch(RocksWriteBatch* batch, uint64_t raft_index) {
    // write txn cache
    auto s = batch->WriteTxnCache(txn_cache_);
    if (!s.ok()) {
        return s;
    }
    // update applied index
    s = batch->Put(meta_cf_, applied_key_, std::to_string(raft_index));
    if (!s.ok()) {
        return s;
    }
    // batch write to db
    auto rs = db_->Write(write_options_, batch->getBatch());
    if (!rs.ok()) {
        return Status(Status::kIOError, "write batch", s.ToString());
    }
    applied_index_ = raft_index;
    return Status::OK();
}

Status RocksDBImpl::recoverTxnCache() {
    assert(txn_cache_ != nullptr);

    auto db_iter = db_->NewIterator(rocksdb::ReadOptions(true, false), txn_cf_);
    if (db_iter == nullptr) {
        return Status(Status::kIOError, "rebuild txn cache", "could not create iter");
    }

    RocksIterator iter(db_iter, start_key_, end_key_);
    while (iter.Valid()) {
        auto s = txn_cache_->Put(iter.Key(), iter.Value());
        if (!s.ok()) {
            return s;
        }
        iter.Next();
    }
    return iter.status();
}

Status RocksDBImpl::Open() {
    if (txn_cache_ != nullptr) {
        auto s = recoverTxnCache();
        if (!s.ok()) {
            return s;
        }
    }
    return loadApplied();
}

Status RocksDBImpl::Close() {
    return Status::OK();
}

Status RocksDBImpl::truncate() {
    applied_index_ = 0;

    // truncate data cf
    auto s = db_->DeleteRange(write_options_, data_cf_, start_key_, end_key_);
    if (!s.ok()) {
        return Status(Status::kIOError, "truncate data cf", s.ToString());
    }

    // truncate txn cf
    s = db_->DeleteRange(write_options_, txn_cf_, start_key_, end_key_);
    if (!s.ok()) {
        return Status(Status::kIOError, "truncate txn cf", s.ToString());
    }

    // truncate txn cache
    if (txn_cache_ != nullptr) {
        auto iter = txn_cache_->NewIterator(start_key_, end_key_);
        while (iter->Valid()) {
            txn_cache_->Delete(iter->Key());
            iter->Next();
        }
    }

    return deleteApplied();
}

Status RocksDBImpl::Destroy() {
    return this->truncate();
}

uint64_t RocksDBImpl::PersistApplied() {
    return applied_index_;
}

Status RocksDBImpl::Get(CFType cf, const std::string& key, std::string& value) {
    // get from cache if exist
    if (cf == CFType::kTxn && txn_cache_ != nullptr) {
        return txn_cache_->Get(key, value);
    }

    auto handle = getColumnFamily(cf);
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
    RocksWriteBatch batch(data_cf_, txn_cf_);
    auto s = batch.Put(cf, key, value);
    if (!s.ok()) {
        return s;
    }
    return writeBatch(&batch, raft_index);
}

Status RocksDBImpl::Delete(CFType cf, const std::string& key, uint64_t raft_index) {
    RocksWriteBatch batch(data_cf_, txn_cf_);
    auto s = batch.Delete(cf, key);
    if (!s.ok()) {
        return s;
    }
    return writeBatch(&batch, raft_index);
}

WriteBatchPtr RocksDBImpl::NewWriteBatch() {
    return WriteBatchPtr(new RocksWriteBatch(data_cf_, txn_cf_));
}

Status RocksDBImpl::Write(WriteBatch* batch, uint64_t raft_index) {
    auto rwb = dynamic_cast<RocksWriteBatch*>(batch);
    return writeBatch(rwb, raft_index);
}

Status RocksDBImpl::ApplySnapshotStart(uint64_t raft_index) {
    return this->truncate();
}

Status RocksDBImpl::ApplySnapshotData(WriteBatch* batch) {
    auto rwb = dynamic_cast<RocksWriteBatch*>(batch);
    auto s = rwb->WriteTxnCache(txn_cache_);
    if (!s.ok()) {
        return s;
    }
    auto rs = db_->Write(write_options_, rwb->getBatch());
    if (!rs.ok()) {
        return Status(Status::kIOError, "write snap data", s.ToString());
    }
    return Status::OK();
}

Status RocksDBImpl::ApplySnapshotFinish(uint64_t raft_index) {
    return saveApplied(raft_index);
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
    // TODO: use a txn cache iterator
    if (txn_cache_) {
        txn_iter.reset(new MassIterator(txn_cache_, start, limit));
    } else {
        txn_iter.reset(new RocksIterator(iterators[1], start, limit));
    }

    return Status::OK();
}

Status RocksDBImpl::SplitDB(uint64_t split_range_id, const std::string& split_key,
             uint64_t raft_index, std::unique_ptr<DB>& split_db) {
    auto s = manager_->CreatSplit(split_range_id, split_key, end_key_, split_db);
    if (!s.ok()) {
        return s;
    }
    // set applied index
    return dynamic_cast<RocksDBImpl*>(split_db.get())->saveApplied(raft_index);
}

Status RocksDBImpl::ApplySplit(const std::string& split_key, uint64_t raft_index) {
    end_key_ = split_key;
    return saveApplied(raft_index);
}

} // namespace db
} // namespace ds
} // namespace chubaodb
