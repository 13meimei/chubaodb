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

#include "kv_fetcher.h"

namespace chubaodb {
namespace ds {
namespace storage {

std::unique_ptr<KvFetcher> KvFetcher::Create(Store& store, const dspb::SelectRequest& req) {
    if (!req.key().empty()) {
        return std::unique_ptr<KvFetcher>(new PointerKvFetcher(store, req.key(), true));
    } else {
        return std::unique_ptr<KvFetcher>(new TxnRangeKvFetcher(store, req.scope().start(), req.scope().limit()));
    }
}

std::unique_ptr<KvFetcher> KvFetcher::Create(Store& store, const dspb::ScanRequest& req) {
    return std::unique_ptr<KvFetcher>(new TxnRangeKvFetcher(store, req.start_key(), req.end_key()));
}


PointerKvFetcher::PointerKvFetcher(Store& s, const std::string& key, bool fetch_intent) :
    store_(s), key_(key), fetch_intent_(fetch_intent) {
}

Status PointerKvFetcher::Next(KvRecord& rec) {
    rec.Clear();

    if (fetched_) { // only once
        return Status::OK();
    }

    fetched_ = true;

    auto s = store_.Get(key_, &rec.value, false);
    if (s.ok()) {
        rec.MarkHasValue();
    } else if (s.code() != Status::kNotFound) { // error
        return s;
    }

    if (fetch_intent_) {
        s = store_.GetTxnValue(key_, rec.intent);
        if (s.ok()) {
            rec.MarkHasIntent();
        } else if (s.code() != Status::kNotFound) { // error
            return s;
        }
    }

    if (rec.Valid()) {
        rec.key = key_;
    }

    return Status::OK();
}

RangeKvFetcher::RangeKvFetcher(Store& s, const std::string& start, const std::string& limit) :
    iter_(s.NewIterator(start, limit)) {
    if (iter_ == nullptr) {
        status_ = Status(Status::kIOError, "create iterator failed", "null");
    }
}

Status RangeKvFetcher::Next(KvRecord& rec) {
    rec.Clear();

    if (!status_.ok()) {
        return status_;
    }
    // iter is over
    if (!iter_->Valid()) {
        status_ = iter_->status();
        return status_;
    }

    rec.key = iter_->Key();
    rec.value = iter_->Value();
    rec.MarkHasValue();
    iter_->Next();

    return status_;
}

TxnRangeKvFetcher::TxnRangeKvFetcher(Store& s, const std::string& start, const std::string& limit) {
    status_ = s.NewIterators(data_iter_, txn_iter_, start, limit);
}

bool TxnRangeKvFetcher::valid() {
    if (over_ || !status_.ok()) {
        return false;
    }

    auto s = data_iter_->status();
    if (!s.ok()) {
        status_ = std::move(s);
        return false;
    }
    s = txn_iter_->status();
    if (!s.ok()) {
        status_  = std::move(s);
        return false;
    }
    if (!data_iter_->Valid() && !txn_iter_->Valid()) {
        over_ = true;
        return false;
    }
    return true;
}

Status TxnRangeKvFetcher::Next(KvRecord& rec) {
    rec.Clear();

    if (!valid()) {
        return status_;
    }

    bool has_data = false, has_intent = false;
    std::string data_key, intent_key;
    auto both_valid = data_iter_->Valid() && txn_iter_->Valid();
    if (both_valid) {
        data_key = data_iter_->Key();
        intent_key = txn_iter_->Key();
        if (data_key == intent_key) {
            has_data = true;
            has_intent = true;
        } else if (data_key < intent_key) {
            has_data = true;
        } else {
            has_intent = true;
        }
    } else if (data_iter_->Valid()) {
        data_key = data_iter_->Key();
        has_data = true;
    } else { // only intent valid
        intent_key = txn_iter_->Key();
        has_intent = true;
    }

    // read data iter
    if (has_data) {
        rec.key = std::move(data_key);
        rec.value = data_iter_->Value();
        rec.MarkHasValue();
        data_iter_->Next();
    }

    // read txn iter
    if (has_intent) {
        rec.key = std::move(intent_key);
        rec.intent = txn_iter_->Value();
        rec.MarkHasIntent();
        txn_iter_->Next();
    }

    return Status::OK();
}

} // namespace storage
} // namespace ds
} // namespace chubaodb
