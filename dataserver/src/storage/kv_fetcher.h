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

#include "store.h"
#include "db/db.h"

namespace chubaodb {
namespace ds {
namespace storage {

struct KvRecord {
    enum Flags: uint32_t {
        kHasValue = 1U << 0,
        kHasIntent = 1U << 1,
    };

    std::string key;
    std::string value;
    std::string intent;
    uint32_t flag = 0;

    void MarkHasValue() { flag |= Flags::kHasValue; }
    bool HasValue() const { return (flag & Flags::kHasValue) != 0; }

    void MarkHasIntent() { flag |= Flags::kHasIntent; }
    bool HasIntent() const { return (flag & Flags::kHasIntent) != 0; }

    void Clear() { flag = 0; }
    bool Valid() const { return HasValue() || HasIntent(); }

    uint64_t Size() const {
        if (!Valid()) return 0;
        uint64_t size = key.size();
        if (HasValue()) {
            size += value.size();
        }
        if (HasIntent()) {
            size += intent.size();
        }
        return size;
    }
};


class KvFetcher {
public:
    KvFetcher() = default;
    virtual ~KvFetcher() = default;

    virtual Status Next(KvRecord& rec) = 0;

    static std::unique_ptr<KvFetcher> Create(Store& store, const dspb::SelectRequest& req);
    static std::unique_ptr<KvFetcher> Create(Store& store, const dspb::ScanRequest& req);
};


class PointerKvFetcher : public KvFetcher {
public:
    PointerKvFetcher(Store& s, const std::string& key, bool fetch_intent);

    Status Next(KvRecord& rec) override;

private:
    Store& store_;
    const std::string& key_;
    bool fetch_intent_ = false;
    bool fetched_ = false;
};


class RangeKvFetcher : public KvFetcher {
public:
    RangeKvFetcher(Store& s, const std::string& start, const std::string& limit);

    Status Next(KvRecord& rec) override;

private:
    db::IteratorPtr iter_ = nullptr;
    Status status_;
};


class TxnRangeKvFetcher : public KvFetcher {
public:
    TxnRangeKvFetcher(Store& s, const std::string& start, const std::string& limit);

    Status Next(KvRecord& rec) override;

private:
    bool valid();

private:
    db::IteratorPtr data_iter_;
    db::IteratorPtr txn_iter_;
    bool over_ = false;
    Status status_;
};

} // namespace storage
} // namespace ds
} // namespace chubaodb
