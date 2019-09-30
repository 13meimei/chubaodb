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

#include <map>
#include <string>

#include "base/status.h"
#include "dspb/txn.pb.h"

#include "field_value.h"

namespace chubaodb {
namespace ds {
namespace storage {

class RowResult {
public:
    RowResult();
    ~RowResult();

    RowResult(const RowResult&) = delete;
    RowResult& operator=(const RowResult&) = delete;

    const std::string& GetKey() const { return key_; }
    void SetKey(const std::string& key) { key_ = key; }

    uint64_t GetVersion() const { return version_; }
    void SetVersion(uint64_t ver) { version_ = ver; }

    bool AddField(uint64_t col, std::unique_ptr<FieldValue>& field);
    FieldValue* GetField(uint64_t col) const;

    void Reset();

    void EncodeTo(const dspb::SelectRequest& req, dspb::RowValue* to);

private:
    std::string key_;
    uint64_t version_ = 0;
    std::map<uint64_t, FieldValue*> fields_;
};

class RowDecoder {
public:
    RowDecoder(const std::vector<basepb::Column>& primary_keys,
        const dspb::SelectRequest& req);

    ~RowDecoder();

    RowDecoder(const RowDecoder&) = delete;
    RowDecoder& operator=(const RowDecoder&) = delete;

    Status Decode(const std::string& key, const std::string& buf, RowResult& result);
    Status DecodeAndFilter(const std::string& key, const std::string& buf, RowResult& result, bool& match);

    std::string DebugString() const;

private:
    void addExprColumn(const dspb::Expr& expr);
    Status decodePrimaryKeys(const std::string& key, RowResult& result);
    Status decodeFields(const std::string& buf, RowResult& result);

private:
    const std::vector<basepb::Column>& primary_keys_;
    std::map<uint64_t, dspb::ColumnInfo> cols_;
    std::unique_ptr<dspb::Expr> where_expr_;
};

} /* namespace storage */
} /* namespace ds */
} /* namespace chubaodb */