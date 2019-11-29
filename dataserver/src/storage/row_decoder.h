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

#include <functional>
#include <map>
#include <unordered_set>
#include <string>

#include "base/status.h"
#include "dspb/txn.pb.h"

#include "field_value.h"

namespace chubaodb {
namespace ds {
namespace storage {

struct RowColumnInfo {
    u_int64_t col_id;
    bool asc;
};

class RowResult {
public:
    RowResult();
    ~RowResult();

    RowResult(const RowResult &);
    RowResult &operator=(const RowResult &);

    bool operator==(const RowResult &other) const {
        bool bEq = true;
        if (col_order_by_infos_ != nullptr && other.col_order_by_infos_ != nullptr 
            && col_order_by_infos_->size() == other.col_order_by_infos_->size()) {
            for (auto &colInfo : *(col_order_by_infos_)) {
                auto field1 = GetField(colInfo.col_id);
                auto field2 = other.GetField(colInfo.col_id);
                if (field1 != nullptr && field2 != nullptr) {
                    if (!fcompare(*(field1), *(field2), CompareOp::kEqual)) {
                        bEq = false;
                        break;
                    }
                } else if (field1 != field2){
                    bEq = false;
                    break;
                }
            }
        }

        return bEq;
    }

    friend bool operator<(const RowResult &r1, const RowResult &r2) {
        bool bCmp = false;
        if (r1.col_order_by_infos_ != nullptr && r2.col_order_by_infos_ != nullptr 
            && r1.col_order_by_infos_->size() == r2.col_order_by_infos_->size()) {
            for (auto &colInfo : *(r1.col_order_by_infos_)) {
                auto field1 = r1.GetField(colInfo.col_id);
                auto field2 = r2.GetField(colInfo.col_id);
                if (!field1 || !field2) {
                    continue;
                }
                if (fcompare(*(field1), *(field2), CompareOp::kEqual)) {
                    continue;
                }

                bCmp = fcompare(*(field1), *(field2), CompareOp::kLess);
                if (!colInfo.asc) {
                    bCmp = !bCmp;
                }
                break;
            }
        }

        return bCmp;
    }

    const std::string &GetKey() const { return key_; }
    void SetKey(const std::string &key) { key_ = key; }

    const std::string &GetValue() const { return value_; }
    void SetValue(const std::string &value) { value_ = value; }

    uint64_t GetVersion() const { return version_; }
    void SetVersion(uint64_t ver) { version_ = ver; }

    bool AddField(uint64_t col, std::unique_ptr<FieldValue> &field);
    FieldValue *GetField(uint64_t col) const;

    void SetAggFields(std::string &fields) { fields_agg_ = fields; }
    std::string GetAggFields() { return fields_agg_; }

    void SetColumnInfos(std::vector<RowColumnInfo> &infos) { col_order_by_infos_ = &infos; }

    std::string hash() const {
        std::string val;
        if (col_order_by_infos_ != nullptr) {
            for (auto &it : *col_order_by_infos_) {
                val += std::to_string(it.col_id);
            }
        }

        return val;
    }

    void Reset();

    void EncodeTo(const dspb::SelectRequest &req, dspb::RowValue *to);
    void EncodeTo(const std::vector<uint64_t> &col_ids, dspb::RowValue *to);
    void EncodeTo(const std::vector<int> &off_sets, const std::vector<uint64_t> &col_ids, dspb::RowValue *to);

private:
    void CopyFrom(const RowResult &);

private:
    std::string fields_agg_;
    std::string key_;
    std::string value_;
    uint64_t version_ = 0;
    std::map<uint64_t, FieldValue *> fields_;
    std::vector<RowColumnInfo> *col_order_by_infos_ = nullptr;
};

class Decoder {
public:
    Decoder() = default;
    virtual ~Decoder() = default;

    virtual Status Decode(const std::string &key, const std::string &buf, RowResult &result) = 0;
    virtual Status DecodeAndFilter(const std::string &key, const std::string &buf, RowResult &result, bool &match) = 0;

    virtual std::string DebugString() const = 0;

    static std::unique_ptr<Decoder> CreateDecoder(const std::vector<basepb::Column> &primary_keys,
                                                  const dspb::SelectRequest &req);
    static std::unique_ptr<Decoder> CreateDecoder(const std::vector<basepb::Column> &primary_keys,
                                                  const dspb::TableRead &req);
    static std::unique_ptr<Decoder> CreateDecoder(const std::vector<basepb::Column> &primary_keys,
                                                  const dspb::IndexRead &req);
    static std::unique_ptr<Decoder> CreateDecoder(const std::vector<basepb::Column> &primary_keys,
                                                  const dspb::DataSample &req);

private:
    virtual void addExprColumn(const dspb::Expr &expr) = 0;
    virtual Status decodePrimaryKeys(const std::string &key, RowResult &result) = 0;
    virtual Status decodeFields(const std::string &buf, RowResult &result) = 0;
};

class RowDecoder : public Decoder {
public:
    RowDecoder(const std::vector<basepb::Column> &primary_keys,
               const dspb::SelectRequest &req);

    RowDecoder(const std::vector<basepb::Column> &primary_keys,
               const dspb::TableRead &req);
    RowDecoder(const std::vector<basepb::Column> &primary_keys,
               const dspb::DataSample &req);

    ~RowDecoder();

    RowDecoder(const RowDecoder &) = delete;
    RowDecoder &operator=(const RowDecoder &) = delete;

    virtual Status Decode(const std::string &key, const std::string &buf, RowResult &result);
    virtual Status DecodeAndFilter(const std::string &key, const std::string &buf, RowResult &result, bool &match);

    virtual std::string DebugString() const;

private:
    virtual void addExprColumn(const dspb::Expr &expr);
    virtual Status decodePrimaryKeys(const std::string &key, RowResult &result);
    virtual Status decodeFields(const std::string &buf, RowResult &result);

private:
    const std::vector<basepb::Column> &primary_keys_;
    std::map<uint64_t, dspb::ColumnInfo> cols_;
    std::unique_ptr<dspb::Expr> where_expr_;
};

class IndexUniqueDecoder : public Decoder {
public:
    IndexUniqueDecoder(const std::vector<basepb::Column> &primary_keys,
                       const dspb::IndexRead &req);

    ~IndexUniqueDecoder();

    IndexUniqueDecoder(const IndexUniqueDecoder &) = delete;
    IndexUniqueDecoder &operator=(const IndexUniqueDecoder &) = delete;

    virtual Status Decode(const std::string &key, const std::string &buf, RowResult &result);
    virtual Status DecodeAndFilter(const std::string &key, const std::string &buf, RowResult &result, bool &match);

    virtual std::string DebugString() const;

private:
    virtual void addExprColumn(const dspb::Expr &expr);
    virtual Status decodePrimaryKeys(const std::string &key, RowResult &result);
    virtual Status decodeFields(const std::string &buf, RowResult &result);

private:
    const std::vector<basepb::Column> &primary_keys_;
    std::vector<dspb::ColumnInfo> index_cols_;
    std::unique_ptr<dspb::IndexRead> index_read_;
    std::unordered_set<uint64_t> cols_;
};

class IndexNonUniqueDecoder : public Decoder {
public:
    IndexNonUniqueDecoder(const std::vector<basepb::Column> &primary_keys,
                          const dspb::IndexRead &req);

    ~IndexNonUniqueDecoder();

    IndexNonUniqueDecoder(const IndexNonUniqueDecoder &) = delete;
    IndexNonUniqueDecoder &operator=(const IndexNonUniqueDecoder &) = delete;

    virtual Status Decode(const std::string &key, const std::string &buf, RowResult &result);
    virtual Status DecodeAndFilter(const std::string &key, const std::string &buf, RowResult &result, bool &match);

    virtual std::string DebugString() const;

private:
    virtual void addExprColumn(const dspb::Expr &expr);
    virtual Status decodePrimaryKeys(const std::string &key, RowResult &result);
    virtual Status decodeFields(const std::string &buf, RowResult &result);

private:
    const std::vector<basepb::Column> &primary_keys_;
    std::vector<dspb::ColumnInfo> index_cols_;
    std::unique_ptr<dspb::IndexRead> index_read_;
    std::unordered_set<uint64_t> cols_;
};

} /* namespace storage */
} /* namespace ds */
} /* namespace chubaodb */

namespace std {
template <>
struct hash<chubaodb::ds::storage::RowResult> {
    size_t operator()(const chubaodb::ds::storage::RowResult &res) const noexcept {
        return std::hash<decltype(res.hash())>()(res.hash());
    }
};
} // namespace std
