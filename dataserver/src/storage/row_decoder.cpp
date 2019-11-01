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

#include "row_decoder.h"

#include <algorithm>
#include <sstream>

#include "common/ds_encoding.h"
#include "field_value.h"
#include "store.h"
#include "util.h"

namespace chubaodb {
namespace ds {
namespace storage {

RowResult::RowResult() {}

RowResult::RowResult(const RowResult &other) {
    CopyFrom(other);
}

RowResult& RowResult::operator=(const RowResult &other) {
    if (this == &other) return *this;

    CopyFrom(other);

    return *this;
}

void RowResult::CopyFrom(const RowResult &other) {
    fields_agg_ = other.fields_agg_;
    key_ = other.key_;
    value_ = other.value_;
    version_ = other.version_;
    col_order_by_infos_ = other.col_order_by_infos_;
    for (const auto &it : other.fields_) {
        fields_[it.first] = CopyValue(*it.second);
    }
}

RowResult::~RowResult() {
    Reset();
}

bool RowResult::AddField(uint64_t col, std::unique_ptr<FieldValue>& field) {
    auto ret = fields_.emplace(col, field.get()).second;
    if (ret) {
        auto p = field.release();
        auto q = fields_[col];
        ret = fcompare(*p, *q, CompareOp::kEqual);
        (void)p;
    }
    return ret;
}

FieldValue* RowResult::GetField(uint64_t col) const {
    auto it = fields_.find(col);
    if (it != fields_.cend()) {
        return it->second;
    } else {
        return nullptr;
    }
}

void RowResult::Reset() {
    key_.clear();
    value_.clear();
    version_ = 0;
    std::for_each(fields_.begin(), fields_.end(),
                  [](std::map<uint64_t, FieldValue*>::value_type& p) { delete p.second; });
    fields_.clear();
}

void RowResult::EncodeTo(const dspb::SelectRequest& req, dspb::RowValue* to) {
    std::string buf;
    for (const auto& field: req.field_list()) {
        if (field.has_column()) {
            auto fv = GetField(field.column().id());
            EncodeFieldValue(&buf, fv);
        }
    }
    to->set_fields(buf);
    to->set_version(version_);
}

void RowResult::EncodeTo(const std::vector<uint64_t> &col_ids, dspb::RowValue* to) {
    std::string buf;
    for (const auto& col_id: col_ids) {
        auto fv = GetField(col_id);
        EncodeFieldValue(&buf, fv);
    }

    if (col_ids.empty()) {
        buf = value_;
    }
    to->set_fields(buf);
    to->set_version(version_);
}

void RowResult::EncodeTo(const std::vector<int> &off_sets, const std::vector<uint64_t> &col_ids, dspb::RowValue* to)
{
    std::string buf;

    if (fields_agg_.empty()) {
        for (const auto &i : off_sets) {
            auto col_id = col_ids.at(i);
            auto fv = GetField(col_id);
            EncodeFieldValue(&buf, fv);
        }

    } else {
        // for aggregation
        buf += fields_agg_;
    }

    to->set_fields(buf);
    to->set_version(version_);
}

std::unique_ptr<Decoder> Decoder::CreateDecoder( const std::vector<basepb::Column>& primary_keys,
        const dspb::SelectRequest& req)
{
    return std::unique_ptr<Decoder>(dynamic_cast<Decoder*> (
            new RowDecoder( primary_keys, req )
            ));
}

std::unique_ptr<Decoder> Decoder::CreateDecoder(const std::vector<basepb::Column>& primary_keys,
        const dspb::TableRead & req)
{
    return std::unique_ptr<Decoder>(dynamic_cast<Decoder*> (
            new RowDecoder( primary_keys, req )
            ));
}

std::unique_ptr<Decoder> Decoder::CreateDecoder(const std::vector<basepb::Column>& primary_keys,
        const dspb::IndexRead & req)
{
    if (req.unique()) {
        return std::unique_ptr<Decoder>(dynamic_cast<Decoder*> (
                new IndexUniqueDecoder( primary_keys, req )
                ));
    } else {
        return std::unique_ptr<Decoder>(dynamic_cast<Decoder*> (
                new IndexNonUniqueDecoder( primary_keys, req )
                ));
    }
}

std::unique_ptr<Decoder> Decoder::CreateDecoder(const std::vector<basepb::Column>& primary_keys,
        const dspb::DataSample& req)
{
        return std::unique_ptr<Decoder>(dynamic_cast<Decoder*> (
                new RowDecoder( primary_keys, req )
                ));

}

RowDecoder::RowDecoder(const std::vector<basepb::Column>& primary_keys,
        const dspb::SelectRequest& req) : primary_keys_(primary_keys) {
    if (req.has_where_expr()) {
        where_expr_.reset(new dspb::Expr(req.where_expr()));
    }
    for (const auto& field: req.field_list()) {
        if (field.has_column()) {
            cols_.emplace(field.column().id(), field.column());
        }
    }
    if (where_expr_) {
        addExprColumn(*where_expr_);
    }
}

RowDecoder::RowDecoder(const std::vector<basepb::Column>& primary_keys,
                       const dspb::TableRead & req) : primary_keys_(primary_keys) {

    for (const auto& col: req.columns()) {
        cols_.emplace(col.id(), col);
    }
}

RowDecoder::RowDecoder(const std::vector<basepb::Column>& primary_keys,
                       const dspb::DataSample & req) : primary_keys_(primary_keys) {

    for (const auto& col: req.columns()) {
        cols_.emplace(col.id(), col);
    }
}
void RowDecoder::addExprColumn(const dspb::Expr& expr) {
    if (expr.expr_type() == dspb::Column) {
        cols_.emplace(expr.column().id(), expr.column());
    }
    for (const auto& child: expr.child()) {
        addExprColumn(child);
    }
}


RowDecoder::~RowDecoder() = default;

Status RowDecoder::decodePrimaryKeys(const std::string& key, RowResult& result) {
    if (key.size() <= kRowPrefixLength) {
        return Status(Status::kCorruption, "insufficient row key length", EncodeToHexString(key));
    }
    size_t offset = kRowPrefixLength;
    assert(!primary_keys_.empty());
    Status status;
    for (const auto& column: primary_keys_) {
        std::unique_ptr<FieldValue> value;
        auto it = cols_.find(column.id());
        if (it != cols_.end()) {
            status = decodePK(key, offset, column, &value);
        } else {
            status = decodePK(key, offset, column, nullptr);
        }
        if (!status.ok()) {
            return status;
        }
        if (value != nullptr) {
            if (!result.AddField(column.id(), value)) {
                return Status(Status::kDuplicate, "repeated field on column", column.name());
            }
        }
    }
    return Status::OK();
}

Status RowDecoder::decodeFields(const std::string& buf, RowResult& result) {
    uint32_t col_id = 0;
    EncodeType enc_type;
    size_t tag_offset = 0;
    for (size_t offset = 0; offset < buf.size();) {
        tag_offset = offset;
        if (!DecodeValueTag(buf, tag_offset, &col_id, &enc_type)) {
            return Status(Status::kCorruption,
                          std::string("decode row value tag failed at offset ") + std::to_string(offset),
                          EncodeToHexString(buf));
        }
        if (col_id == kVersionColumnID) {
            int64_t version = 0;
            if (DecodeIntValue(buf, offset, &version)) {
                result.SetVersion(static_cast<uint64_t>(version));
            } else {
                return Status(Status::kCorruption,
                              std::string("decode version value failed at offset ") + std::to_string(offset),
                              EncodeToHexString(buf));
            }
            continue;
        }
        auto it = cols_.find(col_id);
        if (it == cols_.end()) {
            if (!SkipValue(buf, offset)) {
                return Status(Status::kCorruption,
                              std::string("decode skip value tag failed at offset ") + std::to_string(offset),
                              EncodeToHexString(buf));
            }
            continue;
        }
        std::unique_ptr<FieldValue> value;
        auto s = decodeField(buf, offset, it->second, value);
        if (!s.ok()) {
            return s;
        }
        if (!result.AddField(it->first, value)) {
            return Status(Status::kDuplicate, "repeated field on column", std::to_string(it->second.id()));
        }
    }
    return Status::OK();
}

Status RowDecoder::Decode(const std::string& key, const std::string& buf, RowResult& result) {

    auto s = decodePrimaryKeys(key, result);
    if (!s.ok()) {
        return s;
    }

    s = decodeFields(buf, result);
    if (!s.ok()) {
        return s;
    }

    result.SetKey(key);
    result.SetValue(buf);
    return Status::OK();
}

Status RowDecoder::DecodeAndFilter(const std::string& key, const std::string& buf,
                                   RowResult& result, bool& matched) {
    auto s = Decode(key, buf, result);
    if (!s.ok()) {
        return s;
    }

    matched = true;
    if (where_expr_) {
        return filterExpr(result, *where_expr_, matched);
    } else {
        return Status::OK();
    }
}

std::string RowDecoder::DebugString() const {
    std::ostringstream ss;
    ss << "filters: " << (where_expr_ ? where_expr_->ShortDebugString() : "");
    return ss.str();
}

IndexUniqueDecoder::IndexUniqueDecoder(const std::vector<basepb::Column>& primary_keys,
                       const dspb::IndexRead & req) : primary_keys_(primary_keys) {

    for ( const auto &col: req.columns()) {
        bool found = false;
        for (const auto col_pri : primary_keys_) {
            if ( col.id()  == col_pri.id()) {
                found = true;
                break;
            }
        }

        if (!found) {
            index_cols_.push_back(col);
        }

        cols_.emplace(col.id());
    }

    index_read_.reset(new dspb::IndexRead());
    index_read_->CopyFrom(req);
}

void IndexUniqueDecoder::addExprColumn(const dspb::Expr& expr) {
/*
   if (expr.expr_type() == dspb::Column) {
        //index_cols_.emplace(expr.column().id(), expr.column());
        index_cols.push_back(expr.column());
    }
    for (const auto& child: expr.child()) {
        addExprColumn(child);
    }
*/
}

IndexUniqueDecoder::~IndexUniqueDecoder() = default;

Status IndexUniqueDecoder::decodePrimaryKeys(const std::string& key, RowResult& result) {
    if (key.size() <= kRowPrefixLength) {
        return Status(Status::kCorruption, "insufficient index-unique key length", EncodeToHexString(key));
    }
    size_t offset = kRowPrefixLength;
    assert(!index_cols_.empty());
    assert(!primary_keys_.empty());
    Status s;
    bool hav_null = false;

    int64_t index_id = 0;
    auto flg = DecodeVarintAscending(key, offset, &index_id);
    if (!flg) {
        s = Status(
                Status::kCorruption,
                std::string("decode index-key index_id failed at offset ") + std::to_string(offset),
                EncodeToHexString(key));
    }

    cols_.emplace(index_id);

    for (const auto & column: index_cols_) {
        std::unique_ptr<FieldValue> value;
        auto it = cols_.find(column.id());
        if ( it != cols_.end() ) {
            s = decodeIndexUniqueKey(key, offset, column, &value, hav_null);
        } else {
            s = decodeIndexUniqueKey(key, offset, column, nullptr, hav_null);
        }

        if (!s.ok()) {
            return s;
        }

        if ( value != nullptr ) {
            if (!result.AddField(column.id(), value)) {
                return Status(Status::kDuplicate, "repeated field on column", std::to_string(column.id()));
            }
        }
    }

    if (hav_null && s.ok()) {
        for (const auto& column: primary_keys_) {
            std::unique_ptr<FieldValue> value;
            auto it = cols_.find(column.id());
            if (it != cols_.end()) {
                s = decodePK(key, offset, column, &value);
            } else {
                s = decodePK(key, offset, column, nullptr);
            }
            if (!s.ok()) {
                return s;
            }
            if (value != nullptr) {
                if (!result.AddField(column.id(), value)) {
                    return Status(Status::kDuplicate, "repeated field on column", column.name());
                }
            }
        }
    }

    return s;
}

Status IndexUniqueDecoder::decodeFields(const std::string& buf, RowResult& result) {
    Status s;
    uint32_t col_id = 0;
    EncodeType enc_type;
    size_t offset = 0;
    size_t tag_offset = 0;

    for (size_t offset = 0; offset < buf.size();) {
        tag_offset = offset;
        if (!DecodeValueTag(buf, tag_offset, &col_id, &enc_type)) {
            return Status(Status::kCorruption,
                          std::string("decode row value tag failed at offset ") + std::to_string(offset),
                          EncodeToHexString(buf));
        }
        if (col_id == kVersionColumnID) {
            int64_t version = 0;
            if (DecodeIntValue(buf, offset, &version)) {
                result.SetVersion(static_cast<uint64_t>(version));
            } else {
                return Status(Status::kCorruption,
                              std::string("decode version value failed at offset ") + std::to_string(offset),
                              EncodeToHexString(buf));
            }
            continue;
        } else {

            auto it = cols_.find(col_id);
            if ( it == cols_.end()) {
                if ( !SkipValue(buf, offset)) {
                    return Status(Status::kCorruption,
                                  std::string("decode skip value tag failed at offset ") + std::to_string(offset),
                                  EncodeToHexString(buf));
                }
                continue;
            }

            std::string row_id;
            size_t offset_inner = 0;
            if (DecodeBytesValue(buf, offset, &row_id)) {
                // skip '\x0' + table_id
                offset_inner += kRowPrefixLength;

                for (const auto& column: primary_keys_) {
                    std::unique_ptr<FieldValue> value;
                    s = decodePK(row_id, offset_inner, column, &value);
                    if (!s.ok()) {
                        return s;
                    }

                    if (value != nullptr) {
                        if (!result.AddField(column.id(), value)) {
                            return Status(Status::kDuplicate, "repeated field on column", column.name());
                        }
                    }
                }
            } else {
                return Status(Status::kCorruption,
                        std::string("decode version value failed at offset ") + std::to_string(offset),
                        EncodeToHexString(buf)
                );
            }
        }
    }

    return s;
}

Status IndexUniqueDecoder::Decode(const std::string& key, const std::string& buf, RowResult& result) {

    auto s = decodePrimaryKeys(key, result);
    if (!s.ok()) {
        return s;
    }

    s = decodeFields(buf, result);
    if (!s.ok()) {
        return s;
    }

    result.SetKey(key);
    result.SetValue(buf);
    return Status::OK();
}

Status IndexUniqueDecoder::DecodeAndFilter(const std::string& key, const std::string& buf,
                                   RowResult& result, bool& matched) {

    Status s;
    matched = true;
    s = Decode(key, buf, result);
    return s;
}

std::string IndexUniqueDecoder::DebugString() const {
    std::ostringstream ss;
    ss << "index unique, request info : " << ( index_read_ ? index_read_->ShortDebugString() : "");
    return ss.str();
}

IndexNonUniqueDecoder::IndexNonUniqueDecoder(const std::vector<basepb::Column>& primary_keys,
                       const dspb::IndexRead & req) : primary_keys_(primary_keys) {

    for ( const auto &col: req.columns()) {
        bool found = false;
        for (const auto col_pri : primary_keys_) {
            if ( col.id()  == col_pri.id()) {
                found = true;
                break;
            }
        }

        if (!found) {
            index_cols_.push_back(col);
        }

        cols_.emplace(col.id());
    }

    index_read_.reset(new dspb::IndexRead());
    index_read_->CopyFrom(req);
}

void IndexNonUniqueDecoder::addExprColumn(const dspb::Expr& expr) {
/*
    if (expr.expr_type() == dspb::Column) {
//        index_cols_.emplace(expr.column().id(), expr.column());
        index_cols_.push_back(expr.column());
    }
    for (const auto& child: expr.child()) {
        addExprColumn(child);
    }
*/
}

IndexNonUniqueDecoder::~IndexNonUniqueDecoder() = default;

Status IndexNonUniqueDecoder::decodePrimaryKeys(const std::string& key, RowResult& result) {
    if (key.size() <= kRowPrefixLength) {
        return Status(Status::kCorruption, "insufficient index-unique key length", EncodeToHexString(key));
    }
    size_t offset = kRowPrefixLength;
    assert(!index_cols_.empty());
    assert(!primary_keys_.empty());
    Status s;
    int64_t index_id = 0;
    auto flg = DecodeVarintAscending(key, offset, &index_id);
    if (!flg) {
        s = Status(
                Status::kCorruption,
                std::string("decode index-key index_id failed at offset ") + std::to_string(offset),
                EncodeToHexString(key));
    }

    cols_.emplace(index_id);

    for (const auto & column: index_cols_) {
        std::unique_ptr<FieldValue> value;
        auto it = cols_.find(column.id());

        if ( it != cols_.end()) {
            s = decodeIndexNonUniqueKey(key, offset, column, &value);
        } else {
            s = decodeIndexNonUniqueKey(key, offset, column, nullptr);
        }

        if (!s.ok()) {
            break;
        }

        if ( value != nullptr ) {
            if (!result.AddField(column.id(), value)) {
                return Status(Status::kDuplicate, "repeated field on column", std::to_string(column.id()));
            }
        }
    }

    if (!s.ok()) {
        return s;
    }

    // skip '\x0' + table_id
    offset += kRowPrefixLength;
    for (const auto& column: primary_keys_) {
        std::unique_ptr<FieldValue> value;
        auto it = cols_.find(column.id());
        if ( it != cols_.end() ) {
            s = decodePK(key, offset, column, &value);
        } else {
            s = decodePK(key, offset, column, nullptr);
        }
        if (!s.ok()) {
            return s;
        }
        if (value != nullptr) {
            if (!result.AddField(column.id(), value)) {
                return Status(Status::kDuplicate, "repeated field on column", column.name());
            }
        }
    }

    return s;
}

Status IndexNonUniqueDecoder::decodeFields(const std::string& buf, RowResult& result) {
    Status s;
    uint32_t col_id = 0;
    EncodeType enc_type;
    size_t offset = 0;
    size_t tag_offset = 0;

    for ( size_t offset = 0; offset < buf.size(); ) {
        tag_offset = offset;
        if (!DecodeValueTag(buf, tag_offset, &col_id, &enc_type)) {
            s = Status(Status::kCorruption,
                       std::string("decode row value tag failed at offset ") + std::to_string(offset),
                       EncodeToHexString(buf));
            return s;
        }

        if (col_id == kVersionColumnID) {
            int64_t version = 0;
            if (DecodeIntValue(buf, offset, &version)) {
                result.SetVersion(static_cast<uint64_t>(version));
            } else {
                s = Status(Status::kCorruption,
                           std::string("decode version value failed at offset ") + std::to_string(offset),
                           EncodeToHexString(buf));
                return s;
            }
            continue;
        } else {
            auto it = cols_.find(col_id);
            if ( it == cols_.end()) {
                if ( !SkipValue(buf, offset)) {
                    return Status(Status::kCorruption,
                                  std::string("decode skip value tag failed at offset ") + std::to_string(offset),
                                  EncodeToHexString(buf));
                }
                continue;
            }

            s = Status(Status::kCorruption,
                       std::string("decode version col id failed at offset ") + std::to_string(offset),
                       EncodeToHexString(buf));
            return s;
        }
    }

    return s;
}

Status IndexNonUniqueDecoder::Decode(const std::string& key, const std::string& buf, RowResult& result) {

    auto s = decodePrimaryKeys(key, result);
    if (!s.ok()) {
        return s;
    }

    s = decodeFields(buf, result);
    if (!s.ok()) {
        return s;
    }

    result.SetKey(key);
    result.SetValue(buf);
    return Status::OK();
}

Status IndexNonUniqueDecoder::DecodeAndFilter(const std::string& key, const std::string& buf,
                                   RowResult& result, bool& matched) {

    Status s;
    matched = true;
    s = Decode(key, buf, result);
    return s;

}

std::string IndexNonUniqueDecoder::DebugString() const {
    std::ostringstream ss;
    ss << "index unique, request info : " << ( index_read_ ? index_read_->ShortDebugString() : "");
    return ss.str();
}

} /* namespace storage */
} /* namespace ds */
} /* namespace chubaodb */
