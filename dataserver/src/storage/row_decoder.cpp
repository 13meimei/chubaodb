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

RowResult::~RowResult() {
    Reset();
}

bool RowResult::AddField(uint64_t col, std::unique_ptr<FieldValue>& field) {
    auto ret = fields_.emplace(col, field.get()).second;
    if (ret) {
        auto p = field.release();
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

} /* namespace storage */
} /* namespace ds */
} /* namespace chubaodb */
