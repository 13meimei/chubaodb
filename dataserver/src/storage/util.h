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

#include "base/status.h"
#include "basepb/basepb.pb.h"
#include "dspb/txn.pb.h"
#include "field_value.h"
#include "row_decoder.h"

namespace chubaodb {
namespace ds {
namespace storage {

uint64_t calExpireAt(uint64_t ttl);
bool isExpired(uint64_t expired_at);

Status decodePK(const std::string& key, size_t& offset, const basepb::Column& col,
        std::unique_ptr<FieldValue>* field);

Status decodePK(const std::string& key, size_t& offset, const dspb::ColumnInfo& col,
        std::unique_ptr<FieldValue>* field);

Status decodeField(const std::string& buf, size_t& offset, const dspb::ColumnInfo& col,
        std::unique_ptr<FieldValue>& field);

Status decodeIndexUniqueKey(const std::string& key, size_t& offset, const dspb::ColumnInfo& col,
        std::unique_ptr<FieldValue>* field, bool &hav_null);

Status decodeIndexUniqueField(const std::string& buf, size_t& offset, const dspb::ColumnInfo& col,
        std::unique_ptr<FieldValue>& field);

Status decodeIndexNonUniqueKey(const std::string& key, size_t& offset, const dspb::ColumnInfo& col,
        std::unique_ptr<FieldValue>* field);

Status decodeIndexNonUniqueField(const std::string& buf, size_t& offset, const dspb::ColumnInfo& col,
        std::unique_ptr<FieldValue>& field);

void fillColumnInfo(const basepb::Column& col, dspb::ColumnInfo* info);
void makeColumnExpr(const basepb::Column& col, dspb::Expr* expr);
void makeConstValExpr(const basepb::Column& col, const std::string& value, dspb::Expr* expr);

Status filterExpr(const RowResult& row, const dspb::Expr& expr, bool& matched);

} /* namespace storage */
} /* namespace ds */
} /* namespace chubaodb */
