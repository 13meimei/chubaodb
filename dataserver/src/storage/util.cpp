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

#include "util.h"

#include <chrono>

#include "common/ds_encoding.h"

namespace chubaodb {
namespace ds {
namespace storage {

using namespace std::chrono;

uint64_t calExpireAt(uint64_t ttl) {
    auto epoch = system_clock::now().time_since_epoch();
    return ttl + duration_cast<milliseconds>(epoch).count();
}

bool isExpired(uint64_t expired_at) {
    auto epoch = system_clock::now().time_since_epoch();
    auto now = duration_cast<milliseconds>(epoch).count();
    return static_cast<uint64_t>(now) > expired_at;
}

Status decodePK(const std::string& key, size_t& offset, const basepb::Column& col,
                std::unique_ptr<FieldValue>* field) {
    switch (col.data_type()) {
    case basepb::Tinyint:
    case basepb::Smallint:
    case basepb::Int:
    case basepb::BigInt: {
        if (col.unsigned_()) {
            uint64_t i = 0;
            if (!DecodeUvarintAscending(key, offset, &i)) {
                return Status(
                        Status::kCorruption,
                        std::string("decode row unsigned int pk failed at offset ") + std::to_string(offset),
                        EncodeToHexString(key));
            }
            if (field != nullptr) field->reset(new FieldValue(i));
        } else {
            int64_t i = 0;
            if (!DecodeVarintAscending(key, offset, &i)) {
                return Status(
                        Status::kCorruption,
                        std::string("decode row int pk failed at offset ") + std::to_string(offset),
                        EncodeToHexString(key));
            }
            if (field != nullptr) {
                field->reset(new FieldValue(i));
            }
        }
        return Status::OK();
    }

    case basepb::Float:
    case basepb::Double: {
        double d = 0;
        if (!DecodeFloatAscending(key, offset, &d)) {
            return Status(Status::kCorruption,
                          std::string("decode row float pk failed at offset ") +
                          std::to_string(offset),
                          EncodeToHexString(key));
        }
        if (field != nullptr) {
            field->reset(new FieldValue(d));
        }
        return Status::OK();
    }

    case basepb::Varchar:
    case basepb::Binary:
    case basepb::Date:
    case basepb::TimeStamp: {
        std::string s;
        if (!DecodeBytesAscending(key, offset, &s)) {
            return Status(Status::kCorruption,
                          std::string("decode row string pk failed at offset ") +
                          std::to_string(offset),
                          EncodeToHexString(key));
        }
        if (field != nullptr) {
            field->reset(new FieldValue(std::move(s)));
        }
        return Status::OK();
    }

    default:
        return Status(Status::kNotSupported, "unknown decode field type", col.name());
    }
}

Status decodePK(const std::string& key, size_t& offset, const dspb::ColumnInfo& col,
                std::unique_ptr<FieldValue>* field) {
    switch (col.typ()) {
    case basepb::Tinyint:
    case basepb::Smallint:
    case basepb::Int:
    case basepb::BigInt: {
        if (col.unsigned_()) {
            uint64_t i = 0;
            if (!DecodeUvarintAscending(key, offset, &i)) {
                return Status(
                        Status::kCorruption,
                        std::string("decode row unsigned int pk failed at offset ") + std::to_string(offset),
                        EncodeToHexString(key));
            }
            if (field != nullptr) field->reset(new FieldValue(i));
        } else {
            int64_t i = 0;
            if (!DecodeVarintAscending(key, offset, &i)) {
                return Status(
                        Status::kCorruption,
                        std::string("decode row int pk failed at offset ") + std::to_string(offset),
                        EncodeToHexString(key));
            }
            if (field != nullptr) {
                field->reset(new FieldValue(i));
            }
        }
        return Status::OK();
    }

    case basepb::Float:
    case basepb::Double: {
        double d = 0;
        if (!DecodeFloatAscending(key, offset, &d)) {
            return Status(Status::kCorruption,
                          std::string("decode row float pk failed at offset ") +
                          std::to_string(offset),
                          EncodeToHexString(key));
        }
        if (field != nullptr) {
            field->reset(new FieldValue(d));
        }
        return Status::OK();
    }

    case basepb::Varchar:
    case basepb::Binary:
    case basepb::Date:
    case basepb::TimeStamp: {
        std::string s;
        if (!DecodeBytesAscending(key, offset, &s)) {
            return Status(Status::kCorruption,
                          std::string("decode row string pk failed at offset ") +
                          std::to_string(offset),
                          EncodeToHexString(key));
        }
        if (field != nullptr) {
            field->reset(new FieldValue(std::move(s)));
        }
        return Status::OK();
    }

    default:
        return Status(Status::kNotSupported, "unknown decode field type", std::to_string(col.id()));
    }
}

Status decodeField(const std::string& buf, size_t& offset, const dspb::ColumnInfo& col,
        std::unique_ptr<FieldValue>& field) {
    switch (col.typ()) {
    case basepb::Tinyint:
    case basepb::Smallint:
    case basepb::Int:
    case basepb::BigInt: {
        int64_t i = 0;
        if (!DecodeIntValue(buf, offset, &i)) {
            return Status(
                Status::kCorruption,
                std::string("decode row int value failed at offset ") + std::to_string(offset),
                EncodeToHexString(buf));
        }
        if (col.unsigned_()) {
            field.reset(new FieldValue(static_cast<uint64_t>(i)));
        } else {
            field.reset(new FieldValue(i));
        }
        return Status::OK();
    }

    case basepb::Float:
    case basepb::Double: {
        double d = 0;
        if (!DecodeFloatValue(buf, offset, &d)) {
            return Status(Status::kCorruption,
                          std::string("decode row float value failed at offset ") + std::to_string(offset),
                          EncodeToHexString(buf));
        }
        field.reset(new FieldValue(d));
        return Status::OK();
    }

    case basepb::Varchar:
    case basepb::Binary:
    case basepb::Date:
    case basepb::TimeStamp: {
        std::string s;
        if (!DecodeBytesValue(buf, offset, &s)) {
            return Status(Status::kCorruption,
                          std::string("decode row string value failed at offset ") + std::to_string(offset),
                          EncodeToHexString(buf));
        }
        field.reset(new FieldValue(std::move(s)));
        return Status::OK();
    }

    default:
        return Status(Status::kNotSupported, "unknown decode field type", std::to_string(col.typ()));
    }
}

void fillColumnInfo(const basepb::Column& col, dspb::ColumnInfo* info) {
    info->set_id(col.id());
    info->set_typ(col.data_type());
    info->set_unsigned_(col.unsigned_());
}

void makeColumnExpr(const basepb::Column& col, dspb::Expr* expr) {
    assert(expr != nullptr);
    expr->set_expr_type(dspb::Column);
    auto column_info = expr->mutable_column();
    fillColumnInfo(col, column_info);
}

void makeConstValExpr(const basepb::Column& col, const std::string& value, dspb::Expr* expr) {
    assert(expr != nullptr);
    expr->set_value(value);
    switch (col.data_type()) {
    case basepb::Tinyint:
    case basepb::Smallint:
    case basepb::Int:
    case basepb::BigInt:
        expr->set_expr_type(col.unsigned_() ? dspb::Const_UInt : dspb::Const_Int);
        break;
    case basepb::Float:
    case basepb::Double:
        expr->set_expr_type(dspb::Const_Double);
        break;
    case basepb::Varchar:
    case basepb::Binary:
    case basepb::Date:
    case basepb::TimeStamp:
        expr->set_expr_type(dspb::Const_Bytes);
        break;
    default:
        expr->set_expr_type(dspb::Invalid_Expr);
    }
}

struct ScopedFV {
    FieldValue *val = nullptr;
    bool is_ref = false;

    ScopedFV(FieldValue *val_arg, bool is_ref_arg) : 
        val(val_arg), is_ref(is_ref_arg) {}

    ~ScopedFV() { if (!is_ref) { delete val;} }

    ScopedFV(const ScopedFV&) = delete;
    ScopedFV& operator=(const ScopedFV&) = delete;
};

using ScopedFVPtr = std::unique_ptr<ScopedFV>;

Status evaluateExpr(const RowResult& row, const dspb::Expr& expr, ScopedFVPtr& val) {
    switch (expr.expr_type()) {
    case dspb::Column: {
        auto fv = row.GetField(expr.column().id());
        if (fv != nullptr) {
            val.reset(new ScopedFV(fv, true));
        }
        break;
    }
    case dspb::Const_Int: {
        int64_t i = strtoll(expr.value().c_str(), NULL, 10);
        auto fv = new FieldValue(i);
        val.reset(new ScopedFV(fv, false));
        break;
    }
    case dspb::Const_UInt: {
        uint64_t i = strtoull(expr.value().c_str(), NULL, 10);
        auto fv = new FieldValue(i);
        val.reset(new ScopedFV(fv, false));
        break;
    }
    case dspb::Const_Double: {
        double d = strtod(expr.value().c_str(), NULL);
        auto fv = new FieldValue(d);
        val.reset(new ScopedFV(fv, false));
        break;
    }
    case dspb::Const_Bytes: {
        auto s = new std::string(expr.value());
        auto fv = new FieldValue(s);
        val.reset(new ScopedFV(fv, false));
        break;
    }
    case dspb::Plus:
    case dspb::PlusReal:
    case dspb::Minus:
    case dspb::MinusReal:
    case dspb::Mult:
    case dspb::MultReal:
    case dspb::Mod:
    case dspb::ModReal:
    case dspb::Div:
    case dspb::DivReal: {
        if (expr.child_size() != 2) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc, rc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }
        s = evaluateExpr(row, expr.child(1), rc);
        if (!s.ok()) {
            return s;
        }
        if (lc == nullptr || rc == nullptr) {
            return Status::OK();
        }
        auto result = arithCalc(lc->val, rc->val, expr.expr_type());
        if (result) {
            val.reset(new ScopedFV(result.release(), false));
        }
        break;
    }

    case dspb::PlusInt:
    case dspb::MinusInt:
    case dspb::MultInt:
    case dspb::MultIntUnsigned:
    case dspb::IntDivInt:
    case dspb::ModInt: {

        if (expr.child_size() != 2) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc, rc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        s = evaluateExpr(row, expr.child(1), rc);
        if (!s.ok()) {
            return s;
        }

        if (lc == nullptr || rc == nullptr) {
            return Status::OK();
        }
        if ( lc->val->Type() != FieldType::kInt  ||
            rc->val->Type() != FieldType::kInt ) {
            return Status(Status::kTypeConflict,
                    "CastIntToInt type Confflict ",
                     "lc type:[" + lc->val->TypeString() +"], rc type:[" + rc->val->TypeString() + "]"
                    );
        }

        auto result = arithCalc(lc->val, rc->val, expr.expr_type());

        if (result) {
            val.reset(new ScopedFV(result.release(), false));
        } else {
            return Status(Status::kTypeConflict,
                    "arithCalc Failed.",
                    lc->val->ToString() + " " + dspb::ExprType_Name(expr.expr_type()) + " " + rc->val->ToString()
                    );
        }

        break;
    }


    case dspb::CastIntToInt: {
        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kInt ) {
            return Status(Status::kTypeConflict,
                    "CastIntToInt type Confflict ",
                    lc->val->TypeString()
                    );
        }

        val.reset(new ScopedFV( new FieldValue(lc->val->Int()), false));
        break;
    }

    case dspb::CastIntToReal: {
        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kInt ) {
            return Status(Status::kTypeConflict,
                    "CastIntToInt type Confflict ",
                    lc->val->TypeString()
                    );
        }

        val.reset(new ScopedFV( new FieldValue(double(lc->val->Int())), false));
        break;
    }

    case dspb::CastIntToString: {
        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kInt ) {
            return Status(Status::kTypeConflict,
                    "CastIntToInt type Confflict ",
                    lc->val->TypeString()
                    );
        }

        val.reset(new ScopedFV( new FieldValue(lc->val->ToString()) , false));
        break;
    }

    case dspb::CastRealToInt: {
        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kDouble ) {
            return Status(Status::kTypeConflict,
                    "CastRealToString type Confflict ",
                    lc->val->TypeString()
                    );
        }

        val.reset(new ScopedFV( new FieldValue(int64_t(lc->val->Double())) , false));
        break;
    }

    case dspb::CastRealToReal: {
        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kDouble ) {
            return Status(Status::kTypeConflict,
                    "CastRealToString type Confflict ",
                    lc->val->TypeString()
                    );
        }

        val.reset(new ScopedFV( new FieldValue(lc->val->Double()) , false));
        break;
    }

    case dspb::CastRealToString: {
        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kDouble ) {
            return Status(Status::kTypeConflict,
                    "CastRealToString type Confflict ",
                    lc->val->TypeString()
                    );
        }

        val.reset(new ScopedFV( new FieldValue(lc->val->ToString()) , false));
        break;
    }

    case dspb::CastStringToInt: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kBytes) {
            return Status(Status::kTypeConflict,
                    "CastIntToInt type Confflict ",
                    lc->val->TypeString()
                    );
        }


        val.reset(new ScopedFV(
                new FieldValue((int64_t)std::atol(lc->val->Bytes().c_str())),
                false
                ) );
        break;

    }
    case dspb::CastStringToReal: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kBytes) {
            return Status(Status::kTypeConflict,
                    "CastStringToReal type Confflict ",
                    lc->val->TypeString()
                    );
        }


        val.reset(new ScopedFV(
                new FieldValue(std::atof(lc->val->Bytes().c_str())),
                false
                ) );
        break;

    }

    case dspb::CastStringToString: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kBytes) {
            return Status(Status::kTypeConflict,
                    "CastIntToInt type Confflict ",
                    lc->val->TypeString()
                    );
        }

        val.reset(new ScopedFV( new FieldValue(lc->val->ToString()), false));
        break;
    }

    default:
        return Status(Status::kInvalidArgument, "expr type could not evaluate", std::to_string(expr.expr_type()));
    }
    return Status::OK();
}

Status compareExpr(FieldValue* left, FieldValue* right, dspb::ExprType cmp_type, bool& matched) {
    if (left == nullptr || right == nullptr) {
        matched = false;
        return Status::OK();
    }

    switch (cmp_type) {
    case dspb::Equal:
    case dspb::EqualInt:
    case dspb::EqualReal:
    case dspb::EqualString:
        matched = fcompare(*left, *right, CompareOp::kEqual);
        break;
    case dspb::NotEqual:
    case dspb::NotEqualInt:
    case dspb::NotEqualString:
        matched = !fcompare(*left, *right, CompareOp::kEqual);
        break;
    case dspb::Less:
    case dspb::LessInt:
    case dspb::LessString:
        matched = fcompare(*left, *right, CompareOp::kLess);
        break;
    case dspb::LessOrEqual:
    case dspb::LessOrEqualInt:
    case dspb::LessOrEqualString:
        matched = fcompare(*left, *right, CompareOp::kLess) ||
                  fcompare(*left, *right, CompareOp::kEqual);
        break;
    case dspb::Larger:
    case dspb::GreaterInt:
    case dspb::GreaterString:
        matched = fcompare(*left, *right, CompareOp::kGreater);
        break;
    case dspb::LargerOrEqual:
    case dspb::GreaterOrEqualInt:
    case dspb::GreaterOrEqualString:
        matched = fcompare(*left, *right, CompareOp::kGreater) || 
                  fcompare(*left, *right, CompareOp::kEqual);
        break;
    default:
        return Status(Status::kInvalidArgument, "expr type could not compare", std::to_string(cmp_type));
    }
    return Status::OK();
}

Status filterExpr(const RowResult& row, const dspb::Expr& expr, bool& matched) {
    
    switch (expr.expr_type()) {
    case ::dspb::LogicAnd:
        if (expr.child_size() < 2) {
            return Status(Status::kInvalidArgument, "and expr child size", std::to_string(expr.child_size()));
        }
        for (const auto& child_expr: expr.child()) {
            auto s = filterExpr(row, child_expr, matched);
            if (!s.ok() || !matched) {
                return s;
            }
        }
        return Status::OK();

    case ::dspb::LogicOr:
        if (expr.child_size() < 2) {
            return Status(Status::kInvalidArgument, "or expr child size", std::to_string(expr.child_size()));
        }
        for (const auto& child_expr: expr.child()) {
            auto s = filterExpr(row, child_expr, matched);
            if (!s.ok() || matched) {
                return s;
            }
        }
        return Status::OK();

    case ::dspb::LogicNot: {
        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "not expr child size", std::to_string(expr.child_size()));
        }
        auto s = filterExpr(row, expr.child(0), matched);
        matched = !matched;
        return s;
    }

    case ::dspb::Equal:
    case ::dspb::EqualReal:
    case ::dspb::NotEqual:
    case ::dspb::NotEqualReal:
    case ::dspb::Less:
    case ::dspb::LessOrEqual:
    case ::dspb::Larger:
    case ::dspb::LargerOrEqual: {
        if (expr.child_size() != 2) {
            return Status(Status::kInvalidArgument, "compare expr child size", std::to_string(expr.child_size()));
        }
        ScopedFVPtr lc, rc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }
        s = evaluateExpr(row, expr.child(1), rc);
        if (!s.ok()) {
            return s;
        }
        if (lc == nullptr || rc == nullptr) {
            matched = false;
            return Status::OK();
        }
        return compareExpr(lc->val, rc->val, expr.expr_type(), matched);
    }

    case ::dspb::EqualInt:
    case ::dspb::NotEqualInt:
    case ::dspb::LessInt:
    case ::dspb::LessOrEqualInt:
    case ::dspb::GreaterInt:
    case ::dspb::GreaterOrEqualInt: {
        if (expr.child_size() != 2) {
            return Status(Status::kInvalidArgument, "compare expr child size", std::to_string(expr.child_size()));
        }
        ScopedFVPtr lc, rc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }
        s = evaluateExpr(row, expr.child(1), rc);
        if (!s.ok()) {
            return s;
        }
        if (lc == nullptr || rc == nullptr) {
            matched = false;
            return Status::OK();
        }

        if ((lc->val->Type() != FieldType::kInt ) ||
                (rc->val->Type() != FieldType::kInt)
        ) {
            matched = false;
            return Status( Status::kTypeConflict,
                    " compare type conflict ",
                    "lc type:[" + lc->val->TypeString() +"], rc type:[" + rc->val->TypeString() + "]"
                    );
        }


        return compareExpr(lc->val, rc->val, expr.expr_type(), matched);

    }

    case ::dspb::EqualString:
    case ::dspb::NotEqualString:
    case ::dspb::LessString:
    case ::dspb::LessOrEqualString:
    case ::dspb::GreaterString:
    case ::dspb::GreaterOrEqualString: {
        if (expr.child_size() != 2) {
            return Status(Status::kInvalidArgument, "compare expr child size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc, rc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }
        s = evaluateExpr(row, expr.child(1), rc);
        if (!s.ok()) {
            return s;
        }

        if (lc == nullptr || rc == nullptr) {
            matched = false;
            return Status::OK();
        }

        if ((lc->val->Type() != FieldType::kBytes) ||
                (rc->val->Type() != FieldType::kBytes)
        ) {
            matched = false;
            return Status( Status::kTypeConflict,
                    " compare type conflict ",
                    "lc type:[" + lc->val->TypeString() +"], rc type:[" + rc->val->TypeString() + "]"
                    );
        }

        return compareExpr(lc->val, rc->val, expr.expr_type(), matched);
    }

    default:
        return Status(Status::kInvalidArgument, "not a boolean expr type", std::to_string(expr.expr_type()));
    }
}

static const uint8_t kEncodeNull= 0x00;

Status decodeIndexUniqueKey(const std::string& key, size_t& offset, const dspb::ColumnInfo& col,
        std::unique_ptr<FieldValue>* field, bool & hav_null)
{
    Status s;

    if (offset > key.size()) {
        s = Status(
                Status::kCorruption,
                std::string("decode index unique failed at offset") + std::to_string(offset),
                EncodeToHexString(key)
                );
    }

    if (key.at(offset) == kEncodeNull) {
        offset++;

        hav_null = true;
        field = nullptr;
/*
        if (field != nullptr) {
            field->reset(nullptr);
        }
*/
    } else {
        s = decodePK(key, offset, col, field);
    }

    return s;
}

Status decodeIndexUniqueField(const std::string& buf, size_t& offset, const dspb::ColumnInfo& col,
        std::unique_ptr<FieldValue>& field)
{
    Status s;
//    s = decodePK(key, offset, col, field);
    return s;
}

Status decodeIndexNonUniqueKey(const std::string& key, size_t& offset, const dspb::ColumnInfo& col,
        std::unique_ptr<FieldValue>* field)
{

    Status s;

    if (offset < key.size()) {
        s = Status(
                Status::kCorruption,
                std::string("decode index unique failed at offset") + std::to_string(offset),
                EncodeToHexString(key)
                );
    }

    if (key.at(offset) == kEncodeNull) {
        offset++;

        if (field != nullptr) {
            field->reset(nullptr);
        }
    } else {
        s = decodePK(key, offset, col, field);
    }
    return s;
}

Status decodeIndexNonUniqueField(const std::string& buf, size_t& offset, const dspb::ColumnInfo& col,
        std::unique_ptr<FieldValue>& field)
{
    Status s;
    return s;
}


} /* namespace storage */
} /* namespace ds */
} /* namespace chubaodb */
