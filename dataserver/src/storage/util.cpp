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
    case basepb::TinyInt:
    case basepb::SmallInt:
    case basepb::MediumInt:
    case basepb::Int:
    case basepb::BigInt:
    case basepb::Year: {
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
    case basepb::Binary: {
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

    case basepb::Decimal: {
        datatype::MyDecimal d;
        if (!DecodeDecimalAscending(key, offset, &d)) {
            return Status(Status::kCorruption,
                          std::string("decode row decimal pk failed at offset ") +
                          std::to_string(offset),
                          EncodeToHexString(key));
        }
        if (field != nullptr) {
            field->reset(new FieldValue(d));
        }
        return Status::OK();
    }

    case basepb::Date:
    case basepb::DateTime:
    case basepb::TimeStamp: {
        datatype::MyDateTime dt;
        if ( !DecodeDateAscending(key, offset, &dt)) {
            return Status(Status::kCorruption,
                          std::string("decode row datetime pk failed at offset ") +
                          std::to_string(offset),
                          EncodeToHexString(key));
        }
        if (field != nullptr) {
            field->reset(new FieldValue(dt));
        }
        return Status::OK();
    }
    case basepb::Time: {
        datatype::MyTime t;
        if ( !DecodeTimeAscending(key, offset, &t)) {
            return Status(Status::kCorruption,
                          std::string("decode row time pk failed at offset ") +
                          std::to_string(offset),
                          EncodeToHexString(key));
        }
        if (field != nullptr) {
            field->reset(new FieldValue(t));
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
    case basepb::TinyInt:
    case basepb::SmallInt:
    case basepb::MediumInt:
    case basepb::Int:
    case basepb::BigInt:
    case basepb::Year: {
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
    case basepb::Binary: {
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

    case basepb::Decimal: {
        datatype::MyDecimal d;
        if (!DecodeDecimalAscending(key, offset, &d)) {
            return Status(Status::kCorruption,
                          std::string("decode row decimal pk failed at offset ") +
                          std::to_string(offset),
                          EncodeToHexString(key));
        }
        if (field != nullptr) {
            field->reset(new FieldValue(d));
        }
        return Status::OK();
    }
    case basepb::Date:
    case basepb::DateTime:
    case basepb::TimeStamp: {
        datatype::MyDateTime dt;
        if ( !DecodeDateAscending(key, offset, &dt)) {
            return Status(Status::kCorruption,
                          std::string("decode row datetime pk failed at offset ") +
                          std::to_string(offset),
                          EncodeToHexString(key));
        }
        if (field != nullptr) {
            field->reset(new FieldValue(dt));
        }
        return Status::OK();
    }
    case basepb::Time: {
        datatype::MyTime t;
        if ( !DecodeTimeAscending(key, offset, &t)) {
            return Status(Status::kCorruption,
                          std::string("decode row time pk failed at offset ") +
                          std::to_string(offset),
                          EncodeToHexString(key));
        }
        if (field != nullptr) {
            field->reset(new FieldValue(t));
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
    case basepb::TinyInt:
    case basepb::SmallInt:
    case basepb::MediumInt:
    case basepb::Int:
    case basepb::BigInt:
    case basepb::Year: {
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
    case basepb::Binary: {
        std::string s;
        if (!DecodeBytesValue(buf, offset, &s)) {
            return Status(Status::kCorruption,
                          std::string("decode row string value failed at offset ") + std::to_string(offset),
                          EncodeToHexString(buf));
        }
        field.reset(new FieldValue(std::move(s)));
        return Status::OK();
    }

    case basepb::Decimal: {
        datatype::MyDecimal d;
        if (!DecodeDecimalValue(buf, offset, &d)) {
            return Status(Status::kCorruption,
                          std::string("decode row decimal value failed at offset ") +
                          std::to_string(offset),
                          EncodeToHexString(buf));
        }
        field.reset(new FieldValue(d));
        return Status::OK();
    }

    case basepb::Date:
    case basepb::DateTime:
    case basepb::TimeStamp: {
        datatype::MyDateTime dt;
        if (!DecodeDateValue(buf, offset, &dt)) {
            return Status(Status::kCorruption,
                          std::string("decode row datetime value failed at offset ") +
                          std::to_string(offset),
                          EncodeToHexString(buf));
        }
        field.reset(new FieldValue(dt));
        return Status::OK();
    }
    case basepb::Time: {
        datatype::MyTime t;
        if (!DecodeTimeValue(buf, offset, &t)) {
            return Status(Status::kCorruption,
                          std::string("decode row time value failed at offset ") +
                          std::to_string(offset),
                          EncodeToHexString(buf));
        }

        field.reset(new FieldValue(t));
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
    case basepb::TinyInt:
    case basepb::SmallInt:
    case basepb::MediumInt:
    case basepb::Int:
    case basepb::BigInt:
    case basepb::Year:
        expr->set_expr_type(col.unsigned_() ? dspb::Const_UInt : dspb::Const_Int);
        break;
    case basepb::Float:
    case basepb::Double:
        expr->set_expr_type(dspb::Const_Double);
        break;
    case basepb::Varchar:
    case basepb::Binary:
        expr->set_expr_type(dspb::Const_Bytes);
        break;
    case basepb::Decimal:
        expr->set_expr_type(dspb::Const_Decimal);
        break;
    case basepb::Date:
    case basepb::DateTime:
    case basepb::TimeStamp:
        expr->set_expr_type(dspb::Const_Date);
        break;
    case basepb::Time:
        expr->set_expr_type(dspb::Const_Time);
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
    case dspb::Const_Decimal: {
        datatype::MyDecimal d;
        std::string str(expr.value());
        int32_t error = d.FromString(str);
        if (error != datatype::E_DEC_OK) {
            val.reset(nullptr);
        } else {
            auto fv = new FieldValue(d);
            val.reset( new ScopedFV(fv, false));
        }
        break;
    }
    case dspb::Const_Date: {
        datatype::MyDateTime dt;
        datatype::MyDateTime::StWarn st;

        std::string str_tmp(expr.value());
        bool ret_b = dt.FromString( str_tmp, st);
        if ( !ret_b ) {
            dt.SetZero();
        }

        auto fv = new FieldValue(dt);
        val.reset(new ScopedFV(fv, false));
        break;
    }
    case dspb::Const_Time: {
        datatype::MyTime t;
        datatype::MyTime::StWarn st;

        std::string str_tmp(expr.value());
        bool ret_b = t.FromString( str_tmp, st);
        if (!ret_b) {
            t.SetZero();
        }

        auto fv = new FieldValue(t);
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
                    " Int arithmetic operation type Confflict ",
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

    case dspb::PlusDecimal:
    case dspb::MinusDecimal:
    case dspb::MultDecimal:
    case dspb::DivDecimal:
    case dspb::ModDecimal: {

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
        if ( lc->val->Type() != FieldType::kDecimal ||
             rc->val->Type() != FieldType::kDecimal ) {
            return Status(Status::kTypeConflict,
                          "Decimal arithmetic operation type Confflict ",
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
                    "CastIntToReal type Confflict ",
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
                    "CastIntToString type Confflict ",
                    lc->val->TypeString()
                    );
        }

        val.reset(new ScopedFV( new FieldValue(lc->val->ToString()) , false));
        break;
    }

    case dspb::CastIntToDecimal: {
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
                          "CastIntToDecimal type Confflict ",
                          lc->val->TypeString()
            );
        }

        datatype::MyDecimal d;
        d.FromInt(lc->val->Int());

        val.reset(new ScopedFV( new FieldValue(d) , false));
        break;
    }

    case dspb::CastIntToDate: {
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
                          "CastIntToDate type Conflict ",
                          lc->val->TypeString()
            );
        }

        datatype::MyDateTime dt;
        datatype::MyDateTime::StWarn st;
        bool ret_b = dt.FromNumberUint64(lc->val->Int(), st);
        if (!ret_b) {
            dt.SetZero();
        }

        val.reset(new ScopedFV( new FieldValue(dt) , false));
        break;
    }

    case dspb::CastIntToTime: {

        if ( expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kInt ) {
             return Status(Status::kTypeConflict,
                     "CastIntToTime type Conflict",
                     lc->val->TypeString());
        }

        datatype::MyTime t;
        datatype::MyTime::StWarn st;

        bool ret_b = t.FromNumberInt64(lc->val->Int(), st);
        if (!ret_b) {
            t.SetZero();
        }

        val.reset(new ScopedFV( new FieldValue(t), false));
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
                    "CastRealToInt type Confflict ",
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
                    "CastRealToReal type Confflict ",
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

    case dspb::CastRealToDecimal: {
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
                          "CastRealToDecimal type Confflict ",
                          lc->val->TypeString()
            );
        }

        double dval = lc->val->Double();
        datatype::MyDecimal dec;
        dec.FromFloat64(dval);

        val.reset(new ScopedFV( new FieldValue(dec), false));
        break;
    }

    case dspb::CastRealToDate: {

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
                          "CastRealToDate type Confflict ",
                          lc->val->TypeString()
            );
        }

        double dval = lc->val->Double();
        datatype::MyDateTime dt;
        datatype::MyDateTime::StWarn st;
        bool ret_b = dt.FromNumberFloat64(dval, st);
        if (!ret_b) {
            dt.SetZero();
        }

        val.reset(new ScopedFV( new FieldValue(dt), false));
        break;
    }

    case dspb::CastRealToTime: {

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
                          "CastRealToTime type Confflict ",
                          lc->val->TypeString()
            );
        }

        double dval = lc->val->Double();
        datatype::MyTime t;
        datatype::MyTime::StWarn st;
        bool ret_b = t.FromNumberFloat64(dval, st);
        if (!ret_b) {
            t.SetZero();
        }

        val.reset(new ScopedFV( new FieldValue(t), false));
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
                    "CastStringToInt type Confflict ",
                    lc->val->TypeString()
                    );
        }

        auto str_tmp = lc->val->Bytes();
        auto long_tmp = std::stol(str_tmp);

        val.reset(new ScopedFV(new FieldValue(static_cast<int64_t>(long_tmp)), false));
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

        auto str_tmp = lc->val->Bytes();
        auto long_tmp = strtod(str_tmp.c_str(), NULL);

        val.reset(new ScopedFV(new FieldValue(long_tmp), false ));
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
                    "CastStringToString type Confflict ",
                    lc->val->TypeString()
                    );
        }

        val.reset(new ScopedFV( new FieldValue(lc->val->ToString()), false));
        break;
    }
    case dspb::CastStringToDecimal: {

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
                              "CastStringToDecimal type Confflict ",
                              lc->val->TypeString()
                );
            }

            std::string str(lc->val->Bytes());
            datatype::MyDecimal d;
            int32_t error = d.FromString(str);
            if ( error != datatype::E_DEC_OK ) {
                val.reset(new ScopedFV(nullptr, false));
            } else {
                val.reset(new ScopedFV( new FieldValue(d), false ));
            }

           break;
        }

    case dspb::CastStringToDate: {

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
                          "CastStringToDate type Confflict ",
                          lc->val->TypeString()
            );
        }

        std::string str(lc->val->Bytes());
        datatype::MyDateTime dt;
        datatype::MyDateTime::StWarn st;
        bool ret = dt.FromString(str, st);
        if ( !ret ) {
            val.reset(new ScopedFV(nullptr, false));
        } else {
            val.reset(new ScopedFV( new FieldValue(dt), false ));
        }

        break;
    }

    case dspb::CastStringToTime: {

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
                          "CastStringToTime type Confflict ",
                          lc->val->TypeString()
            );
        }

        std::string str(lc->val->Bytes());
        datatype::MyTime t;
        datatype::MyTime::StWarn st;
        bool ret = t.FromString(str, st);
        if ( !ret ) {
            val.reset(new ScopedFV(nullptr, false));
        } else {
            val.reset(new ScopedFV( new FieldValue(t), false ));
        }

        break;
    }

    case dspb::CastDecimalToInt: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kDecimal) {
            return Status(Status::kTypeConflict,
                          "CastDecimalToInt type Confflict ",
                          lc->val->TypeString()
            );
        }

        int32_t error = 0;
        int64_t v = 0;
        lc->val->Decimal().ToInt(v, error);

        val.reset(new ScopedFV( new FieldValue(v), false ));
        break;
    }

    case dspb::CastDecimalToReal: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kDecimal) {
            return Status(Status::kTypeConflict,
                          "CastDecimalToReal type Confflict ",
                          lc->val->TypeString()
            );
        }

        double dval = 0.0;
        int32_t tmp_err = 0;
        lc->val->Decimal().ToFloat64(dval, tmp_err);

        val.reset(new ScopedFV( new FieldValue(dval), false ));
        break;
    }

    case dspb::CastDecimalToDecimal: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kDecimal) {
            return Status(Status::kTypeConflict,
                          "CastDecimalToDecimal type Confflict ",
                          lc->val->TypeString()
            );
        }

        val.reset(new ScopedFV( new FieldValue(lc->val->Decimal()), false ));
        break;
    }

    case dspb::CastDecimalToString: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kDecimal) {
            return Status(Status::kTypeConflict,
                          "CastDecimalToString type Confflict ",
                          lc->val->TypeString()
            );
        }

        val.reset(new ScopedFV( new FieldValue(lc->val->Decimal().ToString()), false ));
        break;
    }

    case dspb::CastDecimalToDate: {
        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kDecimal) {
            return Status(Status::kTypeConflict,
                          "CastDecimalToDate type Conflict ",
                          lc->val->TypeString()
            );
        }

        auto dec_tmp = lc->val->Decimal();
        auto str_tmp = dec_tmp.ToString();
        datatype::MyDateTime dt;
        datatype::MyDateTime::StWarn st;

        bool ret_tmp = dt.FromString(str_tmp, st);
        if (!ret_tmp) {
            dt.SetZero();
        }

        val.reset(new ScopedFV( new FieldValue(dt), false));
        break;
    }

    case dspb::CastDecimalToTime: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kDecimal) {
            return Status(Status::kTypeConflict,
                          "CastDecimalToDate type Conflict ",
                          lc->val->TypeString()
            );
        }

        auto dec_tmp = lc->val->Decimal();
        auto str_tmp = dec_tmp.ToString();
        datatype::MyTime t;
        datatype::MyTime::StWarn st;

        bool ret_tmp = t.FromString(str_tmp, st);
        if (!ret_tmp) {
            t.SetZero();
        }

        val.reset(new ScopedFV( new FieldValue(t), false));
        break;
    }

    case dspb::CastDateToInt: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kDate) {
            return Status(Status::kTypeConflict,
                          "CastDecimalToDate type Conflict ",
                          lc->val->TypeString()
            );
        }

        auto dt_tmp = lc->val->Date();
        auto int64_tmp = static_cast<int64_t>(dt_tmp.ToNumberUint64());
        val.reset(new ScopedFV( new FieldValue(int64_tmp), false));
        break;
    }

    case dspb::CastDateToReal: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kDate) {
            return Status(Status::kTypeConflict,
                          "CastDecimalToDate type Conflict ",
                          lc->val->TypeString()
            );
        }

        auto dt_tmp = lc->val->Date();
        val.reset(new ScopedFV( new FieldValue( dt_tmp.ToNumberFloat64()), false));
        break;

    }

    case dspb::CastDateToString: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kDate) {
            return Status(Status::kTypeConflict,
                          "CastDateToString type Conflict ",
                          lc->val->TypeString()
            );
        }

        val.reset(new ScopedFV( new FieldValue(lc->val->Date().ToString()), false ));
        break;
    }

    case dspb::CastDateToDecimal: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kDate) {
            return Status(Status::kTypeConflict,
                          "CastDateToDecimal type Conflict ",
                          lc->val->TypeString()
            );
        }

        auto dt_tmp = lc->val->Date();
        datatype::MyDecimal dec;
        int32_t ret_tmp = dec.FromString(dt_tmp.ToNumberString());
        if ( ret_tmp != 0) {
            dec.ResetZero();
        }

        val.reset(new ScopedFV(new FieldValue(dec), false));
        break;
    }

    case dspb::CastDateToDate: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kDate) {
            return Status(Status::kTypeConflict,
                          "CastDateToDecimal type Conflict ",
                          lc->val->TypeString()
            );
        }

        auto dt_tmp = lc->val->Date();
        val.reset(new ScopedFV(new FieldValue(dt_tmp), false));
        break;
    }

    case dspb::CastDateToTime: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kDate) {
            return Status(Status::kTypeConflict,
                          "CastDateToDecimal type Conflict ",
                          lc->val->TypeString()
            );
        }

        auto dt_tmp = lc->val->Date();
        datatype::MyTime t;
        datatype::MyTime::StWarn st;
        bool ret_b = t.FromString(dt_tmp.ToNumberString(), st);
        if (!ret_b) {
            t.SetZero();
        }

        val.reset(new ScopedFV(new FieldValue(t), false));
        break;
    }

    case dspb::CastTimeToInt: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kTime) {
            return Status(Status::kTypeConflict,
                          "CastDateToDecimal type Conflict ",
                          lc->val->TypeString()
            );
        }

        auto t_tmp = lc->val->Time();
        val.reset(new ScopedFV(new FieldValue(t_tmp.ToNumberInt64()), false));
        break;
    }

    case dspb::CastTimeToReal: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kTime) {
            return Status(Status::kTypeConflict,
                          "CastDateToDecimal type Conflict ",
                          lc->val->TypeString()
            );
        }

        auto t_tmp = lc->val->Time();
        val.reset(new ScopedFV(new FieldValue(t_tmp.ToNumberFloat64()), false));
        break;
    }

    case dspb::CastTimeToString: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kTime) {
            return Status(Status::kTypeConflict,
                          "CastTimeToString type Confflict ",
                          lc->val->TypeString()
            );
        }

        val.reset(new ScopedFV( new FieldValue(lc->val->Time().ToString()), false ));
        break;
    }

    case dspb::CastTimeToDecimal: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kTime) {
            return Status(Status::kTypeConflict,
                          "CastTimeToDecimal type Conflict ",
                          lc->val->TypeString()
            );
        }

        auto t_tmp = lc->val->Time();
        datatype::MyDecimal dec;
        int32_t ret_tmp = dec.FromString(t_tmp.ToNumberString());
        if ( ret_tmp != 0) {
            dec.ResetZero();
        }

        val.reset(new ScopedFV(new FieldValue(dec), false));
        break;
    }

    case dspb::CastTimeToDate: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kTime) {
            return Status(Status::kTypeConflict,
                          "CastTimeToDate type Conflict ",
                          lc->val->TypeString()
            );
        }

        auto t_tmp = lc->val->Time();
        auto now = std::chrono::system_clock::now();
        auto in_time_t = std::chrono::system_clock::to_time_t(now);
        char buf[100] = {0};
        std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", std::localtime(&in_time_t));

        datatype::MyDateTime dt;
        datatype::MyDateTime::StWarn st;
        bool ret_b = dt.FromString(std::string(buf), st);
        if (!ret_b) {
            dt.SetZero();
        } else {
            dt.SetHour(t_tmp.GetHour());
            dt.SetMinute(t_tmp.GetMinute());
            dt.SetSecond(t_tmp.GetSecond());
            dt.SetMicroSecond(t_tmp.GetMicroSecond());
        }

        val.reset(new ScopedFV(new FieldValue(dt), false));
        break;
    }

    case dspb::CastTimeToTime: {

        if (expr.child_size() != 1) {
            return Status(Status::kInvalidArgument, "arithmetic number size", std::to_string(expr.child_size()));
        }

        ScopedFVPtr lc;
        auto s = evaluateExpr(row, expr.child(0), lc);
        if (!s.ok()) {
            return s;
        }

        if ( lc->val->Type() != FieldType::kTime) {
            return Status(Status::kTypeConflict,
                          "CastTimeToTime type Conflict ",
                          lc->val->TypeString()
            );
        }

        auto t_tmp = lc->val->Time();
        val.reset(new ScopedFV(new FieldValue(t_tmp), false));
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
    case dspb::EqualDecimal:
    case dspb::EqualString:
    case dspb::EqualDate:
    case dspb::EqualTime:
        matched = fcompare(*left, *right, CompareOp::kEqual);
        break;
    case dspb::NotEqual:
    case dspb::NotEqualInt:
    case dspb::NotEqualDecimal:
    case dspb::NotEqualReal:
    case dspb::NotEqualString:
    case dspb::NotEqualDate:
    case dspb::NotEqualTime:
        matched = !fcompare(*left, *right, CompareOp::kEqual);
        break;
    case dspb::Less:
    case dspb::LessInt:
    case dspb::LessDecimal:
    case dspb::LessReal:
    case dspb::LessString:
    case dspb::LessDate:
    case dspb::LessTime:
        matched = fcompare(*left, *right, CompareOp::kLess);
        break;
    case dspb::LessOrEqual:
    case dspb::LessOrEqualInt:
    case dspb::LessOrEqualDecimal:
    case dspb::LessOrEqualReal:
    case dspb::LessOrEqualString:
    case dspb::LessOrEqualDate:
    case dspb::LessOrEqualTime:
        matched = fcompare(*left, *right, CompareOp::kLess) ||
                  fcompare(*left, *right, CompareOp::kEqual);
        break;
    case dspb::Larger:
    case dspb::GreaterInt:
    case dspb::GreaterDecimal:
    case dspb::GreaterReal:
    case dspb::GreaterString:
    case dspb::GreaterDate:
    case dspb::GreaterTime:
        matched = fcompare(*left, *right, CompareOp::kGreater);
        break;
    case dspb::LargerOrEqual:
    case dspb::GreaterOrEqualInt:
    case dspb::GreaterOrEqualDecimal:
    case dspb::GreaterOrEqualReal:
    case dspb::GreaterOrEqualString:
    case dspb::GreaterOrEqualDate:
    case dspb::GreaterOrEqualTime:
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
    case ::dspb::LessReal:
    case ::dspb::LessOrEqual:
    case ::dspb::LessOrEqualReal:
    case ::dspb::Larger:
    case ::dspb::LargerOrEqual:
    case ::dspb::GreaterReal:
    case ::dspb::GreaterOrEqualReal: {
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

    case ::dspb::EqualDecimal:
    case ::dspb::NotEqualDecimal:
    case ::dspb::LessDecimal:
    case ::dspb::LessOrEqualDecimal:
    case ::dspb::GreaterDecimal:
    case ::dspb::GreaterOrEqualDecimal: {
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

        if ((lc->val->Type() != FieldType::kDecimal) ||
            (rc->val->Type() != FieldType::kDecimal)
                ) {
            matched = false;
            return Status( Status::kTypeConflict,
                           " compare type conflict ",
                           "lc type:[" + lc->val->TypeString() +"], rc type:[" + rc->val->TypeString() + "]"
            );
        }


        return compareExpr(lc->val, rc->val, expr.expr_type(), matched);
    }

    case ::dspb::EqualDate:
    case ::dspb::NotEqualDate:
    case ::dspb::LessDate:
    case ::dspb::LessOrEqualDate:
    case ::dspb::GreaterDate:
    case ::dspb::GreaterOrEqualDate: {

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

        if ((lc->val->Type() != FieldType::kDate) ||
            (rc->val->Type() != FieldType::kDate)
                ) {
            matched = false;
            return Status( Status::kTypeConflict,
                           " compare type conflict ",
                           "lc type:[" + lc->val->TypeString() +"], rc type:[" + rc->val->TypeString() + "]"
            );
        }


        return compareExpr(lc->val, rc->val, expr.expr_type(), matched);
    }

    case ::dspb::EqualTime:
    case ::dspb::NotEqualTime:
    case ::dspb::LessTime:
    case ::dspb::LessOrEqualTime:
    case ::dspb::GreaterTime:
    case ::dspb::GreaterOrEqualTime: {

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

        if ((lc->val->Type() != FieldType::kTime) ||
            (rc->val->Type() != FieldType::kTime)
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
