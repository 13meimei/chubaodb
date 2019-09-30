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

#include "sql_parser.h"

#include "storage/util.h"

namespace chubaodb {
namespace test {
namespace helper {

using namespace chubaodb::ds::storage;

static dspb::ExprType parseOpType(hsql::OperatorType type) {
    switch (type) {
    case hsql::kOpAnd:
        return dspb::LogicAnd;
    case hsql::kOpOr:
        return dspb::LogicOr;
    case hsql::kOpNot:
        return dspb::LogicNot;
    case hsql::kOpEquals:
        return dspb::Equal;
    case hsql::kOpNotEquals:
        return dspb::NotEqual;
    case hsql::kOpLess:
        return dspb::Less;
    case hsql::kOpLessEq:
        return dspb::LessOrEqual;
    case hsql::kOpGreater:
        return dspb::Larger;
    case hsql::kOpGreaterEq:
        return dspb::LargerOrEqual;
    case hsql::kOpPlus:
        return dspb::Plus;
    case hsql::kOpMinus:
        return dspb::Minus;
    case hsql::kOpAsterisk:
        return dspb::Mult;
    case hsql::kOpSlash:
        return dspb::Div;
    default:
        throw std::runtime_error(std::string("unsupported op type: ") + std::to_string(type));
    }
}

static void parseOperatorExpr(const Table& t, hsql::Expr* from, dspb::Expr* to) {
    assert(from->type == hsql::kExprOperator);
    to->set_expr_type(parseOpType(from->opType));
    if (from->expr != nullptr) {
        parseExpr(t, from->expr, to->add_child());
    }
    if (from->expr2 != nullptr) {
        parseExpr(t, from->expr2, to->add_child());
    }
}

void parseExpr(const Table& t, hsql::Expr* from, dspb::Expr* to) {
    assert(from != nullptr);
    assert(to != nullptr);

    switch (from->type) {
    case hsql::kExprLiteralFloat:
        to->set_expr_type(dspb::Const_Double);
        to->set_value(std::to_string(from->fval));
        break;
    case hsql::kExprLiteralString:
        to->set_expr_type(dspb::Const_Bytes);
        to->set_value(from->name);
        break;
    case hsql::kExprLiteralInt:
        to->set_expr_type(dspb::Const_Int);
        to->set_value(std::to_string(from->ival));
        break;
    case hsql::kExprColumnRef: {
        to->set_expr_type(dspb::Column);
        auto col = t.GetColumn(from->name);
        fillColumnInfo(col, to->mutable_column());
        break;
    }
    case hsql::kExprOperator:
        parseOperatorExpr(t, from, to);
        break;
    default:
        throw std::runtime_error(std::string("unsupported expr type: ") + std::to_string(from->type));
    }
}

static int64_t parseMustConstInt(hsql::Expr* expr) {
    assert(expr != nullptr);
    if (!expr->isType(hsql::kExprLiteralInt)) {
        throw std::runtime_error(std::string("not literal int expr: ") + std::to_string(expr->type));
    }
    return expr->ival;
}

void parseLimit(hsql::LimitDescription* from, dspb::Limit* to) {
    if (from->limit != nullptr) {
        to->set_count(parseMustConstInt(from->limit));
    }
    if (from->offset != nullptr) {
        to->set_offset(parseMustConstInt(from->offset));
    }
}

void parseFieldList(const Table& t, const hsql::SelectStatement* sel, dspb::SelectRequest* req) {
    if (sel->selectList == nullptr) {
        throw std::runtime_error("invalid select field list");
    }

    for (auto expr: *sel->selectList) {
        switch (expr->type) {
        case hsql::kExprStar: {
            auto cols = t.GetAllColumns();
            for (const auto& col : cols) {
                auto field = req->add_field_list();
                field->set_typ(dspb::SelectField_Type_Column);
                fillColumnInfo(col, field->mutable_column());
            }
            break;
        }
        case hsql::kExprColumnRef: {
            auto field = req->add_field_list();
            field->set_typ(dspb::SelectField_Type_Column);
            auto col = t.GetColumn(expr->name);
            fillColumnInfo(col, field->mutable_column());
            break;
        }
        case hsql::kExprFunctionRef: {
            auto field = req->add_field_list();
            field->set_typ(dspb::SelectField_Type_AggreFunction);
            field->set_aggre_func(expr->name);
            if (expr->exprList == nullptr || expr->exprList->size() != 1) {
                throw std::runtime_error("expect aggragate parameter size = 1");
            }
            auto para_expr = (*expr->exprList)[0];
            switch (para_expr->type) {
            case hsql::kExprStar:
                break;
            case hsql::kExprColumnRef: {
                auto col = t.GetColumn(para_expr->name);
                fillColumnInfo(col, field->mutable_column());
                break;
            }
            default:
                throw std::runtime_error(std::string("unknown aggre paramenter type: ") + std::to_string(para_expr->type));
            }
            break;
        }
        default:
            throw std::runtime_error(std::string("unknown select field list expr type: ") + std::to_string(expr->type));
        }
    }
}

} /* namespace helper */
} /* namespace test */
} /* namespace chubaodb */
