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

#include "aggregate_calc.h"
#include "field_value.h"

namespace chubaodb {
namespace ds {
namespace storage {

std::unique_ptr<AggreCalculator> AggreCalculator::New(const std::string& name,
                                                      const u_int64_t id) {
    if (name == "count") {
        return std::unique_ptr<AggreCalculator>(new CountCalculator(id));
    } else if (name == "min") {
        return std::unique_ptr<AggreCalculator>(new MinCalculator(id));
    } else if (name == "max") {
        return std::unique_ptr<AggreCalculator>(new MaxCalculator(id));
    } else if (name == "sum") {
        return std::unique_ptr<AggreCalculator>(new SumCalculator(id));
    } else if (name == "avg") {
        return std::unique_ptr<AggreCalculator>(new SumCalculator(id));
    } else {
        return nullptr;
    }
}

std::unique_ptr<AggreCalculator> AggreCalculator::New(dspb::ExprType type,
                                                      const u_int64_t id) {

    switch (type)
    {
    case dspb::Avg:
        return std::unique_ptr<AggreCalculator>(new AvgCalculator(id));
    case dspb::Count:
        return std::unique_ptr<AggreCalculator>(new CountCalculator(id));
    case dspb::Max:
        return std::unique_ptr<AggreCalculator>(new MaxCalculator(id));
    case dspb::Min:
        return std::unique_ptr<AggreCalculator>(new MinCalculator(id));
    case dspb::Sum:
            return std::unique_ptr<AggreCalculator>(new SumCalculator(id));
    default:
        return nullptr;
    }
}

std::unique_ptr<AggreCalculator> AggreCalculator::New(const dspb::Expr &f) {
    switch (f.expr_type())
    {
    case dspb::Avg:
        return std::unique_ptr<AggreCalculator>(new AvgCalculator(f.has_column() ? f.column().id() : 0));
    case dspb::Count:
        if (f.child_size() == 1) {
            switch (f.child(0).expr_type())
            {
            case dspb::Column:
                return std::unique_ptr<AggreCalculator>(new CountCalculator(f.child(0).column().id()));
            case dspb::Const_Int:
                return std::unique_ptr<AggreCalculator>(new CountCalculator(AggreCalculator::default_const_col_id));
            default:
                // never go here
                return nullptr;
            }
        } else {
            // count(*) or count(1) ...
            return std::unique_ptr<AggreCalculator>(new CountCalculator(AggreCalculator::default_const_col_id));
        }
        
    case dspb::Max:
        return std::unique_ptr<AggreCalculator>(new MaxCalculator(f.has_column() ? f.column().id() : 0));
    case dspb::Min:
        return std::unique_ptr<AggreCalculator>(new MinCalculator(f.has_column() ? f.column().id() : 0));
    case dspb::Sum:
            return std::unique_ptr<AggreCalculator>(new SumCalculator(f.has_column() ? f.column().id() : 0));
    default:
        return nullptr;
    }
}

CountCalculator::CountCalculator(const u_int64_t id) : AggreCalculator(id) {}
CountCalculator::~CountCalculator() {}

void CountCalculator::Add(const FieldValue* f) {
   if (f != nullptr || col_id_ == AggreCalculator::default_const_col_id) {
        ++count_;
   }
}

std::unique_ptr<FieldValue> CountCalculator::Result() {
    return std::unique_ptr<FieldValue>(new FieldValue(count_));
}

// min
MinCalculator::MinCalculator(const u_int64_t id) : AggreCalculator(id) {}
MinCalculator::~MinCalculator() {
    if (min_value_) {
        delete min_value_; 
    }
}

void MinCalculator::Add(const FieldValue* f) {
    if (f != nullptr) {
        if (min_value_ == nullptr) {
            min_value_ = CopyValue(*f);
        } else if (fcompare(*f, *min_value_, CompareOp::kLess)) {
            // TODO: swap
            delete min_value_;
            min_value_ = CopyValue(*f);
        }
    }
}

std::unique_ptr<FieldValue> MinCalculator::Result() {
    FieldValue* result = nullptr;
    std::swap(result, min_value_);
    return std::unique_ptr<FieldValue>(result);
}

//
// max
MaxCalculator::MaxCalculator(const u_int64_t id) : AggreCalculator(id) {}
MaxCalculator::~MaxCalculator() {
    if (max_value_) {
        delete max_value_; 
    }
}

void MaxCalculator::Add(const FieldValue* f) {
    if (f != nullptr) {
        if (max_value_ == nullptr) {
            max_value_ = CopyValue(*f);
        } else if (fcompare(*f, *max_value_, CompareOp::kGreater)) {
            // TODO: swap
            delete max_value_;
            max_value_ = CopyValue(*f);
        }
    }
}

std::unique_ptr<FieldValue> MaxCalculator::Result() {
    FieldValue* result = nullptr;
    std::swap(result, max_value_);
    return std::unique_ptr<FieldValue>(result);
}

// sum

SumCalculator::SumCalculator(const u_int64_t id) : AggreCalculator(id) {}
SumCalculator::~SumCalculator() {}

void SumCalculator::Add(const FieldValue* f) {
    if (f == nullptr) return;
    
    if (count_ == 0) {
        type_ = f->Type();
        switch (type_) {
            case FieldType::kDouble:
                sum_.fval = f->Double();
                break;
            case FieldType::kInt:
                sum_.ival = f->Int();
                break;
            case FieldType::kUInt:
                sum_.uval = f->UInt();
                break;
            default:
                return;
        }
    } else {
        if (type_ != f->Type()) return;
        switch (type_) {
            case FieldType::kDouble:
                sum_.fval += f->Double();
                break;
            case FieldType::kInt:
                sum_.ival += f->Int();
                break;
            case FieldType::kUInt:
                sum_.uval += f->UInt();
                break;
            default:
                return;
        }
    }
    ++count_;
}

std::unique_ptr<FieldValue> SumCalculator::Result() {
    switch (type_) {
        case FieldType::kDouble:
            return std::unique_ptr<FieldValue>(new FieldValue(sum_.fval));
        case FieldType::kInt:
            return std::unique_ptr<FieldValue>(new FieldValue(sum_.ival));
        case FieldType::kUInt:
            return std::unique_ptr<FieldValue>(new FieldValue(sum_.uval));
        default:
            return nullptr;
    }
}

std::unique_ptr<FieldValue> AvgCalculator::Result() {
    if (count_ == 0) return nullptr;

    switch (type_) {
        case FieldType::kDouble:
            return std::unique_ptr<FieldValue>(new FieldValue(sum_.fval));
        case FieldType::kInt:
            return std::unique_ptr<FieldValue>(new FieldValue(sum_.ival));
        case FieldType::kUInt:
            return std::unique_ptr<FieldValue>(new FieldValue(sum_.uval));
        default:
            return nullptr;
    }
}

} /* namespace storage */
} /* namespace ds */
} /* namespace chubaodb */
