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
                                                      const basepb::Column* col) {
    if (name == "count") {
        return std::unique_ptr<AggreCalculator>(new CountCalculator(col));
    } else if (name == "min") {
        return std::unique_ptr<AggreCalculator>(new MinCalculator(col));
    } else if (name == "max") {
        return std::unique_ptr<AggreCalculator>(new MaxCalculator(col));
    } else if (name == "sum") {
        return std::unique_ptr<AggreCalculator>(new SumCalculator(col));
    } else if (name == "avg") {
        return std::unique_ptr<AggreCalculator>(new SumCalculator(col));
    } else {
        return nullptr;
    }
}

// count
CountCalculator::CountCalculator(const basepb::Column* col) : AggreCalculator(col) {}
CountCalculator::~CountCalculator() {}

void CountCalculator::Add(const FieldValue* f) {
    if (col_ == nullptr || f != nullptr) {
        ++count_;
    }
}

int64_t CountCalculator::Count() const { return 0; }

std::unique_ptr<FieldValue> CountCalculator::Result() {
    return std::unique_ptr<FieldValue>(new FieldValue(count_));
}

//
// min
MinCalculator::MinCalculator(const basepb::Column* col) : AggreCalculator(col) {}
MinCalculator::~MinCalculator() { delete min_value_; }

void MinCalculator::Add(const FieldValue* f) {
    if (f != nullptr) {
        if (min_value_ == nullptr || fcompare(*f, *min_value_, CompareOp::kLess)) {
            // TODO: swap
            delete min_value_;
            min_value_ = CopyValue(*f);
        }
    }
}

int64_t MinCalculator::Count() const { return 0; }

std::unique_ptr<FieldValue> MinCalculator::Result() {
    FieldValue* result = nullptr;
    std::swap(result, min_value_);
    return std::unique_ptr<FieldValue>(result);
}

//
// max
MaxCalculator::MaxCalculator(const basepb::Column* col) : AggreCalculator(col) {}
MaxCalculator::~MaxCalculator() { delete max_value_; }

void MaxCalculator::Add(const FieldValue* f) {
    if (f != nullptr) {
        if (max_value_ == nullptr || fcompare(*f, *max_value_, CompareOp::kGreater)) {
            // TODO: swap
            delete max_value_;
            max_value_ = CopyValue(*f);
        }
    }
}

int64_t MaxCalculator::Count() const { return 0; }

std::unique_ptr<FieldValue> MaxCalculator::Result() {
    FieldValue* result = nullptr;
    std::swap(result, max_value_);
    return std::unique_ptr<FieldValue>(result);
}

// sum

SumCalculator::SumCalculator(const basepb::Column* col) : AggreCalculator(col) {}
SumCalculator::~SumCalculator() {}

void SumCalculator::Add(const FieldValue* f) {
    if (f == nullptr) return;
    if (count_ == 0) {
        type_ = f->Type();
        switch (type_) {
            case FieldType::kFloat:
                sum_.fval = f->Float();
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
            case FieldType::kFloat:
                sum_.fval += f->Float();
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

int64_t SumCalculator::Count() const { return count_; }

std::unique_ptr<FieldValue> SumCalculator::Result() {
    switch (type_) {
        case FieldType::kFloat:
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