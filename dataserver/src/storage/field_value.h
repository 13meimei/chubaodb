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

#include <stdint.h>
#include <string>
#include <vector>

#include "dspb/expr.pb.h"

namespace chubaodb {
namespace ds {
namespace storage {

enum class FieldType : char {
    kInt,
    kUInt,
    kFloat,
    kBytes,
};

class FieldValue {
public:
    explicit FieldValue(int64_t val) : type_(FieldType::kInt) {
        value_.ival = val;
    }
    explicit FieldValue(uint64_t val) : type_(FieldType::kUInt) {
        value_.uval = val;
    }
    explicit FieldValue(double val) : type_(FieldType::kFloat) {
        value_.fval = val;
    }
    explicit FieldValue(std::string* val) : type_(FieldType::kBytes) {
        value_.sval = val;
    }
    explicit FieldValue(const std::string& val) : type_(FieldType::kBytes) {
        value_.sval = new std::string(val);
    }
    explicit FieldValue(std::string&& val) : type_(FieldType::kBytes) {
        value_.sval = new std::string(std::move(val));
    }

    ~FieldValue() {
        if (FieldType::kBytes == type_) delete value_.sval;
    }

    FieldValue(const FieldValue&) = delete;
    FieldValue& operator=(const FieldValue&) = delete;

    FieldType Type() const { return type_; }

    int64_t Int() const {
        if (type_ != FieldType::kInt) return 0;
        return value_.ival;
    }

    uint64_t UInt() const {
        if (type_ != FieldType::kUInt) return 0;
        return value_.uval;
    }

    double Float() const {
        if (type_ != FieldType::kFloat) return 0;
        return value_.fval;
    }

    const std::string& Bytes() const {
        if (type_ != FieldType::kBytes || value_.sval == nullptr)
            return kDefaultBytes;
        return *value_.sval;
    }

    std::string ToString() const {
        if (type_ == FieldType::kInt) {
            return std::to_string(value_.ival);
        } else if (type_ == FieldType::kUInt) {
            return std::to_string(value_.uval);
        } else if (type_ == FieldType::kFloat) {
            return std::to_string(value_.fval);
        } else if (type_ == FieldType::kBytes) {
            return *value_.sval;
        } else {
            return "Unknown field value";
        }
    }

private:
    static const std::string kDefaultBytes;

    FieldType type_;
    union {
        int64_t ival;
        uint64_t uval;
        double fval;
        std::string* sval;
    } value_;
};

enum class CompareOp : char {
    kLess,
    kGreater,
    kEqual,
};

bool fcompare(const FieldValue& lh, const FieldValue& rh, CompareOp op);

FieldValue* CopyValue(const FieldValue& f);
void EncodeFieldValue(std::string* buf, FieldValue* v);
void EncodeFieldValue(std::string* buf, FieldValue* v, uint32_t col_id);

std::unique_ptr<FieldValue> arithCalc(const FieldValue* lh, const FieldValue* rh, dspb::ExprType type);

} /* namespace storage */
} /* namespace ds */
} /* namespace chubaodb */
