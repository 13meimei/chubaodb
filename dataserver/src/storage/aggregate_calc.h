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

#include "field_value.h"
#include "basepb/basepb.pb.h"

namespace chubaodb {
namespace ds {
namespace storage {

class AggreCalculator {
public:
    AggreCalculator(const basepb::Column* col) : col_(col) {}
    virtual ~AggreCalculator() {}

    virtual void Add(const FieldValue* f) = 0;

    virtual int64_t Count() const = 0;

    virtual std::unique_ptr<FieldValue> Result() = 0;

    static std::unique_ptr<AggreCalculator> New(const std::string& name,
                                                const basepb::Column* col);

protected:
    const basepb::Column* const col_;
};

class CountCalculator : public AggreCalculator {
public:
    CountCalculator(const basepb::Column* col);
    ~CountCalculator();

    void Add(const FieldValue* f) override;
    int64_t Count() const override;
    std::unique_ptr<FieldValue> Result() override;

private:
    int64_t count_ = 0;
};

class MinCalculator : public AggreCalculator {
public:
    MinCalculator(const basepb::Column* col);
    ~MinCalculator();

    void Add(const FieldValue* f) override;
    int64_t Count() const override;
    std::unique_ptr<FieldValue> Result() override;

private:
    FieldValue* min_value_ = nullptr;
};

class MaxCalculator : public AggreCalculator {
public:
    MaxCalculator(const basepb::Column* col);
    ~MaxCalculator();

    void Add(const FieldValue* f) override;
    int64_t Count() const override;
    std::unique_ptr<FieldValue> Result() override;

private:
    FieldValue* max_value_ = nullptr;
};

class SumCalculator : public AggreCalculator {
public:
    SumCalculator(const basepb::Column* col);
    ~SumCalculator();

    void Add(const FieldValue* f) override;
    int64_t Count() const override;
    std::unique_ptr<FieldValue> Result() override;

private:
    int64_t count_ = 0;
    union {
        int64_t ival;
        uint64_t uval;
        double fval;
    } sum_;
    FieldType type_ = FieldType::kBytes;
};

} /* namespace storage */
} /* namespace ds */
} /* namespace chubaodb */
