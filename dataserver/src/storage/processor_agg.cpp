// Copyright 2019 The ChubaoDB Authors.
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

#include "processor_agg.h"

namespace chubaodb {
namespace ds {
namespace storage {

Aggregation::Aggregation(const dspb::Aggregation &agg, std::unique_ptr<Processor> processor)
    : processor_(std::move(processor)) {
    for (auto &f : agg.func()) {
        auto cal = AggreCalculator::New(f);
        if (cal) {
            aggre_cals_.push_back(std::move(cal));
        }
    }
}

Aggregation::~Aggregation() {
}

Status Aggregation::next(RowResult &row) {
    Status s = processor_->next(row);
    
    if (!s.ok()) {
        return s;
    }
    while(s.ok()) {
        for (auto &cal : aggre_cals_) {
            cal->Add(row.GetField(cal->GetColumnId()));
        }

        s = processor_->next(row);
    }

    std::string buf;
    for (auto& cal : aggre_cals_) {
        auto f = cal->Result();
        EncodeFieldValue(&buf, f.get());
        if (cal->isAvg()) {
            EncodeIntValue(&buf,kNoColumnID, cal->Count());
        }
    }
    row.SetAggFields(buf);
    return Status::OK();
}

} /* namespace storage */
} /* namespace ds */
} /* namespace chubaodb */
