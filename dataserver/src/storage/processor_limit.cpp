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

#include "processor_limit.h"

namespace chubaodb {
namespace ds {
namespace storage {

Limit::Limit(const dspb::Limit &limit, std::unique_ptr<Processor> processor)
    : limit_(limit.count()),
      offset_(limit.offset()),
      end_(offset_ + limit_),
      processor_(std::move(processor)) {
}

Limit::~Limit() {
}

Status Limit::next(RowResult &row) {
    if (index_ >= end_ || limit_ == 0) {
        return Status(
                Status::kNoMoreData, "limit is reached",
                std::to_string(offset_) + "," + std::to_string(index_)  + "," + std::to_string(end_)
                );

    }

    Status s;
    while (true) {
        row.Reset();
        s = processor_->next(row);
        if (!s.ok()) {
            row.Reset();
            break;
        }

        if ( ++index_ > offset_ ) {
            break;
        }
    }

    return s;
}

} /* namespace storage */
} /* namespace ds */
} /* namespace chubaodb */
