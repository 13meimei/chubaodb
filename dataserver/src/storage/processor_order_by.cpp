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

#include "processor_order_by.h"

namespace chubaodb {
namespace ds {
namespace storage {

OrderBy::OrderBy(const dspb::Ordering &ordering, std::unique_ptr<Processor> processor)
    : ordering_(ordering),
    count_(std::min(ordering_.count(), max_count_)),
    processor_(std::move(processor)) {
    status_check_ordering_ = OrderingCheck();
    if (status_check_ordering_.ok()) {
        ColumnOrderByInfo colInfo;
        for (const auto &column : ordering_.columns()) {
            colInfo.col_id = column.expr().column().id();
            colInfo.asc = column.asc();
            col_order_by_infos_.push_back(colInfo);
        }
        FetchOrderByRows();
    }
}

OrderBy::~OrderBy() {
}

Status OrderBy::OrderingCheck() {
    if (count_ == 0) {
        return Status(
                Status::kInvalid,
                " orderby count == 0 ",
                ""
            );
    }

    if (ordering_.columns_size() <= 0) {
        return Status(
                Status::kInvalid,
                " orderby columns is empty ",
                ""
            );
    }
    return Status::OK();
}

void OrderBy::FetchOrderByRows() {
    Status s;
    do {
        RowResult row;
        s = processor_->next(row);
        if (!s.ok()) {
            break;
        }
        row.SetColumnOrderByInfos(col_order_by_infos_);
        set_result_.insert(row);
        if (set_result_.size() == count_) {
            break;
        }
    } while (s.ok());
    
    while(s.ok()) {
        RowResult row;
        s = processor_->next(row);
        if (!s.ok()) {
            break;
        }
        auto it = set_result_.end();
        --it;
        row.SetColumnOrderByInfos(col_order_by_infos_);
        if (row < (*it)) {
            set_result_.erase(it);
            set_result_.insert(row);
        }
    }
    set_result_it_ = set_result_.begin();
}

Status OrderBy::next(RowResult &row) {
    if (!status_check_ordering_.ok()) {
        return status_check_ordering_;
    }

    if (set_result_it_ == set_result_.end()) {
        return Status(
                Status::kNoMoreData,
                " last key: ",
                EncodeToHexString(get_last_key())
            );
    }

    row = *set_result_it_++;
    
    return Status::OK();
}

} /* namespace storage */
} /* namespace ds */
} /* namespace chubaodb */
