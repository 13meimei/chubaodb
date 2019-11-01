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

#include "processor_data_sample.h"

namespace chubaodb {
namespace ds {
namespace storage {

DataSample::DataSample(const dspb::DataSample & data_sample, const dspb::KeyRange & range_default, Store & s )
    : str_last_key_(""),
    over_(false),
    range_default_(range_default),
    primary_keys_( s.GetPrimaryKeys()),
    row_fetcher_(new RowFetcher( s, data_sample, range_default.start_key(), range_default.end_key()))
{
    for (const auto & col : data_sample.columns()) {
        col_ids.push_back(col.id());
    }
}

DataSample::~DataSample()
{

}

const std::string DataSample::get_last_key()
{
    return str_last_key_;
}

Status DataSample::next( RowResult & row)
{
    Status s;

    if (over_) {
        return Status(
                Status::kNoMoreData,
                " last key: ",
                EncodeToHexString(get_last_key())
            );
    }

    s = row_fetcher_->Next( row, over_);

    if (s.ok()) {
        str_last_key_ =  row.GetKey();
    }

    if (over_) {

        s = Status( Status::kNoMoreData, " last key: ", EncodeToHexString(get_last_key()) );
    }

    return s;

}

const std::vector<uint64_t> DataSample::get_col_ids()
{
    return col_ids;
}



} /* namespace storage */
} /* namespace ds */
} /* namespace chubaodb */

