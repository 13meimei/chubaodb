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

_Pragma("once");

#include "base/status.h"
#include "common/ds_encoding.h"
#include "processor.h"
#include "proto/gen/dspb/processorpb.pb.h"
#include "proto/gen/dspb/txn.pb.h"
#include "row_fetcher.h"

#include <map>
#include <string>
#include <vector>

namespace chubaodb {
namespace ds {
namespace storage {

class Selection : public Processor {

public:
    Selection(const dspb::Selection & selection, std::unique_ptr<Processor> processor);
    ~Selection();

    Selection() = delete;
    Selection(const Selection &) = delete;
    Selection &operator=(const Selection &) = delete;

    virtual Status next(RowResult &row);

    virtual const std::string get_last_key();

    virtual const std::vector<uint64_t> get_col_ids();

private:
    std::vector< dspb::Expr * > exprs_;

    std::unique_ptr<Processor> processor_;
};

} /* namespace storage */
} /* namespace ds */
} /* namespace chubaodb */
