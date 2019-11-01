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

#include "processor_selection.h"
#include "util.h"

namespace chubaodb {
namespace ds {
namespace storage {

Selection::Selection(const dspb::Selection &selection, std::unique_ptr<Processor> processor)
        :processor_(std::move(processor))
{
    for (auto f : selection.filter())
    {
        exprs_.push_back( new dspb::Expr(f));
    }
}

Selection::~Selection()
{

    std::for_each(exprs_.begin(),
            exprs_.end(),
            []( dspb::Expr * ptr ) {
        delete ptr;
    });

    exprs_.clear();
}

Status Selection::next(RowResult &row)
{

    Status s;
    bool matched = true;
    while (true) {
        row.Reset();
        s = processor_->next(row);
        if (!s.ok()) {
            row.Reset();
            break;
        }

        for ( auto e : exprs_ ) {

            s = filterExpr(row, *e, matched);
            if (!s.ok()) {
                row.Reset();
                return s;
            }

            if (!matched) {
                break;
            }
        }

        if (matched) {
            break;
        }
    }

    return s;
}

const std::string Selection::get_last_key()
{
    return processor_->get_last_key();
}

const std::vector<uint64_t> Selection::get_col_ids()
{
    return processor_->get_col_ids();
}

} /* namespace storage */
} /* namespace ds */
} /* namespace chubaodb */
