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

#include "SQLParser.h"

#include "dspb/txn.pb.h"
#include "table.h"

namespace chubaodb {
namespace test {
namespace helper {

void parseExpr(const Table& t, hsql::Expr* from, dspb::Expr* to);
void parseLimit(hsql::LimitDescription* from, dspb::Limit* to);
void parseFieldList(const Table& t, const hsql::SelectStatement* sel, dspb::SelectRequest* req);

} /* namespace helper */
} /* namespace test */
} /* namespace chubaodb */
