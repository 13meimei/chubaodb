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

#include <memory>

#include "dspb/expr.pb.h"

namespace chubaodb {
namespace test {
namespace helper {

class Table {
public:
    Table(const std::string& table_name, uint32_t table_id);
    explicit Table(const basepb::Table& meta);
    ~Table() = default;

    uint64_t GetID() const { return meta_.id(); }
    std::string GetName() const { return meta_.name(); }

    std::vector<basepb::Column> GetPKs() const;
    std::vector<basepb::Column> GetAllColumns() const;
    std::vector<basepb::Column> GetNonPkColumns() const;
    basepb::Column GetColumn(uint64_t id) const;
    basepb::Column GetColumn(const std::string& name) const;

    dspb::ColumnInfo GetColumnInfo(uint64_t id) const;
    dspb::ColumnInfo GetColumnInfo(const std::string& name) const;

    const basepb::Table& GetMeta() const { return meta_; }

    void AddColumn(const std::string& name, basepb::DataType type, bool is_pk = false);

private:
    basepb::Table meta_;
    uint64_t last_col_id_ = 0;
};

std::unique_ptr<Table> CreateAccountTable();
std::unique_ptr<Table> CreateUserTable();
std::unique_ptr<Table> CreateHashUserTable();

std::unique_ptr<Table> CreateAccountTable(uint32_t tid);
std::unique_ptr<Table> CreateUserTable(uint32_t tid);
std::unique_ptr<Table> CreateHashUserTable(uint32_t tid);

} /* namespace helper */
} /* namespace test */
} /* namespace chubaodb */
