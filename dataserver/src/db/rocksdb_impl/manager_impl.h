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

#include <vector>
#include <rocksdb/db.h>
//#include <rocksdb/utilities/blob_db/blob_db.h>

#include "base/system_info.h"
#include "db/db_manager.h"
#include "common/rocksdb_config.h"

namespace chubaodb {
namespace ds {
namespace db {

class RocksDBManager: public DBManager {
public:
    explicit RocksDBManager(const RocksDBConfig& config);
    ~RocksDBManager();

    RocksDBManager(const RocksDBManager&) = delete;
    RocksDBManager& operator=(const RocksDBManager&) = delete;

    Status Init() override;

    Status CreateDB(uint64_t range_id, const std::string& start_key,
            const std::string& end_key, std::unique_ptr<DB>& db) override;

    Status GetUsage(const SystemInfo& sys_info, DBUsage& usage) override;

    std::string MetricInfo(bool verbose) override;

    Status CreatSplit(uint64_t range_id, const std::string& start_key,
            const std::string& end_key, std::unique_ptr<DB>& db);

private:
    Status createDB(uint64_t range_id, const std::string& start_key,
                    const std::string& end_key, bool from_split,
                    std::unique_ptr<DB>& db);

    Status Open();
    void buildDBOptions(const RocksDBConfig& config);
private:
    bool disable_wal_;
    const std::string db_path_;
    rocksdb::Options ops_;

    std::vector<rocksdb::ColumnFamilyHandle*> cf_handles_;
    rocksdb::DB* db_ = nullptr;

    std::shared_ptr<rocksdb::Cache> block_cache_;   // rocksdb block cache
    std::shared_ptr<rocksdb::Cache> row_cache_;     // rocksdb row cache
    std::shared_ptr<rocksdb::Statistics> db_stats_; // rocksdb stats
};

} // namespace db
} // namespace ds
} // namespace chubaodb
