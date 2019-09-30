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

#include "manager_impl.h"

#include <sstream>

//#include <rocksdb/utilities/blob_db/blob_db.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#include <rocksdb/rate_limiter.h>
//#include <rocksdb/utilities/db_ttl.h>

#include "base/util.h"
#include "base/fs_util.h"
#include "common/logger.h"

#include "db_impl.h"

namespace chubaodb {
namespace ds {
namespace db {

static const std::string kTxnCFName = "txn";

RocksDBManager::RocksDBManager(const RocksDBConfig& config) :
    db_path_(config.path) {
    buildDBOptions(config);
}

RocksDBManager::~RocksDBManager() {
    for (auto handle: cf_handles_) {
        delete handle;
    }
    delete db_;
}

void RocksDBManager::buildDBOptions(const RocksDBConfig& config) {
    // db log level
    if (config.enable_debug_log) {
        ops_.info_log_level = rocksdb::DEBUG_LEVEL;
    }

    // db stats
    if (config.enable_stats) {
        db_stats_ = rocksdb::CreateDBStatistics();;
        ops_.statistics = db_stats_;
    }
    disable_wal_ = config.disable_wal;

    // table options include block_size, block_cache_size, etc
    rocksdb::BlockBasedTableOptions table_options;
    table_options.block_size = config.block_size;
    if (config.block_cache_size > 0) {
        block_cache_ = rocksdb::NewLRUCache(config.block_cache_size);
        table_options.block_cache = block_cache_;
    }
    if (config.cache_index_and_filter_blocks){
        table_options.cache_index_and_filter_blocks = true;
    }
    ops_.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

    // row_cache
    if (config.row_cache_size > 0){
        row_cache_ = rocksdb::NewLRUCache(config.row_cache_size);;
        ops_.row_cache = row_cache_;
    }

    ops_.max_open_files = config.max_open_files;
    ops_.create_if_missing = true;
    ops_.use_fsync = true;
    ops_.use_adaptive_mutex = true;
    ops_.bytes_per_sync = config.bytes_per_sync;

    // memtables
    ops_.write_buffer_size = config.write_buffer_size;
    ops_.max_write_buffer_number = config.max_write_buffer_number;
    ops_.min_write_buffer_number_to_merge = config.min_write_buffer_number_to_merge;

    // level & sst file size
    ops_.max_bytes_for_level_base = config.max_bytes_for_level_base;
    ops_.max_bytes_for_level_multiplier = config.max_bytes_for_level_multiplier;
    ops_.target_file_size_base = config.target_file_size_base;
    ops_.target_file_size_multiplier = config.target_file_size_multiplier;

    // compactions and flushes
    if (config.disable_auto_compactions) {
        ops_.disable_auto_compactions = true;
    }
    if (config.background_rate_limit > 0) {
        ops_.rate_limiter = std::shared_ptr<rocksdb::RateLimiter>(
                rocksdb::NewGenericRateLimiter(static_cast<int64_t>(config.background_rate_limit)));
    }
    ops_.max_background_flushes = config.max_background_flushes;
    ops_.max_background_compactions = config.max_background_compactions;
    ops_.level0_file_num_compaction_trigger = config.level0_file_num_compaction_trigger;

    // write pause
    ops_.level0_slowdown_writes_trigger = config.level0_slowdown_writes_trigger;
    ops_.level0_stop_writes_trigger = config.level0_stop_writes_trigger;

    // compress
    auto compress_type = static_cast<rocksdb::CompressionType>(config.compression);
    switch (compress_type) {
        case rocksdb::kSnappyCompression:   // 1
        case rocksdb::kZlibCompression:     // 2
        case rocksdb::kBZip2Compression:    // 3
        case rocksdb::kLZ4Compression:      // 4
        case rocksdb::kLZ4HCCompression:    // 5
        case rocksdb::kXpressCompression:   // 6
            ops_.compression = compress_type;
            break;
        default:
            ops_.compression = rocksdb::kNoCompression;
    }
}

Status RocksDBManager::Open() {
    ops_.create_if_missing = true;
    ops_.create_missing_column_families = true;
    std::vector<rocksdb::ColumnFamilyDescriptor> column_families;

    // default column family
    column_families.emplace_back(rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions());
    // txn column family
    column_families.emplace_back(kTxnCFName, rocksdb::ColumnFamilyOptions());

    rocksdb::Status ret;
    ret = rocksdb::DB::Open(ops_, db_path_, column_families, &cf_handles_, &db_);
    // check open ret
    if (!ret.ok()) {
        return Status(Status::kIOError, "open db", ret.ToString());
    }

    return Status::OK();
}

Status RocksDBManager::Init() {
    //FLOG_INFO("rocksdb configs: {}", ops_.ToString());

    if (!MakeDirAll(db_path_, 0755)) {
        return Status(Status::kIOError, "create rocksdb path", strErrno(errno));
    }

    return Open();
}

Status RocksDBManager::createDB(uint64_t range_id, const std::string& start_key,
                const std::string& end_key, bool from_split,
                std::unique_ptr<DB>& db) {
    RocksDBOptions db_opt;
    db_opt.id = range_id;
    db_opt.start_key = start_key;
    db_opt.end_key = end_key;
    db_opt.wal_disabled = disable_wal_;
    db_opt.replay = !from_split;
    db_opt.db = db_;

    db.reset(new RocksDBImpl(db_opt, this, cf_handles_[0], cf_handles_[1]));

    return db->Open();
}

Status RocksDBManager::CreateDB(uint64_t range_id, const std::string& start_key,
              const std::string& end_key, std::unique_ptr<DB>& db) {
    return createDB(range_id, start_key, end_key, false, db);
}

Status RocksDBManager::CreatSplit(uint64_t range_id, const std::string& start_key,
                  const std::string& end_key, std::unique_ptr<DB>& db) {
    return createDB(range_id, start_key, end_key, true, db);
}

Status RocksDBManager::GetUsage(const SystemInfo& sys_info, DBUsage& usage) {
    uint64_t total = 0, available = 0;
    auto ret = sys_info.GetFileSystemUsage(db_path_.c_str(), &total, &available);
    if (!ret) {
        return Status(Status::kIOError, "collect disk usage", strErrno(errno));
    }

    usage.total_size = total;
    usage.free_size = available;
    usage.used_size = total - available;
    if (total > 0 && available <= total) {
        return Status::OK();
    } else {
        return Status(Status::kInvalid, "db usage", usage.ToString());
    }
}

std::string RocksDBManager::MetricInfo(bool verbose) {
    std::ostringstream ss;

    std::string tr_mem_usage;
    db_->GetProperty("rocksdb.estimate-table-readers-mem", &tr_mem_usage);
    std::string mem_table_usage;
    db_->GetProperty("rocksdb.cur-size-all-mem-tables", &mem_table_usage);

    ss << "rocksdb memory usages: table-readers=" << tr_mem_usage;
    ss << ", memtables=" << mem_table_usage;
    ss << ", block-cache=" << (block_cache_ ? block_cache_->GetUsage() : 0);
    ss << ", pinned=" << (block_cache_ ? block_cache_->GetPinnedUsage() : 0);
    ss << ", row-cache=" << (row_cache_ ? row_cache_->GetUsage() : 0);
    ss << std::endl;

    auto stat = db_stats_;
    if (stat) {
        ss << "rocksdb row-cache stats: hit="  << stat->getAndResetTickerCount(rocksdb::ROW_CACHE_HIT);
        ss << ", miss="  << stat->getAndResetTickerCount(rocksdb::ROW_CACHE_MISS);
        ss << std::endl;

        ss << "rocksdb block-cache stats: hit="  << stat->getAndResetTickerCount(rocksdb::BLOCK_CACHE_HIT);
        ss << ", miss="  << stat->getAndResetTickerCount(rocksdb::BLOCK_CACHE_MISS);
        ss << std::endl;

        ss << "rockdb get histograms: " << stat->getHistogramString(rocksdb::DB_GET) << std::endl;
        ss << "rockdb write histograms: " <<  stat->getHistogramString(rocksdb::DB_WRITE) << std::endl;

        stat->Reset();
    }

    return ss.str();
}

} // namespace db
} // namespace ds
} // namespace chubaodb
