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

#include "rocksdb_config.h"

namespace chubaodb {

bool RocksDBConfig::Load(const INIReader& reader, const std::string& base_data_path) {
    const char *section = "rocksdb";

    path = reader.Get(section, "path", "");
    if (!validateDataPath(base_data_path, kRocksdbPathSuffix, path)) {
        return false;
    }

    storage_type = reader.GetInteger(section, "storage_type", 0);

    bool ret = false;
    std::tie(block_cache_size, ret) =
            loadNeBytesValue(reader, section, "block_cache_size", 1024 * 1024 * 1024);
    if (!ret) return false;

    std::tie(row_cache_size, ret) =
            loadNeBytesValue(reader, section, "row_cache_size", 0);
    if (!ret) return false;

    std::tie(block_size, ret)  =
            loadNeBytesValue(reader, section, "block_size", 16 * 1024);
    if (!ret) return false;

    std::tie(max_open_files, ret) =
            loadIntegerAtLeast(reader, section, "max_open_files", 1024, 100);
    if (!ret) return false;

    std::tie(bytes_per_sync, ret) =
            loadNeBytesValue(reader, section, "bytes_per_sync", 0);
    if (!ret) return false;

    std::tie(write_buffer_size, ret) =
            loadNeBytesValue(reader, section, "write_buffer_size", 128UL * 1024 * 1024);
    if (!ret) return false;

    std::tie(max_write_buffer_number, ret)  =
            loadIntegerAtLeast(reader, section, "max_write_buffer_number", 8, 2);
    if (!ret) return false;

    std::tie(min_write_buffer_number_to_merge, ret)  =
            loadIntegerAtLeast(reader, section, "min_write_buffer_number_to_merge", 1, 1);
    if (!ret) return false;

    std::tie(max_bytes_for_level_base, ret) =
            loadNeBytesValue(reader, section, "max_bytes_for_level_base", 512UL * 1024 * 1024);
    if (!ret) return false;

    std::tie(max_bytes_for_level_multiplier, ret) =
            loadIntegerAtLeast(reader, section, "max_bytes_for_level_multiplier", 10, 1);
    if (!ret) return false;

    std::tie(target_file_size_base, ret) =
            loadNeBytesValue(reader, section, "target_file_size_base", 128UL * 1024 * 1024);
    if (!ret) return false;
    std::tie(target_file_size_multiplier, ret) =
            loadIntegerAtLeast(reader, section, "target_file_size_multiplier", 1, 1);
    if (!ret) return false;

    std::tie(max_background_flushes, ret) =
            loadIntegerAtLeast(reader, section, "max_background_flushes", 2, 1);
    if (!ret) return false;

    std::tie(max_background_compactions, ret) =
            loadIntegerAtLeast(reader, section, "max_background_compactions", 4, 1);
    if (!ret) return false;

    std::tie(background_rate_limit, ret) =
            loadNeBytesValue(reader, section, "background_rate_limit", 0);
    if (!ret) return false;

    disable_auto_compactions = reader.GetBoolean(section, "disable_auto_compactions", false);

    read_checksum = reader.GetBoolean(section, "read_checksum", true);

    std::tie(level0_file_num_compaction_trigger, ret) =
            loadIntegerAtLeast(reader, section, "level0_file_num_compaction_trigger", 8, 1);
    if (!ret) return false;

    std::tie(level0_slowdown_writes_trigger, ret) =
            loadIntegerAtLeast(reader, section, "level0_slowdown_writes_trigger", 40, 1);
    if (!ret) return false;

    std::tie(level0_stop_writes_trigger, ret) =
            loadIntegerAtLeast(reader, section, "level0_stop_writes_trigger", 46, 1);
    if (!ret) return false;

    disable_wal = reader.GetBoolean(section, "disable_wal", false);

    cache_index_and_filter_blocks =
            reader.GetBoolean(section, "cache_index_and_filter_blocks", false);

    compression = reader.GetInteger(section, "compression", 0);


    std::tie(min_blob_size, ret) = loadIntegerAtLeast(reader, section, "min_blob_size", 0, 0);
    if (!ret) return false;

    std::tie(blob_file_size, ret) = loadNeBytesValue(reader, section, "blob_file_size", 256 * 1024 * 1024UL);
    if (!ret) return false;

    enable_garbage_collection = reader.GetBoolean(section, "enable_garbage_collection", true);

    std::tie(blob_gc_percent, ret) = loadIntegerAtLeast(reader, section, "blob_gc_percent", 75, 10);
    if (!ret) return false;
    if (blob_gc_percent > 100) {
        FLOG_CRIT("[Config] invalid rocksdb.blob_gc_percent config: {}", blob_gc_percent);
        return false;
    }
    std::tie(blob_compression, ret) = loadIntegerAtLeast(reader, section, "blob_compression", 0, 0);
    if (!ret) return false;

    std::tie(blob_cache_size, ret) = loadNeBytesValue(reader, section, "blob_cache_size", 0);
    if (!ret) return false;

    std::tie(blob_ttl_range, ret) = loadIntegerAtLeast(reader, section, "blob_ttl_range", 3600, 60);
    if (!ret) return false;

    std::tie(ttl, ret) = loadIntegerAtLeast(reader, section, "ttl", 0, 0);
    if (!ret) return false;

    enable_stats = reader.GetBoolean(section, "enable_stats", true);
    enable_debug_log = reader.GetBoolean(section, "enable_debug_log", false);

    return true;
}

void RocksDBConfig::Print() const {
    FLOG_INFO("rockdb_configs: "
              "\n\tpath: {}"
              "\n\tblock_cache_size: {}"
              "\n\trow_cache_size: {}"
              "\n\tblock_size: {}"
              "\n\tmax_open_files: {}"
              "\n\tbytes_per_sync: {}"
              "\n\twrite_buffer_size: {}"
              "\n\tmax_write_buffer_number: {}"
              "\n\tmin_write_buffer_number_to_merge: {}"
              "\n\tmax_bytes_for_level_base: {}"
              "\n\tmax_bytes_for_level_multiplier: {}"
              "\n\ttarget_file_size_base: {}"
              "\n\ttarget_file_size_multiplier: {}"
              "\n\tmax_background_flushes: {}"
              "\n\tmax_background_compactions: {}"
              "\n\tbackground_rate_limit: {}"
              "\n\tdisable_auto_compactions: {}"
              "\n\tread_checksum: {}"
              "\n\tlevel0_file_num_compaction_trigger: {}"
              "\n\tlevel0_slowdown_writes_trigger: {}"
              "\n\tlevel0_stop_writes_trigger: {}"
              "\n\tdisable_wal: {}"
              "\n\tcache_index_and_filter_blocks: {}"
              "\n\tcompression: {}"
              "\n\tstorage_type: {}"
              "\n\tmin_blob_size: {}"
              "\n\tblob_file_size: {}"
              "\n\tenable_garbage_collection: {}"
              "\n\tblob_gc_percent: {}"
              "\n\tblob_compression: {}"
              "\n\tblob_cache_size: {}"
              "\n\tblob_ttl_range: {}"
              "\n\tttl: {}"
              "\n\tenable_stats: {}"
              "\n\tenable_debug_log: {}"
              ,
              path,
              block_cache_size,
              row_cache_size,
              block_size,
              max_open_files,
              bytes_per_sync,
              write_buffer_size,
              max_write_buffer_number,
              min_write_buffer_number_to_merge,
              max_bytes_for_level_base,
              max_bytes_for_level_multiplier,
              target_file_size_base,
              target_file_size_multiplier,
              max_background_flushes,
              max_background_compactions,
              background_rate_limit,
              disable_auto_compactions,
              read_checksum,
              level0_file_num_compaction_trigger,
              level0_slowdown_writes_trigger,
              level0_stop_writes_trigger,
              disable_wal,
              cache_index_and_filter_blocks,
              compression,
              storage_type,
              min_blob_size,
              blob_file_size,
              enable_garbage_collection,
              blob_gc_percent,
              blob_compression,
              blob_cache_size,
              blob_ttl_range,
              ttl,
              enable_stats,
              enable_debug_log
    );

    if (ttl > 0) {
        FLOG_WARN("rocksdb ttl enabled. ttl={}", ttl);
    }
    if (disable_auto_compactions) {
        FLOG_WARN("rocksdb auto compactions is disabled.");
    }
}

} // namespace chubaodb
