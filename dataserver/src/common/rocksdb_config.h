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

#include "common/config_util.h"

namespace chubaodb {

struct RocksDBConfig {
    std::string path;
    int storage_type = 0;

    size_t block_cache_size; // default: 1024MB
    size_t row_cache_size;
    size_t block_size; // default: 16K
    int max_open_files;
    size_t bytes_per_sync;
    size_t write_buffer_size;
    int max_write_buffer_number;
    int min_write_buffer_number_to_merge;
    size_t max_bytes_for_level_base;
    int max_bytes_for_level_multiplier;
    size_t target_file_size_base;
    int target_file_size_multiplier;
    int max_background_flushes;
    int max_background_compactions;
    size_t background_rate_limit;
    bool disable_auto_compactions = false;
    bool read_checksum = true;
    int level0_file_num_compaction_trigger;
    int level0_slowdown_writes_trigger;
    int level0_stop_writes_trigger;
    bool disable_wal = false;
    bool cache_index_and_filter_blocks;
    int compression = 0;

    int min_blob_size;
    size_t blob_file_size;
    bool enable_garbage_collection;
    int blob_gc_percent;
    int blob_compression = 0;
    size_t blob_cache_size;
    uint64_t blob_ttl_range; // in seconds

    int ttl = 0;
    bool enable_stats = true;
    bool enable_debug_log = false;

    bool Load(const INIReader& reader, const std::string& base_data_path);
    void Print() const;
};

} // namespace chubaodb
