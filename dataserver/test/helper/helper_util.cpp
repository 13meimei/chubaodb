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

#include "helper_util.h"

#include "common/ds_encoding.h"
#include "db/mass_tree_impl/manager_impl.h"
#include "db/rocksdb_impl/manager_impl.h"

namespace chubaodb {
namespace test {
namespace helper {

using namespace chubaodb::ds;
using namespace chubaodb::ds::db;

uint64_t GetPeerID(uint64_t node_id) {
    return node_id + 100;
}

basepb::Range MakeRangeMeta(Table *t, size_t peers_num) {
    const uint32_t RID = 1;
    return MakeRangeMeta(t, peers_num, RID);
}

basepb::Range MakeRangeMeta(Table *t, size_t peers_num, uint32_t rid) {
    basepb::Range meta;
    meta.set_id(rid);
    EncodeKeyPrefix(meta.mutable_start_key(), t->GetID());
    EncodeKeyPrefix(meta.mutable_end_key(), t->GetID() + 1);

    for (size_t i = 0; i < peers_num; ++i) {
        auto peer = meta.add_peers();
        peer->set_node_id(i + 1);
        peer->set_id(GetPeerID(peer->node_id()));
        peer->set_type(basepb::PeerType_Normal);
    }
    meta.mutable_range_epoch()->set_version(peers_num);
    meta.mutable_range_epoch()->set_conf_ver(peers_num);

    meta.set_table_id(t->GetID());
    auto pks = t->GetPKs();
    if (pks.size() == 0) {
        throw std::runtime_error("invalid table(no primary key)");
    }
    for (const auto& pk : pks) {
        auto p = meta.add_primary_keys();
        p->CopyFrom(pk);
    }

    return meta;
}

static const char kKeyPrefixByte = '\x01';
static const char kIndexPrefixByte = '\x02';

void EncodeKeyPrefix(std::string *buf, uint64_t table_id) {
    buf->push_back(kKeyPrefixByte);
    EncodeUint64Ascending(buf, table_id);
}

void EncodeIndexKeyPrefix(std::string *buf, uint64_t table_id) {
    buf->push_back(kIndexPrefixByte);
    EncodeUint64Ascending(buf, table_id);
}

// append encoded pk values to buf
void EncodePrimaryKey(std::string *buf, const basepb::Column& col, const std::string& val) {
    switch (col.data_type()) {
        case basepb::Tinyint:
        case basepb::Smallint:
        case basepb::Int:
        case basepb::BigInt: {
            if (!col.unsigned_()) {
                int64_t i = strtoll(val.c_str(), NULL, 10);
                EncodeVarintAscending(buf, i);
            } else {
                uint64_t i = strtoull(val.c_str(), NULL, 10);
                EncodeUvarintAscending(buf, i);
            }
            break;
        }

        case basepb::Float:
        case basepb::Double: {
            double d = strtod(val.c_str(), NULL);
            EncodeFloatAscending(buf, d);
            break;
        }

        case basepb::Varchar:
        case basepb::Binary:
        case basepb::Date:
        case basepb::TimeStamp: {
            EncodeBytesAscending(buf, val.c_str(), val.size());
            break;
        }

        default:
            throw std::runtime_error(std::string("EncodePrimaryKey: invalid column data type: ") +
                std::to_string(static_cast<int>(col.data_type())));
    }
}

void EncodeColumnValue(std::string *buf, const basepb::Column& col, const std::string& val) {
    switch (col.data_type()) {
    case basepb::Tinyint:
    case basepb::Smallint:
    case basepb::Int:
    case basepb::BigInt: {
        if (!col.unsigned_()) {
            int64_t i = strtoll(val.c_str(), NULL, 10);
            EncodeIntValue(buf, static_cast<uint32_t>(col.id()), i);
        } else {
            uint64_t i = strtoull(val.c_str(), NULL, 10);
            EncodeIntValue(buf, static_cast<uint32_t>(col.id()), static_cast<int64_t>(i));
        }
        break;
    }

    case basepb::Float:
    case basepb::Double: {
        double d = strtod(val.c_str(), NULL);
        EncodeFloatValue(buf, static_cast<uint32_t>(col.id()), d);
        break;
    }

    case basepb::Varchar:
    case basepb::Binary:
    case basepb::Date:
    case basepb::TimeStamp: {
        EncodeBytesValue(buf, static_cast<uint32_t>(col.id()), val.c_str(), val.size());
        break;
    }

    default:
        throw std::runtime_error(std::string("EncodeColumnValue: invalid column data type: ") +
                                 std::to_string(static_cast<int>(col.data_type())));
    }
}

void DecodeColumnValue(const std::string& buf, size_t& offset, const dspb::ColumnInfo& col, std::string *val) {
    switch (col.typ()) {
    case basepb::Tinyint:
    case basepb::Smallint:
    case basepb::Int:
    case basepb::BigInt: {
        int64_t i = 0;
        DecodeIntValue(buf, offset, &i);
        if (col.unsigned_()) {
            *val = std::to_string(static_cast<uint64_t>(i));
        } else {
            *val = std::to_string(i);
        }
        break;
    }

    case basepb::Float:
    case basepb::Double: {
        double d = 0.0;
        DecodeFloatValue(buf, offset, &d);
        *val = std::to_string(d);
        break;
    }

    case basepb::Varchar:
    case basepb::Binary:
    case basepb::Date:
    case basepb::TimeStamp: {
        DecodeBytesValue(buf, offset, val);
        break;
    }

    default:
        throw std::runtime_error(std::string("EncodeColumnValue: invalid column data type: ") +
                                 std::to_string(static_cast<int>(col.typ())));
    }
}

void InitLog() {
    auto str = ::getenv("LOG_LEVEL");
    std::string log_level = "info";
    if (str != nullptr) {
        log_level = str;
    }
    LoggerSetLevel(log_level);
}

Status NewDBManager(const std::string& path, std::unique_ptr<ds::db::DBManager>& db_manager) {
    auto engine = ::getenv("ENGINE");

    if (engine != nullptr && strcmp(engine, "rocksdb") == 0) {
        RocksDBConfig opt;
        opt.path = path;
        opt.storage_type = 0;

        opt.block_cache_size = 1024 * 1024 * 1024; // default: 1024MB
        opt.row_cache_size = 0;
        opt.block_size = 16 * 1024; // default: 16K
        opt.max_open_files = 100;
        opt.bytes_per_sync = 0;
        opt.write_buffer_size = 128 * 1024 * 1024;
        opt.max_write_buffer_number = 8;
        opt.min_write_buffer_number_to_merge = 1;
        opt.max_bytes_for_level_base = 512 * 1024 * 1024;
        opt.max_bytes_for_level_multiplier = 10;
        opt.target_file_size_base = 128 * 1024 * 1024;
        opt.target_file_size_multiplier = 1;
        opt.max_background_flushes = 2;
        opt.max_background_compactions = 4;
        opt.background_rate_limit = 0;
        opt.disable_auto_compactions = false;
        opt.read_checksum = true;
        opt.level0_file_num_compaction_trigger = 8;
        opt.level0_slowdown_writes_trigger = 40;
        opt.level0_stop_writes_trigger = 46;
        opt.disable_wal = false;
        opt.cache_index_and_filter_blocks = false;
        opt.compression = 0;

        opt.min_blob_size = 0;
        opt.blob_file_size = 256 * 1024 * 1024;
        opt.enable_garbage_collection = true;
        opt.blob_gc_percent = 75;
        opt.blob_compression = 0;
        opt.blob_cache_size = 0;
        opt.blob_ttl_range = 3600; // in seconds

        opt.ttl = 0;
        opt.enable_stats = true;
        opt.enable_debug_log = false;

        db_manager.reset(new RocksDBManager(opt));

    } else {
        MasstreeOptions opt;
        opt.rcu_interval_ms = 2000;
        opt.data_path = path;
        db_manager.reset(new MasstreeDBManager(opt));
    }

    return db_manager->Init();
}

} /* namespace helper */
} /* namespace test */
} /* namespace chubaodb */

