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
    // TODO: support rocksdb
    MasstreeOptions opt;
    opt.rcu_interval_ms = 2000;
    opt.data_path = path;
    db_manager.reset(new MasstreeDBManager(opt));
    return db_manager->Init();
}

} /* namespace helper */
} /* namespace test */
} /* namespace chubaodb */

