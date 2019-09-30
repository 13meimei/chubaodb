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

#include "admin_server.h"

#include <sstream>
#include <functional>

#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "jemalloc/jemalloc.h"

#include "server/version.h"
#include "server/range_server.h"
#include "server/run_status.h"
#include "server/worker.h"

namespace chubaodb {
namespace ds {
namespace admin {

using namespace std;
using namespace rapidjson;
using chubaodb::ds::server::ContextServer;

using JsonWriter = rapidjson::Writer<rapidjson::StringBuffer>;
using GetInfoFunc = std::function<Status(ContextServer*, const vector<string>&, JsonWriter&)>;
using GetInfoFunMap = std::map<std::string, GetInfoFunc>;

static vector<string> parsePath(const std::string& str) {
    vector<string> result;
    std::stringstream ss(str);
    std::string item;
    while (std::getline(ss, item, '.')) {
        result.push_back(item);
    }
    return result;
}

static Status getServerInfo(ContextServer* ctx, const vector<string>& path, JsonWriter& writer) {
    writer.Key("version");
    writer.String(server::GetGitDescribe().c_str());
    writer.Key("build_date");
    writer.String(server::GetBuildDate().c_str());
    writer.Key("build_type");
    writer.String(server::GetBuildType().c_str());

    writer.Key("range_count");
    writer.Uint64(ctx->range_server->GetRangesCount());

    writer.Key("leader_count");
    writer.Uint64(ctx->run_status->GetLeaderCount());

    writer.Key("db_usage_percent");
    writer.Uint64(ctx->run_status->GetDBUsedPercent());

    writer.Key("fast_queue_size");
    writer.Uint64(ctx->worker->FastQueueSize());
    writer.Key("slow_queue_size");
    writer.Uint64(ctx->worker->SlowQueueSize());
    return Status::OK();
}

static Status getRangeInfo(ContextServer* ctx, const vector<string>& path, JsonWriter& writer) {
    assert(!path.empty());
    auto rs = ctx->range_server;
    if (path.size() == 1) {
        writer.Key("count");
        writer.Uint64(rs->GetRangesCount());
        return Status::OK();
    }

    // range.{range_id}
    uint64_t id = 0;
    try {
        id = std::stoull(path[1]);
    } catch (std::exception &e) {
        return Status(Status::kInvalidArgument, "raft id", path[1]);
    }
    auto rng = rs->Find(id);
    if (rng == nullptr) {
        return Status(Status::kNotFound, "range", std::to_string(id));
    }
    writer.Key("valid");
    writer.Bool(rng->Valid());

    // add meta infos
    auto meta = rng->GetMeta();
    writer.Key("version");
    writer.Uint64(meta.range_epoch().version());
    writer.Key("conf_ver");
    writer.Uint64(meta.range_epoch().conf_ver());
    writer.Key("start_key");
    writer.String(EncodeToHex(meta.start_key()).c_str());
    writer.Key("end_key");
    writer.String(EncodeToHex(meta.end_key()).c_str());
    writer.Key("peers");
    writer.StartArray();
    for (const auto& peer: meta.peers()) {
        writer.StartObject();
        writer.Key("node_id");
        writer.Uint64(peer.node_id());
        writer.Key("peer_id");
        writer.Uint64(peer.id());
        writer.Key("type");
        writer.String(basepb::PeerType_Name(peer.type()).c_str());
        writer.EndObject();
    }
    writer.EndArray();

    auto split_range_id = rng->GetSplitRangeID();
    if (split_range_id > 0) {
        writer.Key("split_range_id");
        writer.Uint64(split_range_id);
    }
    writer.Key("submit_queue");
    writer.Uint64(rng->GetSubmitQueueSize());

    // table info
    writer.Key("table_id");
    writer.Uint64(meta.table_id());
    writer.Key("pks");
    writer.StartArray();
    for (const auto& pk : meta.primary_keys()) {
        writer.Key("id");
        writer.Uint64(pk.id());
        writer.Key("name");
        writer.String(pk.name().c_str());
        writer.Key("type");
        writer.String(basepb::DataType_Name(pk.data_type()).c_str());
    }
    writer.EndArray();

    return Status::OK();
}

static Status getRaftInfo(ContextServer* ctx, const vector<string>& path, JsonWriter& writer) {
    assert(!path.empty());

    auto rs = ctx->raft_server;
    if (path.size() == 1) {
        raft::ServerStatus ss;
        rs->GetStatus(&ss);
        writer.Key("count");
        writer.Uint64(ss.total_rafts_count);
        writer.Key("snap_apply");
        writer.Uint64(ss.total_snap_applying);
        writer.Key("snap_send");
        writer.Uint64(ss.total_snap_sending);
        return Status::OK();
    }

    // raft.{range_id}
    uint64_t id = 0;
    try {
        id = std::stoull(path[1]);
    } catch (std::exception &e) {
        return Status(Status::kInvalidArgument, "raft id", path[1]);
    }
    auto raft = rs->FindRaft(id);
    if (raft == nullptr) {
        return Status(Status::kNotFound, "raft", std::to_string(id));
    }
    raft::RaftStatus stat;
    raft->GetStatus(&stat);
    writer.Key("node_id");
    writer.Uint64(stat.node_id);
    writer.Key("leader");
    writer.Uint64(stat.leader);
    writer.Key("term");
    writer.Uint64(stat.term);
    writer.Key("index");
    writer.Uint64(stat.index);
    writer.Key("commit");
    writer.Uint64(stat.commit);
    writer.Key("applied");
    writer.Uint64(stat.applied);
    writer.Key("state");
    writer.String(stat.state.c_str());
    writer.Key("replicas");
    writer.StartArray();
    for (const auto&pr : stat.replicas) {
        writer.StartObject();
        writer.Key("node_id");
        writer.Uint64(pr.second.peer.node_id);
        writer.Key("peer_id");
        writer.Uint64(pr.second.peer.peer_id);
        writer.Key("match");
        writer.Uint64(pr.second.match);
        writer.Key("commit");
        writer.Uint64(pr.second.commit);
        writer.Key("next");
        writer.Uint64(pr.second.next);
        writer.Key("inactive_secs");
        writer.Int(pr.second.inactive_seconds);
        writer.Key("snapshoting");
        writer.Bool(pr.second.snapshotting);
        writer.Key("state");
        writer.String(pr.second.state.c_str());
        writer.EndObject();
    }
    writer.EndArray();

    return Status::OK();
}

static Status getRocksdbInfo(ContextServer* ctx, const vector<string>& path, JsonWriter& writer) {
    writer.Key("version");
    writer.String(server::GetRocksdbVersion().c_str());
    return Status::OK();
}

// get jemalloc stats, from jemalloc wiki
// see https://github.com/jemalloc/jemalloc/wiki/Use-Case%3A-Introspection-Via-mallctl%2A%28%29
static Status getMemoryInfo(ContextServer* ctx, const vector<string>& path, JsonWriter& writer) {
    // Update the statistics cached by je_mallctl.
    uint64_t epoch = 1;
    size_t sz = sizeof(epoch);
    mallctl("epoch", &epoch, &sz, &epoch, sz);

    // Get basic allocation statistics.  Take care to check for
    // errors, since --enable-stats must have been specified at
    // build time for these statistics to be available.
    size_t allocated = 0, active = 0, metadata = 0, resident = 0, mapped = 0;
    sz = sizeof(size_t);
    mallctl("stats.allocated", &allocated, &sz, NULL, 0);
    mallctl("stats.active", &active, &sz, NULL, 0);
    mallctl("stats.metadata", &metadata, &sz, NULL, 0);
    mallctl("stats.resident", &resident, &sz, NULL, 0);
    mallctl("stats.mapped", &mapped, &sz, NULL, 0);

    writer.Key("allocated");
    writer.Uint64(allocated);
    writer.Key("active");
    writer.Uint64(active);
    writer.Key("metadata");
    writer.Uint64(metadata);
    writer.Key("resident");
    writer.Uint64(resident);
    writer.Key("mapped");
    writer.Uint64(mapped);

    return Status::OK();
}

static const GetInfoFunMap get_info_funcs = {
        {"", getServerInfo},
        {"server", getServerInfo},
        {"raft", getRaftInfo},
        {"range", getRangeInfo},
        {"rocksdb", getRocksdbInfo},
        {"memory", getMemoryInfo},
};

Status AdminServer::getInfo(const dspb::GetInfoRequest& req, dspb::GetInfoResponse* resp) {
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    writer.StartObject();

    auto paths = parsePath(req.path());
    auto type = paths.empty() ? "" : paths[0];
    auto it = get_info_funcs.find(type);
    if (it == get_info_funcs.end()) {
        return Status(Status::kNotSupported, "get info", type);
    }

    auto s = (it->second)(context_, paths, writer);
    if (!s.ok()) {
        return s;
    }
    writer.EndObject();
    resp->set_data(buffer.GetString());
    return Status::OK();
}

} // namespace admin
} // namespace ds
} // namespace chubaodb

