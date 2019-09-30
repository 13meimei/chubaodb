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

#include "storage_disk.h"

#include <assert.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <algorithm>
#include <sstream>

#include "common/logger.h"
#include "base/util.h"
#include "base/fs_util.h"
#include "log_file.h"

namespace chubaodb {
namespace raft {
namespace impl {
namespace storage {

DiskStorage::DiskStorage(uint64_t id, const std::string& path, const Options& ops)
    : id_(id), path_(path), ops_(ops), meta_file_(path) {}

DiskStorage::~DiskStorage() { Close(); }

Status DiskStorage::Open() {
    auto s = initDir();
    if (!s.ok()) {
        return s;
    }

    s = initMeta();
    if (!s.ok()) {
        return s;
    }

    s = openLogs();
    if (!s.ok()) {
        return s;
    }

    assert(!log_files_.empty());
    uint64_t first = log_files_[0]->Index();
    if (trunc_meta_.index() + 1 < first) {
        return Status(
            Status::kCorruption, "inconsistent truncate meta with log files",
            std::to_string(trunc_meta_.index() + 1) + " < " + std::to_string(first));
    }

    return Status::OK();
}

Status DiskStorage::initDir() {
    assert(!path_.empty());
    auto ret = ops_.readonly ? CheckDirExist(path_) : MakeDirAll(path_, 0755);
    if (!ret) {
        return Status(Status::kIOError, "init directory " + path_, strErrno(errno));
    }
    return Status::OK();
}

Status DiskStorage::initMeta() {
    auto s = meta_file_.Open(ops_.readonly);
    if (!s.ok()) {
        return s;
    }

    s = meta_file_.Load(&hard_state_, &trunc_meta_);
    if (!s.ok()) {
        return s;
    }

    if (!ops_.readonly && ops_.initial_first_index > 1) {
        if (trunc_meta_.index() > 1 || hard_state_.commit() > 1) {
            std::ostringstream ss;
            ss << "incompatible trunc index or commit: (" << trunc_meta_.index() << ", ";
            ss << hard_state_.commit() << ")";
            return Status(Status::kInvalidArgument, "initial truncate", ss.str());
        }

        hard_state_.set_commit(ops_.initial_first_index - 1);
        s = meta_file_.SaveHardState(hard_state_);
        if (!s.ok()) {
            return s;
        }

        trunc_meta_.set_index(ops_.initial_first_index - 1);
        trunc_meta_.set_term(1);
        s = meta_file_.SaveTruncMeta(trunc_meta_);
        if (!s.ok()) {
            return s;
        }

        s = meta_file_.Sync();
        if (!s.ok()) {
            return s;
        }
    }
    return s;
}

Status DiskStorage::checkLogsValidate(const std::map<uint64_t, uint64_t>& logs) {
    uint64_t prev_seq = 0;
    uint64_t prev_index = 0;
    for (auto it = logs.cbegin(); it != logs.cend(); ++it) {
        if (it != logs.cbegin()) {
            if (prev_seq + 1 != it->first || prev_index >= it->second) {
                std::ostringstream ss;
                ss << "invalid log file order between (" << prev_seq << "-" << prev_index
                   << ") and (" << it->first << "-" << it->second << ")";
                return Status(Status::kCorruption, "raft logger", ss.str());
            }
        }
        prev_seq = it->first;
        prev_index = it->second;
    }
    return Status::OK();
}

Status DiskStorage::listLogs(std::map<uint64_t, uint64_t>* logs) {
    logs->clear();

    DIR* dir = ::opendir(path_.c_str());
    if (NULL == dir) {
        return Status(Status::kIOError, "call opendir", strErrno(errno));
    }

    struct dirent* ent = NULL;
    while (true) {
        errno = 0;
        ent = ::readdir(dir);
        if (NULL == ent) {
            if (0 == errno) {
                break;
            } else {
                closedir(dir);
                return Status(Status::kIOError, "call readdir", strErrno(errno));
            }
        }
        // TODO: call stat if d_type is DT_UNKNOWN
        if (ent->d_type == DT_REG || ent->d_type == DT_UNKNOWN) {
            uint64_t seq = 0;
            uint64_t offset = 0;
            if (!parseLogFileName(ent->d_name, seq, offset)) {
                continue;
            }
            auto it = logs->emplace(seq, offset);
            if (!it.second) {
                closedir(dir);
                return Status(Status::kIOError, "repeated log sequence",
                              std::to_string(seq));
            }
        }
    }
    closedir(dir);
    return Status::OK();
}

Status DiskStorage::openLogs() {
    std::map<uint64_t, uint64_t> logs;
    auto s = listLogs(&logs);
    if (!s.ok()) return s;
    s = checkLogsValidate(logs);
    if (!s.ok()) return s;

    if (logs.empty()) {
        if (ops_.readonly) {
            return Status(Status::kCorruption, "open logs", "no log file");
        }
        auto f = new LogFile(path_, 1, trunc_meta_.index() + 1);
        s = f->Open(ops_.allow_corrupt_startup);
        if (!s.ok()) {
            return s;
        }
        log_files_.push_back(f);
    } else {
        size_t count = 0;
        for (auto it = logs.begin(); it != logs.end(); ++it) {
            auto f = new LogFile(path_, it->first, it->second, ops_.readonly);
            s = f->Open(ops_.allow_corrupt_startup, count == logs.size() - 1);
            if (!s.ok()) {
                return s;
            } else {
                log_files_.push_back(f);
            }
            ++count;
        }
    }

    auto last = log_files_.back();
    last_index_ = (last->LogSize() == 0) ? (last->Index() - 1) : last->LastIndex();

    return Status::OK();
}

Status DiskStorage::closeLogs() {
    std::for_each(log_files_.begin(), log_files_.end(), [](LogFile* f) { delete f; });
    log_files_.clear();
    return Status::OK();
}

Status DiskStorage::StoreHardState(const pb::HardState& hs) {
    if (ops_.readonly) {
        return Status(Status::kNotSupported, "store hard state", "read only");
    }

    auto s = meta_file_.SaveHardState(hs);
    if (!s.ok()) return s;
    hard_state_ = hs;

    if (ops_.always_sync) {
        return meta_file_.Sync();
    } else {
        return Status::OK();
    }
}

Status DiskStorage::InitialState(pb::HardState* hs) const {
    *hs = hard_state_;
    return Status::OK();
}

Status DiskStorage::tryRotate() {
    assert(!log_files_.empty());
    auto f = log_files_.back();
    if (f->FileSize() >= ops_.log_file_size) {
        auto s = f->Rotate();
        if (!s.ok()) {
            return s;
        }
        auto newf = new LogFile(path_, f->Seq() + 1, last_index_ + 1);
        s = newf->Open(false);
        if (!s.ok()) {
            return s;
        }
        log_files_.push_back(newf);
    }
    return Status::OK();
}

Status DiskStorage::save(const EntryPtr& e) {
    auto s = tryRotate();
    if (!s.ok()) return s;
    auto f = log_files_.back();
    s = f->Append(e);
    if (!s.ok()) {
        return s;
    }
    last_index_ = e->index();
    return Status::OK();
}

Status DiskStorage::StoreEntries(const std::vector<EntryPtr>& entries) {
    if (ops_.readonly) {
        return Status(Status::kNotSupported, "store entries", "read only");
    }

    if (entries.empty()) {
        return Status::OK();
    }

    Status s;
    for (size_t i = 1; i < entries.size(); ++i) {
        if (entries[i]->index() != entries[i - 1]->index() + 1) {
            std::ostringstream ss;
            ss << "discontinuous index (" << entries[i]->index() << "-";
            ss << entries[i - 1]->index() << ") at input entries index " << i-1;
            return Status(Status::kInvalidArgument, "StoreEntries", ss.str());
        }
    }

    if (entries[0]->index() > last_index_ + 1) {
        std::ostringstream ss;
        ss << "append log index " << entries[0]->index() << " out of bound: ";
        ss << "current last index is " << last_index_;
        return Status(Status::kInvalidArgument, "store entries", ss.str());
    } else if (entries[0]->index() <= last_index_) {
        s = truncateNew(entries[0]->index());
        if (!s.ok()) {
            return s;
        }
    }

    for (const auto& e : entries) {
        s = save(e);
        if (!s.ok()) {
            return s;
        }
    }
    // flush
    s = log_files_.back()->Flush();
    if (!s.ok()) {
        return s;
    }
    // sync
    if (ops_.always_sync) {
        return log_files_.back()->Sync();
    } else {
        return Status::OK();
    }
}

Status DiskStorage::Term(uint64_t index, uint64_t* term, bool* is_compacted) const {
    if (index < trunc_meta_.index()) {
        *term = 0;
        *is_compacted = true;
        return Status::OK();
    } else if (index == trunc_meta_.index()) {
        *term = trunc_meta_.term();
        *is_compacted = false;
        return Status::OK();
    } else if (index > last_index_) {
        return Status(Status::kInvalidArgument, "out of bound", std::to_string(index));
    } else {
        *is_compacted = false;
        auto it = std::lower_bound(log_files_.cbegin(), log_files_.cend(), index,
                                   [](LogFile* f, uint64_t index) { return f->LastIndex() < index; });
        if (it == log_files_.cend()) {
            return Status(Status::kNotFound, "locate term log file", std::to_string(index));
        }
        return (*it)->Term(index, term);
    }
}

Status DiskStorage::FirstIndex(uint64_t* index) const {
    *index = trunc_meta_.index() + 1;
    return Status::OK();
}

Status DiskStorage::LastIndex(uint64_t* index) const {
    *index = std::max(last_index_, trunc_meta_.index());
    return Status::OK();
}

Status DiskStorage::Entries(uint64_t lo, uint64_t hi, uint64_t max_size,
                            std::vector<EntryPtr>* entries, bool* is_compacted) const {
    if (lo <= trunc_meta_.index()) {
        *is_compacted = true;
        return Status::OK();
    } else if (hi > last_index_ + 1) {
        return Status(Status::kInvalidArgument, "out of bound", std::to_string(hi));
    }

    *is_compacted = false;

    // search start file
    auto it = std::lower_bound(log_files_.cbegin(), log_files_.cend(), lo,
            [](LogFile* f, uint64_t index) { return f->LastIndex() < index; });
    if (it == log_files_.cend()) {
        return Status(Status::kNotFound, "locate file", std::to_string(lo));
    }

    uint64_t size = 0;
    Status s;
    for (uint64_t index = lo; index < hi; ++index) {
        auto f = *it;
        if (index > f->LastIndex()) {
            ++it; // switch next file
            if (it == log_files_.cend()) {
                break;
            } else {
                f = *it;
            }
        }

        EntryPtr e;
        s = f->Get(index, &e);
        if (!s.ok()) return s;
        size += e->ByteSizeLong();
        if (size > max_size) {
            if (entries->empty()) {
                entries->push_back(e);
            }
            break;
        } else {
            entries->push_back(e);
        }
    }
    return Status::OK();
}

Status DiskStorage::truncateOld(uint64_t index, uint64_t& trunc_index, uint64_t& trunc_term) {
    Status s;
    while (log_files_.size() > ops_.max_log_files) {
        auto f = log_files_[0];
        if (f->LastIndex() <= index) {
            trunc_index = f->LastIndex();
            s = f->Term(f->LastIndex(), &trunc_term);
            if (!s.ok()) {
                return s;
            }
            s = f->Destroy();
            if (!s.ok()) {
                return s;
            }
            delete f;
            log_files_.erase(log_files_.begin());
        } else {
            break;
        }
    }
    return Status::OK();
}

Status DiskStorage::truncateNew(uint64_t index) {
    Status s;
    while (!log_files_.empty()) {
        auto last = log_files_.back();
        if (last->Index() > index) {
            s = last->Destroy();
            if (!s.ok()) return s;
            delete last;
            log_files_.pop_back();
        } else {
            s = last->Truncate(index);
            if (!s.ok()) {
                return s;
            } else {
                last_index_ = index - 1;
                return Status::OK();
            }
        }
    }

    if (log_files_.empty()) {
        return Status(Status::kInvalidArgument, "append log index less than truncated",
                      std::to_string(index));
    }
    return Status::OK();
}

Status DiskStorage::truncateAll() {
    Status s;
    for (auto log_file : log_files_) {
        s = log_file->Destroy();
        if (!s.ok()) {
            return s;
        }
        delete log_file;
    }
    log_files_.clear();

    auto f = new LogFile(path_, 1, trunc_meta_.index() + 1);
    s = f->Open(false);
    if (!s.ok()) {
        return s;
    }
    log_files_.push_back(f);
    last_index_ = trunc_meta_.index();

    return Status::OK();
}

Status DiskStorage::Truncate(uint64_t index) {
    if (ops_.readonly) {
        return Status(Status::kNotSupported, "truncate", "read only");
    }

    if (log_files_.size() <= ops_.max_log_files || index <= trunc_meta_.index()) {
        return Status::OK();
    }

    uint64_t trunc_index = 0, trunc_term = 0;
    auto s = truncateOld(index, trunc_index, trunc_term);
    if (!s.ok()) {
        return s;
    }
    if (trunc_index != 0) {
        trunc_meta_.set_index(trunc_index);
        trunc_meta_.set_term(trunc_term);
        s = meta_file_.SaveTruncMeta(trunc_meta_);
        if (!s.ok()) {
            return s;
        }
        s = meta_file_.Sync();
        if (!s.ok()) {
            return s;
        }
        FLOG_DEBUG("raftlog[{}] truncate to {}", id_, trunc_index);
    }

    return Status::OK();
}

Status DiskStorage::ApplySnapshot(const pb::SnapshotMeta& meta) {
    if (ops_.readonly) {
        return Status(Status::kNotSupported, "apply snapshot", "read only");
    }

    hard_state_.set_commit(meta.index());
    auto s = meta_file_.SaveHardState(hard_state_);
    if (!s.ok()) {
        return s;
    }

    trunc_meta_.set_index(meta.index());
    trunc_meta_.set_term(meta.term());
    s = meta_file_.SaveTruncMeta(trunc_meta_);
    if (!s.ok()) {
        return s;
    }

    s = meta_file_.Sync();
    if (!s.ok()) {
        return s;
    }

    return truncateAll();
}

Status DiskStorage::Close() {
    auto s = meta_file_.Close();
    if (!s.ok()) return s;
    return closeLogs();
}

Status DiskStorage::Destroy(bool backup) {
    if (ops_.readonly) {
        return Status(Status::kNotSupported, "destroy", "read only");
    }

    bool flag = false;
    // only destroy once
    if (destroyed_.compare_exchange_strong(flag, true, std::memory_order_acquire,
                                           std::memory_order_relaxed)) {
        if (backup) {
            std::string bak_path = path_ + ".bak." + std::to_string(time(NULL));
            int ret = ::rename(path_.c_str(), bak_path.c_str());
            if (ret != 0) {
                return Status(Status::kIOError, "rename", strErrno(errno));
            }
        } else {
            if (!RemoveDirAll(path_.c_str())) {
                return Status(Status::kIOError, "RemoveDirAll", strErrno(errno));
            }
        }
    }
    return Status::OK();
}


#ifndef NDEBUG
void DiskStorage::TEST_Add_Corruption1() {
    //
    log_files_.back()->TEST_Append_RandomData();
}

void DiskStorage::TEST_Add_Corruption2() {
    //
    log_files_.back()->TEST_Truncate_RandomLen();
}

void DiskStorage::TEST_Add_Corruption3() {
    // TODO:
}
#endif

} /* namespace storage */
} /* namespace impl */
} /* namespace raft */
} /* namespace chubaodb */
