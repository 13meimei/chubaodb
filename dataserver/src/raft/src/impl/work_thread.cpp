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

#include "work_thread.h"

#include <assert.h>
#include <thread>
#include "base/util.h"
#include "common/logger.h"
#include "raft_exception.h"
#include "server_impl.h"

namespace chubaodb {
namespace raft {
namespace impl {

void Work::Do() {
    if (!(*stopped)) {
        if (msg == nullptr) {
            f0();
        } else {
            f1(msg);
        }
    }
}

WorkThread::WorkThread(RaftServerImpl* server, size_t queue_capcity,
                       const std::string& name)
    : server_(server), capacity_(queue_capcity), running_(true) {
    assert(server_ != nullptr);
    assert(capacity_ > 0);

    thr_.reset(new std::thread(std::bind(&WorkThread::run, this)));
    AnnotateThread(thr_->native_handle(), name.c_str());
}

WorkThread::~WorkThread() { shutdown(); }

bool WorkThread::submit(uint64_t owner, std::atomic<bool>* stopped, uint64_t unique_sequence,
                            uint16_t rw_flag, const std::function<void(MessagePtr&)>& handle,
                            std::string& cmd)
{
    decltype(write_batch_pos_.begin()) it;
    bool notify = false;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (!running_) {
            return false;
        }

        if (rw_flag == chubaodb::raft::WRITE_FLAG) {
            it = write_batch_pos_.find(owner);
            if (it != write_batch_pos_.end() && it->second->entries_size() < kMaxBatchSize) {
                //merge
                auto entry = it->second->add_entries();
                entry->set_type(pb::ENTRY_NORMAL);
                entry->mutable_data()->swap(cmd); //set entry->data by cmd
                return true;
            }
        } else {
            it = read_batch_pos_.find(owner);
            if (it != read_batch_pos_.end() && it->second->entries_size() < kMaxBatchSize) {
                auto entry = it->second->add_entries();
                entry->set_type(pb::ENTRY_NORMAL);
                entry->set_index(unique_sequence);
                entry->mutable_data()->swap(cmd); //set entry->data by cmd
                return true;
            }
        }

        if (queue_.size() >= capacity_) {
            return false;
        } else {
            // can't merge new message
            MessagePtr msg(new pb::Message);
            auto entry = msg->add_entries();
            entry->set_type(pb::ENTRY_NORMAL);
            entry->mutable_data()->swap(cmd); //set entry->data by cmd
            if (rw_flag == chubaodb::raft::WRITE_FLAG) {
                msg->set_type(pb::LOCAL_MSG_PROP);
                write_batch_pos_[owner] = msg;
            } else {
                msg->set_type(pb::LOCAL_MSG_READ);
                entry->set_index(unique_sequence);
                read_batch_pos_[owner] = msg;
            }
            Work w;
            w.owner = owner;
            w.stopped = stopped;
            w.f1 = handle;
            w.msg = msg;
            queue_.push(w);
            notify = true;
        }
    }

    if (notify) {
        cv_.notify_one();
    }
    return true;
}

bool WorkThread::tryPost(const Work& w) {
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (!running_) return false;
        if (queue_.size() >= capacity_) {
            FLOG_WARN("queue is full!!!, please wait");
            return false;
        } else {
            queue_.push(w);
        }
    }
    cv_.notify_one();
    return true;
}

void WorkThread::post(const Work& w) {
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (!running_) {
            FLOG_WARN("raft worker is not running, please check!");
            return;
        }
        queue_.push(w);
    }
    cv_.notify_one();
}

void WorkThread::waitPost(const Work& w) {
    std::unique_lock<std::mutex> lock(mu_);
    while (queue_.size() >= capacity_ && running_) {
        cv_.wait(lock);
    }

    if (running_) {
        queue_.push(w);
        lock.unlock();
        cv_.notify_one();
    }
}

void WorkThread::shutdown() {
    {
        std::unique_lock<std::mutex> lock(mu_);
        if (!running_) return;
        running_ = false;
    }
    cv_.notify_one();
    thr_->join();
}

void WorkThread::run() {
    std::queue<Work> work_queue;
    decltype(write_batch_pos_) write_batch_pos;
    decltype(write_batch_pos_) read_batch_pos;
    decltype(write_batch_pos_.begin()) it;

    while (running_) {
        std::unique_lock<std::mutex> lock(mu_);

        if (queue_.empty()) {
            cv_.wait(lock);
        }

        work_queue.swap(queue_);
        write_batch_pos.swap(write_batch_pos_);
        read_batch_pos.swap(read_batch_pos_);
        lock.unlock();
        cv_.notify_one();

        while(!work_queue.empty()) {
            Work w = work_queue.front();
            work_queue.pop();

            if (w.msg != nullptr) {
                switch (w.msg->type()) {
                case pb::LOCAL_MSG_PROP:
                    assert(w.owner != 0);
                    it = write_batch_pos.find(w.owner);
                    if (it != write_batch_pos.end() && it->second == w.msg) {
                        assert(it->second->type() == pb::LOCAL_MSG_PROP);
                        write_batch_pos.erase(it);
                    }
                    break;
                case pb::LOCAL_MSG_READ:
                    assert(w.owner != 0);
                    it = read_batch_pos.find(w.owner);
                    if (it != read_batch_pos.end() && it->second == w.msg) {
                        assert(it->second->type() == pb::LOCAL_MSG_READ);
                        read_batch_pos.erase(it);
                    }
                    break;
                default:
                    FLOG_WARN("ignore message type: {}", w.msg->type());
                }
            }
            try {
                w.Do();
            } catch (RaftException& e) {
                assert(w.owner > 0);
                FLOG_ERROR("raft[{}] throw an exception: {}. removed.", w.owner, e.what());
                server_->RemoveRaft(w.owner);
            }
        }
    }
}

int WorkThread::size() const {
    std::lock_guard<std::mutex> lock(mu_);
    return queue_.size();
}

} /* namespace impl */
} /* namespace raft */
} /* namespace chubaodb */
