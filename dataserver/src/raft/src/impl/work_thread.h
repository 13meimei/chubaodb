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

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <functional>
#include <unordered_map>
#include "raft_types.h"

namespace chubaodb {
namespace raft {
namespace impl {

class RaftImpl;
class RaftServerImpl;

static const int kMaxBatchSize = 64;

struct Work {
    uint64_t owner = 0;
    std::atomic<bool>* stopped = nullptr;

    std::function<void()> f0;

    std::function<void(MessagePtr&)> f1;
    MessagePtr msg = nullptr;

    void Do();
};

class WorkThread {
public:
    WorkThread(RaftServerImpl* server, size_t queue_capcity,
               const std::string& name = "raft-worker");
    ~WorkThread();

    WorkThread(const WorkThread&) = delete;
    WorkThread& operator=(const WorkThread&) = delete;

    bool submit(uint64_t owner, std::atomic<bool>* stopped, uint64_t unique_sequence,
                uint16_t rw_flag, const std::function<void(MessagePtr&)>& handle, std::string& cmd);

    bool tryPost(const Work& w);
    void post(const Work& w);
    void waitPost(const Work& w);
    void shutdown();
    int size() const;

private:
    bool pull(Work* w);
    void run();

private:
    RaftServerImpl* server_ = nullptr;
    const size_t capacity_ = 0;

    std::unique_ptr<std::thread> thr_;
    bool running_ = false;
    std::queue<Work> queue_;
    std::unordered_map<uint64_t, MessagePtr> write_batch_pos_;
    std::unordered_map<uint64_t, MessagePtr> read_batch_pos_;
    mutable std::mutex mu_;
    std::condition_variable cv_;
};

} /* namespace impl */
} /* namespace raft */
} /* namespace chubaodb */
