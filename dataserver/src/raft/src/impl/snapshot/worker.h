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

#include <string>
#include <memory>
#include <condition_variable>
#include <thread>

namespace chubaodb {
namespace raft {
namespace impl {

class SnapTask;
class SnapWorkerPool;

class SnapWorker final {
public:
    explicit SnapWorker(SnapWorkerPool* pool, const std::string& thread_name);
    ~SnapWorker();

    SnapWorker(const SnapWorker&) = delete;
    SnapWorker& operator=(const SnapWorker&) = delete;

    void Post(const std::shared_ptr<SnapTask>& task);

private:
    void runTask();

private:
    SnapWorkerPool* pool_ = nullptr;

    bool running_ = true;

    std::shared_ptr<SnapTask> task_;
    std::mutex mu_;
    std::condition_variable cv_;
    std::thread thr_;
};

} /* namespace impl */
} /* namespace raft */
} /* namespace chubaodb */
