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

#include <cstddef>

namespace chubaodb {
namespace raft {
namespace bench {

struct BenchConfig {
    std::size_t request_num = 1000000;
    std::size_t thread_num = 3;
    std::size_t range_num = 1;
    std::size_t concurrency = 100;

    bool use_memory_raft_log = false;
    bool use_inprocess_transport = false;
    std::size_t raft_thread_num = 1;
    std::size_t apply_thread_num = 1;
};

extern BenchConfig bench_config;

} /* namespace bench */
} /* namespace raft */
} /* namespace chubaodb */
