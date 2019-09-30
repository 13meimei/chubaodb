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

#include <assert.h>
#include <sys/time.h>
#include <unistd.h>
#include <future>
#include <iostream>
#include <thread>
#include <vector>
#include <functional>

#ifdef USE_GPERF
#include <gperftools/profiler.h>
#endif

#include "address.h"
#include "config.h"
#include "node.h"
#include "common/logger.h"

using namespace chubaodb;
using namespace chubaodb::raft;
using namespace chubaodb::raft::bench;

struct BenchContext {
    std::atomic<int64_t> counter = {0};
    std::vector<std::shared_ptr<Range>> leaders;
};

void runBenchmark(BenchContext *ctx) {
    while (true) {
        std::vector<std::shared_future<bool>> futures;
        for (size_t i = 0; i < bench_config.concurrency; ++i) {
            auto num = ctx->counter.fetch_sub(1);
            if (num > 0) {
                futures.push_back(
                    (ctx->leaders)[num % ctx->leaders.size()]->AsyncRequest());
            } else {
                break;
            }
        }
        if (!futures.empty()) {
            for (auto &f : futures) {
                f.wait();
                assert(f.valid());
                assert(f.get());
            }
        } else {
            return;
        }
    }
}

int main(int argc, char *argv[]) {
    auto addr_mgr = std::make_shared<bench::NodeAddress>(3);

    std::vector<std::shared_ptr<bench::Node>> cluster;
    for (size_t i = 1; i <= 3; ++i) {
        auto node = std::make_shared<bench::Node>(i, addr_mgr);
        node->Start();
        cluster.push_back(node);
    }

    BenchContext context;
    context.counter = bench_config.request_num;
    context.leaders.resize(bench_config.range_num);
    for (uint64_t i = 1; i <= bench_config.range_num; ++i) {
        for (auto &n : cluster) {
            auto r = n->GetRange(i);
            r->WaitLeader();
            if (r->IsLeader()) {
                context.leaders[i-1] = r;
            }
        }
    }

#ifdef USE_GPERF
    ProfilerStart("./bench.prof");
#endif
    struct timeval start, end, taken;
    gettimeofday(&start, NULL);

    std::vector<std::thread> threads;
    for (size_t i = 0; i < bench_config.thread_num; ++i) {
        threads.emplace_back(std::thread(std::bind(&runBenchmark, &context)));
    }
    for (auto &t : threads) {
        t.join();
    }

    gettimeofday(&end, NULL);
    timersub(&end, &start, &taken);
#ifdef USE_GPERF
    ProfilerStop();
#endif
    FLOG_INFO("bench_config.request_num: {}, requests taken: {}s{}ms",bench_config.request_num, taken.tv_sec, taken.tv_usec/100);
    //std::cout << bench_config.request_num << " requests taken " << taken.tv_sec << "s "
    //          << taken.tv_usec / 1000 << "ms" << std::endl;
    FLOG_INFO("ops: {}", (bench_config.request_num*100)/(taken.tv_sec*1000+taken.tv_usec/1000));
    //std::cout << "ops: "
    //          << (bench_config.request_num * 1000) /
    //                 (taken.tv_sec * 1000 + taken.tv_usec / 1000)
    //          << std::endl;

    return 0;
}
