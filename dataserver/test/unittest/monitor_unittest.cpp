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

#include <gtest/gtest.h>

#include "base/system_info.h"
#include "common/statistics.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace chubaodb;

TEST(Monitor, Basic) {
    auto s = SystemInfo::New();
    uint64_t total = 0, available = 0;
    ASSERT_TRUE(s->GetFileSystemUsage(".", &total, &available));
    ASSERT_GT(total, 0U);
    ASSERT_GT(available, 0U);
    ASSERT_GE(total, available);

    ASSERT_TRUE(s->GetMemoryUsage(&total, &available));
    std::cout << "memory total: " << total << std::endl;
    std::cout << "memory available: " << available << std::endl;
}

TEST(Monitor, Statistics) {
    Statistics s;
    s.PushTime(HistogramType::kRaft, 123);
    std::cout << s.ToString() << std::endl;
    s.ToString();
}

} /* namespace  */
