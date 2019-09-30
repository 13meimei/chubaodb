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

#include "common/server_config.h"
#include "range_context_impl.h"
#include "range_server.h"

namespace chubaodb {
namespace ds {
namespace server {

class DefaultSplitPolicy : public range::SplitPolicy {
public:
    bool IsEnabled() const override { return true; }

    std::string Description() const override {
        return std::string("Default");
    }

    uint64_t CheckSize() const override {
        return ds_config.range_config.check_size;
    }

    uint64_t SplitSize() const override {
        return ds_config.range_config.split_size;
    }

    uint64_t MaxSize() const override {
        return ds_config.range_config.max_size;
    }
};

RangeContextImpl::RangeContextImpl(ContextServer *s, TimerQueue* timer_queue) :
    server_(s),
    timer_queue_(timer_queue),
    split_policy_(new DefaultSplitPolicy) {
}

uint64_t RangeContextImpl::GetDBUsagePercent() const {
    return server_->run_status->GetDBUsedPercent();
}

void RangeContextImpl::ScheduleCheckSize(uint64_t range_id) {
    server_->range_server->StatisPush(range_id);
}

std::shared_ptr<range::Range> RangeContextImpl::FindRange(uint64_t range_id) {
    return server_->range_server->Find(range_id);
}

// split
Status RangeContextImpl::SplitRange(uint64_t range_id, const dspb::SplitCommand &req,
                  uint64_t raft_index) {
    return server_->range_server->SplitRange(range_id, req, raft_index);
}

}  // namespace server
}  // namespace ds
}  // namespace chubaodb
