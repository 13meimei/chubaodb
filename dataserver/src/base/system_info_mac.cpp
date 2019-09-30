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

#include "system_info_mac.h"

#include <sys/param.h>
#include <sys/mount.h>
#include <cstring>

namespace chubaodb {

bool MacSystemInfo::GetFileSystemUsage(const char *path, uint64_t *total, uint64_t *available) const {
    struct statfs buf;
    memset(&buf, 0, sizeof(buf));
    int ret = ::statfs(path, &buf);
    if (ret == 0) {
        *total = buf.f_bsize * buf.f_blocks;
        *available = buf.f_bsize * buf.f_bavail;
        return true;
    } else {
        return false;
    }
}

bool MacSystemInfo::GetMemoryUsage(uint64_t *total, uint64_t *available) const {
    // TODO: fix me
    *total = 8UL * 1024 * 1024 * 1024;
    *available = 4UL * 1024 * 1024 * 1024;
    return true;
}

}  // namespace chubaodb
