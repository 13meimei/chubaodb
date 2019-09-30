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

#include "system_info.h"

#ifdef __linux__
#include "system_info_linux.h"
#elif defined(__APPLE__)
#include "system_info_mac.h"
#else
#error unsupport platform
#endif

namespace chubaodb {

std::unique_ptr<SystemInfo> SystemInfo::New(bool docker) {
#ifdef __linux__
    if (!docker) {
        return std::unique_ptr<SystemInfo>(new LinuxSystemInfo);
    } else {
        return std::unique_ptr<SystemInfo>(new DockerLinuxSystemInfo);
    }
#elif defined(__APPLE__)
    return std::unique_ptr<SystemInfo>(new MacSystemInfo);
#else
#error unsupport platform
#endif
}

} // namespace chubaodb
