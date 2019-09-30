// Copyright 2019 The ChuBao Authors
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

package monitoring

import (
	"fmt"
	"time"
)

var _ Monitor = &ConsoleMonitor{}

type ConsoleMonitor struct {
	key string
}

func (ConsoleMonitor) New(key string) Monitor {
	return &ConsoleMonitor{key: key}
}

func (cm *ConsoleMonitor) Alive() {
	fmt.Println("monitor", cm.key, " alive")
}

func (cm *ConsoleMonitor) Alarm(detail string) {
	fmt.Println("monitor", cm.key, "alarm", detail)
}

func (cm *ConsoleMonitor) FunctionTP(startTime time.Time, hasErr bool) {
	fmt.Println("monitor", cm.key, "functionTP use time:", time.Now().Sub(startTime), "err:", hasErr)
}
