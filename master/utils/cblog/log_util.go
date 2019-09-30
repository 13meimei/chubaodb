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

package cblog

import (
	"fmt"
	"github.com/chubaodb/chubaodb/master/utils/log"
	"io"
	"runtime/debug"
)

//log err if not nil
func CloseIfNotNil(c io.Closer) {
	err := c.Close()

	if err == nil {
		return
	}

	if log.IsInfoEnabled() {
		fmt.Println(string(debug.Stack()))
	}

	log.Error(err.Error())
}

//log err if not nil
func FunIfNotNil(f func() error) {
	err := f()

	if err == nil {
		return
	}

	if log.IsDebugEnabled() {
		fmt.Println(string(debug.Stack()))
	}

	log.Error(err.Error())
}

//log err if not nil
func LogErrAndReturn(err error) error {
	if err == nil {
		return err
	}
	if log.IsInfoEnabled() {
		fmt.Println(string(debug.Stack()))
	}
	log.Error(err.Error())
	return err
}
