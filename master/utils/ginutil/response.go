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

package ginutil

import (
	"encoding/json"
	"github.com/chubaodb/chubaodb/master/entity"
	"github.com/chubaodb/chubaodb/master/utils/log"
	"github.com/chubaodb/chubaodb/master/utils/monitoring"
	"github.com/chubaodb/chubaodb/master/utils/reflect"
	"github.com/gin-gonic/gin"
	"net/http"
	"strings"
	"time"
)

const (
	Timeout    = "timeout"
	CancelFunc = "__cancelFunc"
	Ctx        = "__ctx"
	Start      = "__start_time"
)

type Response struct {
	ginContext *gin.Context
	monitor    monitoring.Monitor
}

func New(ginContext *gin.Context, monitor monitoring.Monitor) *Response {
	return &Response{
		ginContext: ginContext,
		monitor:    monitor,
	}
}

func NewAutoMehtodName(ginContext *gin.Context, monitor monitoring.Monitor) *Response {
	response := &Response{
		ginContext: ginContext,
	}

	if monitor != nil {
		response.monitor = monitor.New(reflect.RuntimeMethodName(2))
	}

	return response
}

func (resp *Response) Send(data entity.ProtoResp) {
	//if log.IsDebugEnabled() { TODO: REMOVE ME
	//	reply, _ := json.Marshal(data)
	//	log.Debug(string(reply))
	//}

	if resp.isProtoType() {
		reply, err := data.Marshal()
		if err != nil {
			log.Error("proto marshal response err:[%s] obj:[%v]", err.Error(), data)
			resp.Fail(err)
			return
		}
		resp.ginContext.Data(http.StatusOK, "application/proto", reply)
	} else {
		reply, err := json.Marshal(data)
		if err != nil {
			log.Error("json marshal response err:[%s] obj:[%v]", err.Error(), data)
			resp.Fail(err)
			return
		}
		resp.ginContext.Data(http.StatusOK, "application/json", reply)
	}

	//write monitor info
	if resp.monitor != nil {
		if value, exists := resp.ginContext.Get(Start); exists {
			resp.monitor.FunctionTP(value.(time.Time), false)
		}
	}
}

func (resp *Response) SendJson(obj interface{}) {
	reply, err := json.Marshal(obj)
	if err != nil {
		log.Error("json marshal response err:[%s] obj:[%v]", err.Error(), obj)
		resp.Fail(err)
		return
	}
	resp.ginContext.Data(http.StatusOK, "application/json", reply)
}

func (resp *Response) isProtoType() bool {
	cType := resp.ginContext.Request.Header.Get("Content-Type")
	if cType == "" {
		return false
	}
	return strings.HasPrefix(cType, "application/proto")
}

func (resp *Response) Fail(err error) {
	resp.ginContext.Data(http.StatusInternalServerError, "application/text", []byte(err.Error()))
}
