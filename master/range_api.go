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

package master

import (
	"bytes"
	"context"
	"fmt"
	"github.com/chubaodb/chubaodb/master/entity"
	"github.com/chubaodb/chubaodb/master/entity/pkg/basepb"
	"github.com/chubaodb/chubaodb/master/entity/pkg/mspb"
	"github.com/chubaodb/chubaodb/master/service"
	"github.com/chubaodb/chubaodb/master/utils/cblog"
	"github.com/chubaodb/chubaodb/master/utils/ginutil"
	"github.com/chubaodb/chubaodb/master/utils/monitoring"
	"github.com/gin-gonic/gin"
	"github.com/patrickmn/go-cache"
	"github.com/spf13/cast"
	"net/http"
	"time"
)

type rangeApi struct {
	router       *gin.Engine
	rangeService *service.BaseService
	monitor      monitoring.Monitor
}

func ExportToRangeHandler(router *gin.Engine, rangeService *service.BaseService, monitor monitoring.Monitor) {

	c := &rangeApi{router: router, rangeService: rangeService, monitor: monitor}

	router.Handle(http.MethodPost, "/range/heartbeat", base30.PaincHandler, base30.TimeOutHandler, c.rangeHeartbeat, base30.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/range/ask_split", base30.PaincHandler, base30.TimeOutHandler, c.askSplit, base30.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/range/routers", base30.PaincHandler, base30.TimeOutHandler, c.getRouter, base30.TimeOutEndHandler)

}

var rangeCache = cache.New(20*time.Minute, 30*time.Minute)

func oldRangeCacheValue(key string) *mspb.RangeHeartbeatRequest {
	if get, b := rangeCache.Get(key); b && get != nil {
		return get.(*mspb.RangeHeartbeatRequest)
	}
	return nil
}

// range will register to store
func (ra *rangeApi) rangeHeartbeat(c *gin.Context) {
	ctx, _ := c.Get(Ctx)

	req := &mspb.RangeHeartbeatRequest{}
	if err := bind(c, req); err != nil {
		ginutil.NewAutoMehtodName(c, ra.monitor).Send(&mspb.RangeHeartbeatResponse{Header: entity.Err(err)})
		return
	}

	if req.Range.RangeEpoch == nil {
		req.Range.RangeEpoch = &basepb.RangeEpoch{}
	}

	key := cast.ToString(req.Range.Id)

	oldRequest := oldRangeCacheValue(key)

	var (
		save = true
		err  error
	)

	if oldRequest != nil {
		if save, err = isChangeRange(oldRequest, req); err != nil {
			ginutil.NewAutoMehtodName(c, ra.monitor).Send(&mspb.RangeHeartbeatResponse{Header: entity.Err(cblog.LogErrAndReturn(err))})
			return
		}
	}

	if save {
		up, err := ra.rangeService.Heartbeat(ctx.(context.Context), req.Range)
		if err != nil {
			ginutil.NewAutoMehtodName(c, ra.monitor).Send(&mspb.RangeHeartbeatResponse{Header: entity.Err(err)})
			return
		}
		if up || oldRequest == nil {
			rangeCache.Set(key, req, 20*time.Minute)
		} else {
			rangeCache.Set(key, oldRequest, 20*time.Minute)
		}
	}

	ginutil.NewAutoMehtodName(c, ra.monitor).Send(&mspb.RangeHeartbeatResponse{Header: entity.OK(), RangeId: req.Range.Id, Epoch: &basepb.RangeEpoch{ConfVer: req.Range.RangeEpoch.ConfVer, Version: req.Range.RangeEpoch.Version}})
}

func isChangeRange(old, new *mspb.RangeHeartbeatRequest) (bool, error) {

	if old.Term == new.Term && old.Range.RangeEpoch.ConfVer == new.Range.RangeEpoch.ConfVer && old.Range.RangeEpoch.Version == new.Range.RangeEpoch.Version {

		if bytes.Compare(old.Range.StartKey, new.Range.StartKey) != 0 || bytes.Compare(old.Range.EndKey, new.Range.EndKey) != 0 {
			return false, fmt.Errorf("version check rangeID:[%d] start[%s, %s] and end:[%s, %s] key not same ", old.Range.Id, old.Range.StartKey, new.Range.StartKey, old.Range.EndKey, new.Range.EndKey)
		}

		return false, nil
	}

	flag := old.Term > new.Term || old.Range.RangeEpoch.ConfVer > new.Range.RangeEpoch.ConfVer || old.Range.RangeEpoch.Version > new.Range.RangeEpoch.Version

	flag2 := old.Term < new.Term || old.Range.RangeEpoch.ConfVer < new.Range.RangeEpoch.ConfVer || old.Range.RangeEpoch.Version < new.Range.RangeEpoch.Version

	if flag == flag2 {
		//impossibility
		return false, fmt.Errorf("===================version check rangeID:[%d] version term:[%d] , confVer:[%d], version:[%d] contradiction cache term:[%d] , confVer:[%d], version:[%d] err", new.Range.Id, new.Term, new.Range.RangeEpoch.ConfVer, new.Range.RangeEpoch.Version, old.Term, old.Range.RangeEpoch.ConfVer, old.Range.RangeEpoch.Version)
	}

	if flag {
		return false, fmt.Errorf("version check rangeID:[%d] version term:[%d] , confVer:[%d], version:[%d] less than cache term:[%d] , confVer:[%d], version:[%d] err", new.Range.Id, new.Term, new.Range.RangeEpoch.ConfVer, new.Range.RangeEpoch.Version, old.Term, old.Range.RangeEpoch.ConfVer, old.Range.RangeEpoch.Version)
	}

	return true, nil

}

// ask to create a new range , it will return newRangeID , and peer ids , all id is unique
func (ra *rangeApi) askSplit(c *gin.Context) {
	ctx, _ := c.Get(Ctx)

	req := &mspb.AskSplitRequest{}
	if err := bind(c, req); err != nil {
		ginutil.NewAutoMehtodName(c, ra.monitor).Send(&mspb.AskSplitResponse{Header: entity.Err(err)})
		return
	}

	newRangeID, newPeerIDs, err := ra.rangeService.AskSplit(ctx.(context.Context), req.Range, req.Force)
	if err != nil {
		ginutil.NewAutoMehtodName(c, ra.monitor).Send(&mspb.AskSplitResponse{Header: entity.Err(err)})
		return
	}

	ginutil.NewAutoMehtodName(c, ra.monitor).Send(&mspb.AskSplitResponse{Header: entity.OK(), Range: req.Range, NewRangeId: newRangeID, NewPeerIds: newPeerIDs, SplitKey: req.SplitKey})
}

//get router for proxy , if max is zero it will return all
func (ra *rangeApi) getRouter(c *gin.Context) {
	ctx, _ := c.Get(Ctx)

	req := &mspb.GetRouteRequest{}
	if err := bind(c, req); err != nil {
		ginutil.NewAutoMehtodName(c, ra.monitor).Send(&mspb.GetRouteResponse{Header: entity.Err(err)})
		return
	}

	ranges, err := ra.rangeService.GetRoute(ctx.(context.Context), int(req.Max), req.DbId, req.TableId, req.Key)
	if err != nil {
		ginutil.NewAutoMehtodName(c, ra.monitor).Send(&mspb.GetRouteResponse{Header: entity.Err(err)})
		return
	}

	ginutil.NewAutoMehtodName(c, ra.monitor).Send(&mspb.GetRouteResponse{Header: entity.OK(), Routes: ranges})
}
