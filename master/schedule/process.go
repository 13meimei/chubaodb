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

package schedule

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/chubaodb/chubaodb/master/entity"
	"github.com/chubaodb/chubaodb/master/entity/pkg/basepb"
	"github.com/chubaodb/chubaodb/master/entity/pkg/dspb"
	"github.com/chubaodb/chubaodb/master/service"
	"github.com/chubaodb/chubaodb/master/utils/hack"
	"github.com/spf13/cast"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jasonlvhit/gocron"

	"github.com/chubaodb/chubaodb/master/utils/log"
	"go.etcd.io/etcd/clientv3/concurrency"
)

type ProcessJob interface {
	/**
	@param nodeMap [nodeID] node
	@param nodeInfoMap [nodeID] nodeInfo
	@param nodeRangeNum [nodeID] NumRangeInNode
	@param ranges rangeList
	@param rangeGroup [dbID][tableID] range
	@param tableMap [tableID] table
	@param dbMap [dbID] dbn
	*/
	process(ctx context.Context, stop *bool, nodeMap map[uint64]*basepb.Node, nodeInfoMap map[uint64]*dspb.NodeInfoResponse, nodeRangeNum map[uint64]int, ranges []*basepb.Range, rangeGroup map[[2]uint64][]*basepb.Range, tableMap map[uint64]*basepb.Table, dbMap map[uint64]*basepb.DataBase)
}

var skipJob = fmt.Errorf("skip job")

func process(mCtx context.Context, service *service.BaseService, jobs []ProcessJob) {

	ctx, cancel := context.WithTimeout(mCtx, 5*time.Minute)
	defer cancel()

	defer func() {
		if err := recover(); err != nil {
			log.Error("!!!!!!!!!!!System ERROR: " + cast.ToString(err))
		}
		select {
		case err := <-ctx.Done():
			log.Error("process job stop timeout is [%d]s so cancel it info:[%v]", entity.Conf().Global.ScheduleSecond, err)
		default:
			cancel()
		}

	}()

	scheduleTime := int64(entity.Conf().Global.ScheduleSecond) * int64(time.Second)

	var err = service.STM(ctx, func(stm concurrency.STM) error {
		timeBytes := stm.Get(entity.ClusterCleanJobKey)

		var value uint64

		if len(timeBytes) == 0 {
			value = 0
		} else {
			value = binary.LittleEndian.Uint64([]byte(timeBytes))
		}

		if time.Now().UnixNano() > int64(value) {
			bytes := make([]byte, 8)
			binary.LittleEndian.PutUint64(bytes, uint64(time.Now().UnixNano()+scheduleTime))
			stm.Put(entity.ClusterCleanJobKey, string(bytes))
			return nil
		}
		return skipJob
	})
	if err == skipJob {
		log.Info("skip schedule task .....")
		return
	}
	if err != nil {
		log.Error("clean task has err for get ClusterCleanJobKey err: %s", err.Error())
		return
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				bytes := make([]byte, 8)
				binary.LittleEndian.PutUint64(bytes, uint64(time.Now().UnixNano()+scheduleTime))
				if err := service.Store.Put(ctx, entity.ClusterCleanJobKey, bytes); err != nil {
					log.Error("reput master has err:[%s]", err.Error())
					cancel()
				}
			}
			time.Sleep(time.Duration(scheduleTime / 3))
		}
	}()

	now := time.Now()

	log.Info("Start clean task")

	ranges, rangeGroup, err := groupRanges(ctx, service)
	if err != nil {
		log.Error("query all ranges err:[%s]", err)
		//TODO alarm
		return
	}

	tableGroup, err := groupTables(ctx, service)
	if err != nil {
		log.Error("prefix scan table err:[%s]  ", err.Error())
		//TODO alarm
		return
	}

	databaseMap, err := groupDatabase(ctx, service)
	if err != nil {
		log.Error("prefix scan database err:[%s]  ", err.Error())
		//TODO alarm
		return
	}

	endNodeNum, nodeMap, nodeInfoMap, err := groupNodeInfo(ctx, cancel, service)
	if err != nil {
		log.Error("query all nodes err:[%s]", err)
		//TODO alarm
		return
	}

	for atomic.LoadInt64(endNodeNum) > 0 {
		select {
		case <-ctx.Done():
			return
		default:
			time.Sleep(time.Millisecond * 100)
		}
	}

	nodeRangeNum := groupNodeRangeNum(nodeMap, nodeInfoMap)

	if len(nodeInfoMap) != len(nodeMap) {
		log.Error("nodeInfoMap:[%d] not equals nodeMap:[%d] so skip schedule ", len(nodeInfoMap), len(nodeMap))
		return
	}

	stop := hack.PBool(false)
	for i := 0; i < len(jobs); i++ {
		jobs[i].process(ctx, stop, nodeMap, nodeInfoMap, nodeRangeNum, ranges, rangeGroup, tableGroup, databaseMap)
	}

	log.Info("process job over use time:[%v]", time.Now().Sub(now))

}

func groupNodeInfo(ctx context.Context, cancel context.CancelFunc, service *service.BaseService) (*int64, map[uint64]*basepb.Node, map[uint64]*dspb.NodeInfoResponse, error) {
	nodeMap := make(map[uint64]*basepb.Node)
	nodeInfoMap := make(map[uint64]*dspb.NodeInfoResponse)

	nodes, err := service.QueryOnlineNodes(ctx)
	endNodeNum := hack.PInt64(int64(len(nodes)))
	if err != nil {
		log.Error("query all nodes err:[%s]", err)
		return nil, nil, nil, err
	}

	lock := sync.Mutex{}

	for _, node := range nodes {
		nodeMap[node.Id] = node
		go func(n *basepb.Node) {
			defer atomic.AddInt64(endNodeNum, -1)
			if nodeInfo, err := service.NodeInfo(ctx, n); err != nil {
				log.Error("get nodeInfo [%d]:[%s] err:[%s]", n.Id, nodeAddr(n), err)
			} else {
				lock.Lock()
				nodeInfoMap[n.Id] = nodeInfo
				lock.Unlock()
			}
		}(node)
	}

	return endNodeNum, nodeMap, nodeInfoMap, nil
}

func groupNodeRangeNum(nodeMap map[uint64]*basepb.Node, nodeInfos map[uint64]*dspb.NodeInfoResponse) map[uint64]int {

	nodeRangeNumMap := make(map[uint64]int)

	for _, node := range nodeMap {
		nodeRangeNumMap[node.Id] = 0
	}

	for _, nodeInfo := range nodeInfos {
		nodeRangeNumMap[nodeInfo.NodeId] = len(nodeInfo.RangeInfos)
	}
	return nodeRangeNumMap
}

func groupDatabase(ctx context.Context, service *service.BaseService) (map[uint64]*basepb.DataBase, error) {
	bases, err := service.QueryDatabases(ctx)
	databaseMap := make(map[uint64]*basepb.DataBase, len(bases))
	if err != nil {
		return nil, err
	}

	for _, b := range bases {
		databaseMap[b.Id] = b
	}
	return databaseMap, nil
}

func groupTables(ctx context.Context, service *service.BaseService) (map[uint64]*basepb.Table, error) {
	group := make(map[uint64]*basepb.Table)
	_, values, err := service.PrefixScan(ctx, entity.PrefixTable)
	if err != nil {
		return nil, err
	}

	for _, bytes := range values {
		table := &basepb.Table{}
		if err := table.Unmarshal(bytes); err != nil {
			log.Error("unmarshal table err:[%s] , value:[%s] ", err.Error(), string(bytes))
			continue
		}
		group[table.Id] = table
	}

	return group, nil
}

//process ranges
func groupRanges(ctx context.Context, service *service.BaseService) ([]*basepb.Range, map[[2]uint64][]*basepb.Range, error) {

	group := make(map[[2]uint64][]*basepb.Range)

	_, values, err := service.PrefixScan(ctx, entity.PrefixRange)
	if err != nil {
		log.Error("prefix scan range err:[%s]  ", err.Error())
		return nil, group, err
	}

	ranges := make([]*basepb.Range, 0, len(values))
	for _, bytes := range values {
		rgn := &basepb.Range{}
		if err := rgn.Unmarshal(bytes); err != nil {
			log.Error("unmarshal range err:[%s] , value:[%s] ", err.Error(), string(bytes))
			continue
		}
		ranges = append(ranges, rgn)
	}

	for _, rgn := range ranges {
		group[[2]uint64{rgn.DbId, rgn.TableId}] = append(group[[2]uint64{rgn.DbId, rgn.TableId}], rgn)
	}
	return ranges, group, nil
}

//the job check range infos and fix it
func StartScheduleJob(ctx context.Context, service *service.BaseService) {

	if entity.Conf().Global.ScheduleSecond <= 0 {
		entity.Conf().Global.ScheduleSecond = 60
		log.Warn("not set ScheduleSecond in config so default:%d", entity.Conf().Global.ScheduleSecond)
	}

	if entity.Conf().Global.PeerDownSecond <= 0 {
		entity.Conf().Global.PeerDownSecond = 300
		log.Warn("not set PeerDownSecond in config so default:%d", entity.Conf().Global.PeerDownSecond)
	}

	if entity.Conf().Global.MemoryRatio <= 0 {
		entity.Conf().Global.MemoryRatio = 0.8
		log.Warn("not set MemoryRation in config so default:%d", entity.Conf().Global.MemoryRatio)
	}

	//diff range and table remove outside range , it need lock table by distlock
	//diff table and node range , to del range in ds
	scheduler := gocron.NewScheduler()
	scheduler.Every(uint64(entity.Conf().Global.ScheduleSecond)).Seconds().Do(process, ctx, service, []ProcessJob{
		&GCJob{service: service},
		&FailoverJob{service: service},
		&BalanceJob{service: service},
	})
	<-scheduler.Start()
}

func nodeAddr(node *basepb.Node) string {
	return fmt.Sprintf("%s:%d", node.GetIp(), node.GetServerPort())
}
