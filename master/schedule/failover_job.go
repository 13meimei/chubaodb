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
	"fmt"
	"github.com/chubaodb/chubaodb/master/entity"
	"github.com/chubaodb/chubaodb/master/entity/pkg/basepb"
	"github.com/chubaodb/chubaodb/master/service"
	"github.com/chubaodb/chubaodb/master/utils/log"
)

var _ ProcessJob = &FailoverJob{}

// it check range num when  not equal table.replica it will create or delete range to node
type FailoverJob struct {
	service *service.BaseService
}

func (fj *FailoverJob) process(ctx *processContext) {
	if len(ctx.nodeHandlerMap) <= 2 || !fj.service.ConfigFailOver(ctx) {
		return
	}

	if ctx.stop {
		log.Info("got stop so skip FailoverJob")
		return
	}

	log.Info("start FailoverJob begin")
    m := entity.Monitor()
	fj.deleteDownPeer(ctx)
	if ctx.stop {
		return
	}

	//add range if rng.Peers < table.replicaNum or rng.Peers > table.replicaNum
	for _, rng := range ctx.rangeMap {
		table := ctx.tableMap[rng.TableId]
		if table == nil || table.Status != basepb.TableStatus_TableRunning {
			log.Info("table[%v] status is not Not sure")
			ctx.stop = true
			continue
		}
		if len(rng.Peers) < int(table.ReplicaNum) {
			log.Info("db:[%d] table:[%d] replica[ %d / %d ] not enough so create ", rng.DbId, rng.TableId, len(rng.Peers), table.ReplicaNum)
			if err := fj.createRangeToNode(ctx, rng); err != nil {
				log.Info("add range err :[%s]", err.Error())
				if m != nil {
					m.GetGauge(m.GetCluster(), "schedule", "event", "create_range", "fail").Add(1)
				}
			} else {
				ctx.stop = true
				if m != nil {
					m.GetGauge(m.GetCluster(), "schedule", "event", "create_range", "success").Add(1)
				}
			}

		} else if len(rng.Peers) > int(table.ReplicaNum) {
			log.Info("db:[%d] table:[%d] range:[%d] replica[%d/%d] so much so delete ", rng.DbId, rng.TableId, rng.Id, len(rng.Peers), table.ReplicaNum)
			if err := fj.deleteRangeToNode(ctx, rng); err != nil {
				log.Info("delete range err :[%s]", err.Error())
				if m != nil {
					m.GetGauge(m.GetCluster(), "schedule", "event", "delete_range", "fail").Add(1)
				}
			} else {
				ctx.stop = true
				if m != nil {
					m.GetGauge(m.GetCluster(), "schedule", "event", "delete_range", "success").Add(1)
				}
			}
		}
	}
}

func (fj *FailoverJob) deleteDownPeer(ctx *processContext) {
	m := entity.Monitor()
	for _, nh := range ctx.nodeHandlerMap {
		for _, rh := range nh.RangeHanders {
			if !rh.IsLeader {
				continue
			}
			for _, ps := range rh.PeersStatus {
				if ps.DownSeconds > uint64(entity.Conf().Global.PeerDownSecond) {
					log.Warn("to delete node:[%d] range:[%d] peer:[%d] because it DownSeconds:[%ds]", ps.Peer.NodeId, rh.Id, ps.Peer.Id, ps.DownSeconds)
					if err := fj.service.SyncDeleteRangeToNode(ctx, rh.Range, ps.Peer.Id, ps.Peer.NodeId); err != nil {
						log.Error("delete range to node err :[%s]", err.Error())
						if m != nil {
							m.GetGauge(m.GetCluster(), "schedule", "event", "delete_range", "fail").Add(1)
						}
					} else {
						ctx.stop = true
						if m != nil {
							m.GetGauge(m.GetCluster(), "schedule", "event", "delete_range", "success").Add(1)
						}
					}
				}
			}
		}
	}
}

func (fj *FailoverJob) createRangeToNode(ctx *processContext, rng *basepb.Range) error {

	nh, err := ctx.nodeHandlerMap.MinArriveNodeByRange(ctx.tableMap[rng.TableId].Type, rng)
	if err != nil {
		return err
	}

	err = fj.service.CreateRangeToNode(ctx, nh.Node, rng)
	if err == nil {
		nh.RangeNum = nh.RangeNum + 1
	}
	return err
}

//if use this , means the node.Replica > table.Replica
func (fj *FailoverJob) deleteRangeToNode(ctx *processContext, rng *basepb.Range) (err error) {

	nh := ctx.nodeHandlerMap[rng.Leader]
	if nh == nil {
		return fmt.Errorf("leader[%d] not found In nodeMap so skip ", rng.Leader)
	}

	rh := nh.GetRH(rng.Id)

	if rh == nil {
		return fmt.Errorf("range[%d] not found In node so skip ", rng.Id, nh.Id)
	}

	if err := rh.CanDeleteRange(ctx.nodeHandlerMap); err != nil {
		return err
	}

	peer, err := rh.MaxNumberPeer(ctx.nodeHandlerMap)
	if err != nil {
		return err
	}

	if err = fj.service.SyncDeleteRangeToNode(ctx, rng, peer.Id, peer.NodeId); err == nil {
		nh.RangeNum = nh.RangeNum - 1
	}

	return
}
