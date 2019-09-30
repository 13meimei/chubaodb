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
	"fmt"
	"github.com/chubaodb/chubaodb/master/entity"
	"github.com/chubaodb/chubaodb/master/entity/errs"
	"github.com/chubaodb/chubaodb/master/entity/pkg/basepb"
	"github.com/chubaodb/chubaodb/master/entity/pkg/dspb"
	"github.com/chubaodb/chubaodb/master/entity/pkg/mspb"
	"github.com/chubaodb/chubaodb/master/service"
	"github.com/chubaodb/chubaodb/master/utils"
	"github.com/chubaodb/chubaodb/master/utils/log"
	"math"
)

var _ ProcessJob = &FailoverJob{}

// it check range num when  not equal table.replica it will create or delete range to node
type FailoverJob struct {
	service *service.BaseService
}

func (gc *FailoverJob) process(ctx context.Context, stop *bool, nodeMap map[uint64]*basepb.Node, nodeInfoMap map[uint64]*dspb.NodeInfoResponse, nodeRangeNum map[uint64]int, ranges []*basepb.Range, rangeGroup map[[2]uint64][]*basepb.Range, tableMap map[uint64]*basepb.Table, dbMap map[uint64]*basepb.DataBase) {
	if len(nodeInfoMap) <= 2 || !gc.service.ConfigFailOver(ctx) {
		return
	}

	if *stop {
		log.Info("got stop so skip FailoverJob")
		return
	}

	log.Info("start FailoverJob begin")

	if gc.deleteDwonPeer(ctx, nodeInfoMap, nodeMap) {
		*stop = true
		return
	}

	//add range if rng.Peers < table.replicaNum or rng.Peers > table.replicaNum
	for dbTableID, rngs := range rangeGroup {
		table := tableMap[dbTableID[1]]
		if table == nil || table.Status != basepb.TableStatus_TableRunning {
			log.Info("table[%v] status is not Not sure")
			*stop = true
			continue
		}
		property, err := utils.ParseTableProperties(table.Properties)
		if err != nil {
			log.Error("parse table properties err:[%s] content:[%s]", err.Error(), table.Properties)
			continue
		}
		for _, rng := range rngs {
			if len(rng.Peers) < property.ReplicaNum {
				log.Info("db:[%d] table:[%d] replica[ %d / %d ] not enough so create ", rng.DbId, rng.TableId, len(rng.Peers), property.ReplicaNum)
				if err := gc.createRangeToNode(ctx, rng, nodeMap, nodeInfoMap, nodeRangeNum); err != nil {
					log.Info("add range err :[%s]", err.Error())
				}
				*stop = true
			} else if len(rng.Peers) > property.ReplicaNum {
				log.Info("db:[%d] table:[%d] replica[%d/%d] so much so delete ", rng.DbId, rng.TableId, len(rng.Peers), property.ReplicaNum)
				if err := gc.deleteRangeToNode(ctx, rng, nodeInfoMap, nodeMap, nodeRangeNum); err != nil {
					log.Info("add range err :[%s]", err.Error())
				} else {
					*stop = true
				}
			}
		}
	}

}

func (gc *FailoverJob) deleteDwonPeer(ctx context.Context, nodeInfoMap map[uint64]*dspb.NodeInfoResponse, nodeMap map[uint64]*basepb.Node) bool {
	flag := false
	for nodeID, info := range nodeInfoMap {
		for _, ri := range info.RangeInfos {

			if ri.Range.Leader != nodeID {
				continue
			}

			for _, ps := range ri.PeersStatus {
				if ps.DownSeconds > uint64(entity.Conf().Global.PeerDownSecond) {
					log.Warn("to delete node:[%d] range:[%d] peer:[%d] because it DownSeconds:[%ds]", ps.Peer.NodeId, ri.Range.Id, ps.Peer.Id, ps.DownSeconds)
					if err := gc.service.SyncDeleteRangeToNode(ctx, ri.Range, ps.Peer.Id, ps.Peer.NodeId); err != nil {
						log.Error("delete range to node err :[%s]", err.Error())
					} else {
						flag = true
					}
				}
			}
		}
	}
	return flag
}

func (gc *FailoverJob) createRangeToNode(ctx context.Context, rng *basepb.Range, nodeMap map[uint64]*basepb.Node, nodeInfoMap map[uint64]*dspb.NodeInfoResponse, nodeRangeNum map[uint64]int) error {
	var minNodeID uint64 = 0
	var nodeInfo *dspb.NodeInfoResponse
	minNodeNum := math.MaxInt32

	for nodeID, num := range nodeRangeNum {
		for _, peer := range rng.Peers {
			if peer.NodeId == nodeID {
				goto next
			}
		}

		//check memory size
		nodeInfo = nodeInfoMap[nodeID]
		if float64(nodeInfo.Stats.UsedSize)/float64(nodeInfo.Stats.Capacity) >= entity.Conf().Global.MemoryRatio {
			log.Error("node:[%d] used memory:[%d] gather config memory_ration[%d] so skip check", nodeID, float64(nodeInfo.Stats.UsedSize)/float64(nodeInfo.Stats.Capacity), entity.Conf().Global.MemoryRatio)
			continue
		}

		if minNodeNum > num {
			minNodeNum = num
			minNodeID = nodeID
		}
	next:
	}

	if minNodeID == 0 {
		return errs.Error(mspb.ErrorType_NodeNotEnough)
	}

	err := gc.service.CreateRangeToNode(ctx, nodeMap[minNodeID], rng)
	if err == nil {
		nodeRangeNum[minNodeID] = minNodeNum + 1
	}
	return err
}

//if use this , means the node.Replica > table.Replica
func (gc *FailoverJob) deleteRangeToNode(ctx context.Context, rng *basepb.Range, nodeInfoMap map[uint64]*dspb.NodeInfoResponse, nodeMap map[uint64]*basepb.Node, nodeRangeNum map[uint64]int) (err error) {

	nodeInfo := nodeInfoMap[rng.Leader]
	if nodeInfo == nil {
		return fmt.Errorf("leader[%d] not found In nodeMap so skip ", rng.Leader)
	}

	for _, ri := range nodeInfo.RangeInfos {
		if ri.Range.Id == rng.Id {
			if ri.Range.Leader != nodeInfo.NodeId {
				return fmt.Errorf("leader has changed rangeLeader:[%d] rangeInfoLeader:[%s] ", rng.Leader, ri.Range.Leader)
			}
			for _, pr := range ri.PeersStatus {
				if pr.Snapshotting || pr.Peer.Type != basepb.PeerType_PeerType_Normal || pr.DownSeconds > 0 {
					return fmt.Errorf("range:[%d] has down or snapshotting peer so skip del ", rng.Id)
				}
			}
		}
	}

	var maxPeer, minPeer *basepb.Peer
	maxNodeNum := -1
	minNodeNum := math.MaxInt32

	for _, peer := range rng.Peers {
		num, found := nodeRangeNum[peer.NodeId]
		if !found {
			return errs.Error(mspb.ErrorType_NodeStateConfused)
		}
		if num > maxNodeNum {
			maxNodeNum = num
			maxPeer = peer
		}

		if num < minNodeNum {
			minNodeNum = num
			minPeer = peer
		}

	}

	if maxPeer == nil || minPeer == nil || maxPeer.NodeId == minPeer.NodeId {
		log.Error("maxPeer:[%v] minPeer:[%v] , Impossible err", maxPeer, minPeer)
		return errs.Error(mspb.ErrorType_NodeNotEnough) // Impossible
	}

	if err = gc.service.SyncDeleteRangeToNode(ctx, rng, maxPeer.Id, maxPeer.NodeId); err == nil {
		nodeRangeNum[maxPeer.NodeId] = maxNodeNum - 1
	}

	return
}
