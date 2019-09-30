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
	"github.com/chubaodb/chubaodb/master/entity"
	"github.com/chubaodb/chubaodb/master/entity/pkg/basepb"
	"github.com/chubaodb/chubaodb/master/entity/pkg/dspb"
	"github.com/chubaodb/chubaodb/master/service"
	"github.com/chubaodb/chubaodb/master/utils/log"
	"math"
	"math/rand"
	"sort"
	"time"
)

var _ ProcessJob = &BalanceJob{}

// check master distribution
// check range num in diff node is balanced , one job only move one range
type BalanceJob struct {
	service *service.BaseService
}

func (gc *BalanceJob) process(ctx context.Context, stop *bool, nodeMap map[uint64]*basepb.Node, nodeInfoMap map[uint64]*dspb.NodeInfoResponse, nodeRangeNum map[uint64]int, ranges []*basepb.Range, rangeGroup map[[2]uint64][]*basepb.Range, tableMap map[uint64]*basepb.Table, dbMap map[uint64]*basepb.DataBase) {
	if len(nodeInfoMap) <= 2 || !gc.service.ConfigBalanced(ctx) {
		return
	}
	if *stop {
		log.Info("got stop so skip BalanceJob")
		return
	}

	log.Info("start BalanceJob begin")

	kvList := sortToList(nodeRangeNum)

	gc.balanceMaster(ctx, stop, nodeMap, nodeInfoMap)

	if kvList[len(kvList)-1].num-kvList[0].num >= 3 {
		log.Info("start balance range begin")
		gc.balanceRange(ctx, stop, kvList, nodeRangeNum, nodeMap, nodeInfoMap)
		*stop = true
	}

}

func (gc *BalanceJob) balanceMaster(ctx context.Context, stop *bool, nodeMap map[uint64]*basepb.Node, nodeInfoMap map[uint64]*dspb.NodeInfoResponse) {
	nodeMasterNum := gc.groupNodeInfoMaster(nodeInfoMap)

	allMaster := 0
	for _, v := range nodeMasterNum {
		allMaster += v
	}

	kvList := sortToList(nodeMasterNum)

	maxNode := kvList[len(kvList)-1]

	for _, kv := range kvList {
		if maxNode.num-kv.num >= 3 {
			midNum := float64((maxNode.num - kv.num) / 2)
			average := float64(allMaster/len(nodeMasterNum) - kv.num)
			log.Info("start balance leader begin")
			if gc.transferLeader(ctx, int(math.Max(math.Min(midNum, average), 1)), maxNode.nodeID, kv.nodeID, nodeMap, nodeInfoMap) {
				*stop = true
				return
			}

		}
	}
}

func (bj *BalanceJob) transferLeader(ctx context.Context, num int, from uint64, to uint64, nodeMap map[uint64]*basepb.Node, nodeInfoMap map[uint64]*dspb.NodeInfoResponse) bool {

	log.Info("try transferLeader num:[%d]", num)

	fromInfo := nodeInfoMap[from]
	toInfo := nodeInfoMap[to]

	rand.Shuffle(len(fromInfo.RangeInfos), func(i, j int) {
		fromInfo.RangeInfos[i], fromInfo.RangeInfos[j] = fromInfo.RangeInfos[j], fromInfo.RangeInfos[i]
	})

	for _, ri := range fromInfo.RangeInfos {
		if ri.Range.Leader == fromInfo.NodeId {

			//if has not health peer not transfer leader
			for _, prs := range ri.PeersStatus {
				if prs.Snapshotting || prs.Peer.Type != basepb.PeerType_PeerType_Normal || prs.DownSeconds > 0 {
					log.Warn("node:[%d] range:[%d] peer:[%d] not health so skip this balance range", prs.Peer.NodeId, ri.Range.Id, prs.Peer.Id)
					goto next
				}
			}

			for _, tR := range toInfo.RangeInfos {
				if tR.Range.Id == ri.Range.Id && tR.Range.Leader != toInfo.NodeId {
					if err := bj.service.TransferLeader(ctx, nodeMap[toInfo.NodeId], tR.Range); err != nil {
						log.Error("leader balance to node:[%d] rangeID:[%d] has err:[%s] ", toInfo.NodeId, tR.Range.Id, err.Error())
					} else {
						log.Info("leader balance to node:[%d] rangeID:[%d] ok need num:[%d]", toInfo.NodeId, tR.Range.Id, num)
						num--
						if num == 0 {
							return true
						}
					}
					break
				}
			}
		next:
		}
	}

	return false
}

func (gc *BalanceJob) balanceRange(ctx context.Context, stop *bool, kvList []*kvStruct, nodeRangeNum map[uint64]int, nodeMap map[uint64]*basepb.Node, nodeInfoMap map[uint64]*dspb.NodeInfoResponse) {

	//has snapshot
	for _, ni := range nodeInfoMap {
		for _, ri := range ni.RangeInfos {
			for _, prs := range ri.PeersStatus {
				if prs.Snapshotting || prs.Peer.Type != basepb.PeerType_PeerType_Normal {
					log.Warn("node:[%d] range:[%d] peer:[%d] has snapshot so skip this balance range", prs.Peer.NodeId, ri.Range.Id, prs.Peer.Id)
					return
				}
			}
		}
	}

	allCount := 0
	for _, v := range kvList {
		allCount += v.num
	}

	average := math.Max(float64(allCount/len(kvList)), 1)

	from := kvList[len(kvList)-1]

	fromInfo := nodeInfoMap[from.nodeID]

	count := 0

	to := kvList[0]
	toInfo := nodeInfoMap[to.nodeID]
	//check memory size
	if float64(toInfo.Stats.UsedSize)/float64(toInfo.Stats.Capacity) >= entity.Conf().Global.MemoryRatio {
		log.Error("node:[%d] used memory:[%d] gather config memory_ration[%d] so skip check", to.nodeID, float64(toInfo.Stats.UsedSize)/float64(toInfo.Stats.Capacity), entity.Conf().Global.MemoryRatio)
		return
	}

	if to.num >= int(average) {
		log.Info("the nodeNum:[%d] is average:[%d] so skip blance ", to.num, average)
		return
	}

	//shuffle because master is often continuous
	rand.Shuffle(len(fromInfo.RangeInfos), func(i, j int) {
		fromInfo.RangeInfos[i], fromInfo.RangeInfos[j] = fromInfo.RangeInfos[j], fromInfo.RangeInfos[i]
	})

	for _, fromRangeInfo := range fromInfo.RangeInfos {

		for _, toRangeInfo := range toInfo.RangeInfos {
			if fromRangeInfo.Range.Id == toRangeInfo.Range.Id {
				goto skip
			}
		}

		if err := gc.service.CreateRangeToNode(ctx, nodeMap[to.nodeID], fromRangeInfo.Range); err != nil {
			log.Error("balance range to node err:[%s]", err.Error())
		} else {
			count++
			from.num--
			to.num++
			*stop = true
			log.Info("create range to node:[%d] range:[%d]", to.nodeID, fromRangeInfo.Range.Id)
		}

		if from.num-to.num <= 3 || to.num >= int(average) {
			return
		}

		for count >= 5 { //wait for over got to next

			time.Sleep(5 * time.Second)
			select {
			case <-ctx.Done():
				return
			default:

			}

			snapNum := 0
			for nodeID, _ := range nodeMap {
				response, err := gc.service.NodeInfo(ctx, nodeMap[nodeID])
				if err != nil {
					log.Error("get node:[%d] info err:[%s]", nodeID, err.Error())
					return
				}
				snapNum := 0
				for _, ri := range response.RangeInfos {
					for _, prs := range ri.PeersStatus {
						if prs.Snapshotting || prs.Peer.Type != basepb.PeerType_PeerType_Normal {
							snapNum++
						}
					}
				}
			}

			log.Info("balance range not ok now wait snapshot num:[%d] to next step", snapNum)
			count = snapNum

		}

	skip:
	}
}

func sortToList(nodeRangeNum map[uint64]int) []*kvStruct {
	kvList := make([]*kvStruct, 0, len(nodeRangeNum))
	for k, v := range nodeRangeNum {
		kvList = append(kvList, &kvStruct{nodeID: k, num: v})
	}

	sort.Slice(kvList, func(i, j int) bool {
		return kvList[i].num < kvList[j].num
	})
	return kvList
}

//it return num of not master
func (gc *BalanceJob) groupNodeInfoMaster(nodeInfoMap map[uint64]*dspb.NodeInfoResponse) map[uint64]int {
	nodeMasterNum := make(map[uint64]int)

	for nodeID, _ := range nodeInfoMap {
		nodeMasterNum[nodeID] = 0
	}
	for nodeID, nodeInfo := range nodeInfoMap {
		for _, ri := range nodeInfo.RangeInfos {
			if ri.Range.Leader == nodeID {
				nodeMasterNum[nodeID] = nodeMasterNum[nodeID] + 1
			}
		}
	}
	return nodeMasterNum
}

type kvStruct struct {
	nodeID uint64
	num    int
}
