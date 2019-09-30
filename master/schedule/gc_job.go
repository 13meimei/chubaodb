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
	"github.com/chubaodb/chubaodb/master/entity/errs"
	"github.com/chubaodb/chubaodb/master/entity/pkg/basepb"
	"github.com/chubaodb/chubaodb/master/entity/pkg/dspb"
	"github.com/chubaodb/chubaodb/master/entity/pkg/mspb"
	"github.com/chubaodb/chubaodb/master/service"
	"github.com/chubaodb/chubaodb/master/utils/log"
)

var _ ProcessJob = &GCJob{}

type GCJob struct {
	service *service.BaseService
}

func (gc *GCJob) process(ctx context.Context, stop *bool, nodeMap map[uint64]*basepb.Node, nodeInfoMap map[uint64]*dspb.NodeInfoResponse, nodeRangeNum map[uint64]int, ranges []*basepb.Range, rangeGroup map[[2]uint64][]*basepb.Range, tableMap map[uint64]*basepb.Table, dbMap map[uint64]*basepb.DataBase) {
	if *stop {
		log.Info("got stop so skip GCjob")
		return
	}
	log.Info("start GCjob begin")

	for _, dbNodeInfo := range nodeInfoMap {
		gc.processDbNodeInfo(ctx, stop, dbNodeInfo, rangeGroup, nodeMap, tableMap)
	}

	for dbTableID, ranges := range rangeGroup {
		gc.processRanges(ctx, stop, dbMap[dbTableID[0]], tableMap[dbTableID[1]], ranges)
	}

	for _, table := range tableMap {
		gc.processTable(ctx, dbMap[table.DbId], table)
	}
}

func (gc *GCJob) processRanges(ctx context.Context, stop *bool, db *basepb.DataBase, table *basepb.Table, ranges []*basepb.Range) {
	if table == nil {
		for _, rgn := range ranges {
			log.Info("Could not find Table:[%d] contains range,rangeID:[%d] so remove it from etcd!", rgn.TableId, rgn.Id)
			if err := gc.service.Delete(ctx, entity.RangeKey(rgn.TableId, rgn.Id)); err != nil {
				log.Error("delete range err :[%s]", err.Error())
			} else {
				*stop = true
			}
		}
	}
}

func (gc *GCJob) processTable(ctx context.Context, db *basepb.DataBase, table *basepb.Table) {
	if db == nil {
		log.Info("Could not find db:[%d] contains table:[%d], so remove it from etcd!", table.DbId, table.Id)
		if err := gc.service.Delete(ctx, entity.TableKey(table.DbId, table.Id)); err != nil {
			log.Error("delete table err :[%s]", err.Error())
		}
	}
}

func (gc *GCJob) processDbNodeInfo(ctx context.Context, stop *bool, response *dspb.NodeInfoResponse, rangeGroup map[[2]uint64][]*basepb.Range, nodeMap map[uint64]*basepb.Node, tableMap map[uint64]*basepb.Table) {

	for _, ri := range response.RangeInfos {
		if _, found := tableMap[ri.Range.TableId]; !found {

			//double check table has

			log.Info("double check table[%d] is exists", ri.Range.TableId)
			table, err := gc.service.GetTable(ctx, ri.Range.DbId, "", ri.Range.TableId, "")
			if table != nil || mspb.ErrorType_NotExistTable != errs.Code(err) {
				log.Warn("table[%d] is exists so skip del err:[%v]", ri.Range.TableId, err)
				continue
			}

			for _, p := range ri.Range.Peers {
				node := nodeMap[response.NodeId]
				if node != nil {
					if err := gc.service.DsClient().DeleteRange(ctx, nodeAddr(node), ri.Range.Id, p.Id); err != nil {
						log.Error("delete range err:[%s]", err.Error())
					} else {
						*stop = true
						log.Info("job delete range ok :rangeID:[%d], peerID:[%d]", ri.Range.Id, p.Id)
					}
				}

			}
		} else { //if this range has db and table , check range is out of
			dbRngs := rangeGroup[[2]uint64{ri.Range.DbId, ri.Range.TableId}]

			for _, dbRng := range dbRngs {
				if dbRng.Id == ri.Range.Id {
					if ri.Range.RangeEpoch.Version <= dbRng.RangeEpoch.Version && ri.Range.RangeEpoch.ConfVer < dbRng.RangeEpoch.ConfVer {
						dbPeers := make(map[uint64]*basepb.Peer)
						for _, p := range dbRng.Peers {
							dbPeers[p.Id] = p
						}

						for _, p := range ri.Range.Peers {
							if _, found := dbPeers[p.Id]; !found && p.NodeId == response.NodeId {
								log.Info("find outside range not in range peers node:[%d] peerID:[%d] version:[%v] dbVersion:[%v]", response.NodeId, p.Id, ri.Range.RangeEpoch, dbRng.RangeEpoch)
								if err := gc.service.SyncDeleteRangeToNode(ctx, ri.Range, p.Id, p.NodeId); err != nil {
									log.Error("delete range by node:[%d] peer:[%d] has err:[%s]", p.NodeId, p.Id, err.Error())
								} else {
									*stop = true
									log.Info("delete range by node:[%d] peer:[%d]", p.NodeId, p.Id)
								}
							}
						}
					}
				}
			}

		}
	}

}
