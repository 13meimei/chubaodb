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

package service

import (
	"bytes"
	"context"
	"fmt"
	client "github.com/chubaodb/chubaodb/master/client/ds_client"
	"github.com/chubaodb/chubaodb/master/entity"
	"github.com/chubaodb/chubaodb/master/entity/errs"
	"github.com/chubaodb/chubaodb/master/entity/pkg/basepb"
	"github.com/chubaodb/chubaodb/master/entity/pkg/dspb"
	"github.com/chubaodb/chubaodb/master/entity/pkg/mspb"
	"github.com/chubaodb/chubaodb/master/utils"
	unsafeBytes "github.com/chubaodb/chubaodb/master/utils/bytes"
	"github.com/chubaodb/chubaodb/master/utils/cblog"
	"github.com/chubaodb/chubaodb/master/utils/log"
	"go.etcd.io/etcd/clientv3/concurrency"
	"math"
	"math/rand"
	"sort"
	"time"
)

// return true for update , false is not write
func (rs *BaseService) Heartbeat(ctx context.Context, rg *basepb.Range) (bool, error) {

	var flag bool

	err := rs.STM(ctx, func(stm concurrency.STM) error {

		dbRange := new(basepb.Range)

		realBytes := stm.Get(entity.RangeKey(rg.TableId, rg.Id))

		if len(realBytes) > 0 {
			if err := dbRange.Unmarshal([]byte(realBytes)); err != nil {
				return cblog.LogErrAndReturn(err)
			}

			if dbRange.RangeEpoch != nil && (dbRange.RangeEpoch.ConfVer > rg.RangeEpoch.ConfVer || dbRange.RangeEpoch.Version > rg.RangeEpoch.Version) {
				flag = false
				return nil
			}
		}

		rgBytes, err := rg.Marshal()

		if err != nil {
			return cblog.LogErrAndReturn(err)
		}

		stm.Put(entity.RangeKey(rg.TableId, rg.Id), unsafeBytes.ByteToString(rgBytes))

		flag = true

		return nil
	})

	return flag, cblog.LogErrAndReturn(err)
}

func (rs *BaseService) AskSplit(ctx context.Context, rng *basepb.Range, force bool) (uint64, []uint64, error) {

	if !rs.ConfigAutoSplit(ctx) {
		log.Warn("auto split is close , so return err:[%s]", errs.Error(mspb.ErrorType_NotAllowSplit))
		return 0, nil, cblog.LogErrAndReturn(errs.Error(mspb.ErrorType_NotAllowSplit))
	}

	table, err := rs.QueryTableByID(ctx, rng.DbId, rng.TableId)
	if err != nil {
		log.Warn("dbID:[%d] tableID:[%d] range:[%d] skip err:[%s]", rng.DbId, rng.TableId, rng.Id, err.Error())
		return 0, nil, cblog.LogErrAndReturn(err)
	}

	// if table status not runing so skip split
	if table.Status != basepb.TableStatus_TableRunning {
		log.Warn("table:[%s/%d] status:[%d] so skip split", table.Name, table.Id, table.Status)
		return 0, nil, cblog.LogErrAndReturn(errs.Error(mspb.ErrorType_NotAllowSplit))
	}

	oldRange, err := rs.QueryRange(ctx, rng.TableId, rng.Id)
	if err != nil {
		log.Warn("dbID:[%d] tableID:[%d] range:[%d] skip err:[%s]", rng.DbId, rng.TableId, rng.Id, err.Error())
		return 0, nil, cblog.LogErrAndReturn(err)
	}

	if rng.GetRangeEpoch().GetConfVer() == oldRange.GetRangeEpoch().GetConfVer() && rng.GetRangeEpoch().GetVersion() == oldRange.GetRangeEpoch().GetVersion() {
		if bytes.Compare(rng.GetStartKey(), oldRange.GetStartKey()) != 0 || bytes.Compare(rng.GetEndKey(), oldRange.GetEndKey()) != 0 {
			err = errs.Error(mspb.ErrorType_RangeMetaConflict)
			log.Error("range[%v] meta abnormal[%v] ", rng, oldRange)
			return 0, nil, cblog.LogErrAndReturn(err)
		}
	}

	property, err := utils.ParseTableProperties(table.Properties)
	if err != nil {
		return 0, nil, cblog.LogErrAndReturn(fmt.Errorf("parse table properties err:[%s] content:[%s]", err.Error(), table.Properties))

	}

	if len(rng.Peers) > property.ReplicaNum {
		return 0, nil, cblog.LogErrAndReturn(fmt.Errorf("peer:[%d] gather than replica:[%d]", len(rng.Peers), property.ReplicaNum))
	}

	newRangeID, err := rs.NewIDGenerate(ctx, entity.SequenceRangeID, 1, 5*time.Second)
	if err != nil {
		return 0, nil, cblog.LogErrAndReturn(err)
	}

	newPeerIDs := make([]uint64, len(rng.Peers))
	for i := range newPeerIDs {
		peerID, err := rs.NewIDGenerate(ctx, entity.SequencePeerID, 1, 5*time.Second)
		if err != nil {
			return 0, nil, cblog.LogErrAndReturn(err)
		}
		newPeerIDs[i] = uint64(peerID)
	}

	log.Info("ask split return  rangeID:[%d] peerIDs:[%v]", newRangeID, newPeerIDs)

	return uint64(newRangeID), newPeerIDs, nil
}

// if tableID ==0 it will return all ranges
func (rs *BaseService) GetRoute(ctx context.Context, max int, dbID, tableID uint64, key []byte) ([]*basepb.Range, error) {

	if dbID > 0 { //check db exist
		if _, err := rs.QueryDBByID(ctx, dbID); err != nil {
			return nil, err
		}
	}

	if tableID > 0 { //check table exist
		table, err := rs.QueryTableByID(ctx, dbID, tableID)
		if err != nil {
			return nil, err
		}
		if table.Status != basepb.TableStatus_TableRunning {
			return nil, errs.Error(mspb.ErrorType_NotExistTable)
		}
	}

	ranges, err := rs.QueryRanges(ctx, tableID)

	if err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}

	sort.Slice(ranges, func(i, j int) bool {
		return bytes.Compare(ranges[i].StartKey, ranges[j].StartKey) >= 0
	})

	start := -1

	for i, rng := range ranges {
		if len(rng.Peers) == 0 { //impossible to happen
			return nil, errs.Error(mspb.ErrorType_NotExistRange)
		}
		if len(key) == 0 || bytes.Compare(key, rng.StartKey) >= 0 {
			start = i
			break
		}
	}

	if start < 0 {
		return []*basepb.Range{}, nil
	}

	if max <= 0 {
		return ranges[start:], nil
	}

	return ranges[start:int(math.Min(float64(len(ranges)), float64(start+max)))], nil
}

func (ns *BaseService) CreateRangeToNode(ctx context.Context, node *basepb.Node, old *basepb.Range) error {
	log.Info("create range to node [%d] range:[%d]", node.Id, old.Id)

	rng, err := ns.QueryRange(ctx, old.TableId, old.Id)
	if err != nil {
		return err
	}

	if rng.RangeEpoch.Version != old.RangeEpoch.Version || rng.RangeEpoch.ConfVer != old.RangeEpoch.ConfVer {
		return cblog.LogErrAndReturn(fmt.Errorf("create range has err the range:[%d] version[%d, %d] old:[%d, %d] not same so skip ", rng.Id, rng.RangeEpoch.Version, rng.RangeEpoch.ConfVer, old.RangeEpoch.Version, old.RangeEpoch.ConfVer))
	}

	for _, peer := range rng.Peers {
		if peer.NodeId == node.Id {
			return nil
		}
	}

	leader, err := ns.GetNode(ctx, rng.Leader)
	if err != nil {
		return err
	}

	peerID, err := ns.NewIDGenerate(ctx, entity.SequencePeerID, 1, 5*time.Second)
	if err != nil {
		return err
	}

	newPeer := &basepb.Peer{
		Id:       uint64(peerID),
		NodeId:   node.Id,
		RaftAddr: nodeRaftAddr(node),
		Type:     basepb.PeerType_PeerType_Learner,
	}

	tryTime := 0

	log.Info("change member by range:[%d] newPeer:[%d]", rng.Id, newPeer.Id)
	err = ns.dsClient.ChangeMember(ctx, nodeServerAddr(leader), &dspb.ChangeRaftMemberRequest{
		RangeId:    rng.Id,
		RangeEpoch: rng.RangeEpoch,
		ChangeType: dspb.ChangeRaftMemberRequest_CT_ADD,
		TargetPeer: newPeer,
	})

	if err != nil {
		log.Error("add memeber:[%d] err:", newPeer)
		return err
	}

	for !hasPeer(rng, newPeer.Id) {

		tryTime++
		if tryTime > 60 {
			return fmt.Errorf("create memeber too long times ")
		}
		time.Sleep(500 * time.Millisecond)
		if rng, err = ns.QueryRange(ctx, old.TableId, old.Id); err != nil {
			return err
		}
	}

	return ns.dsClient.CreateRange(ctx, nodeServerAddr(node), rng)
}

func (ns *BaseService) SyncDelMemeber(ctx context.Context, old *basepb.Range, peerID uint64) error {
	log.Info("del member to rangeID:[%d] peerID:[%d]", old.Id, peerID)

	rng, err := ns.QueryRange(ctx, old.TableId, old.Id)
	if err != nil {
		return cblog.LogErrAndReturn(err)
	}

	if rng.RangeEpoch.Version < old.RangeEpoch.Version || rng.RangeEpoch.ConfVer < old.RangeEpoch.ConfVer {
		return cblog.LogErrAndReturn(fmt.Errorf("del range has err the version[%d, %d] old:[%d, %d] not same so skip ", rng.RangeEpoch.Version, rng.RangeEpoch.ConfVer, old.RangeEpoch.Version, old.RangeEpoch.ConfVer))
	}

	//remove peers to all
	newPeers := make([]*basepb.Peer, 0, len(rng.Peers)-1)
	var removePeer *basepb.Peer
	for _, p := range rng.Peers {
		if p.Id != peerID {
			newPeers = append(newPeers, p)
		} else {
			removePeer = p
		}
	}

	if removePeer == nil {
		log.Info("peer[%d] not in range peers:[%v] so skip ", peerID, rng.Peers)
		return nil
	}

	//check changeMeber peer is leader?
	tryTime := 0

	wantLeaderNode, err := ns.QueryNode(ctx, newPeers[rand.Intn(len(newPeers))].NodeId)
	if err != nil {
		log.Error("query node has err :[%s]", err.Error())
		return err
	}
	for removePeer.NodeId == rng.Leader {
		log.Info("remove peer is leader so to transfer leader")

		if err = ns.TransferLeader(ctx, wantLeaderNode, rng); err != nil {
			log.Error("transfer leader has err :[%s]", err.Error())
			return err
		}
		tryTime++
		if tryTime > 60 {
			return fmt.Errorf("transferLeader too long times ")
		}
		time.Sleep(1 * time.Second)
		if rng, err = ns.QueryRange(ctx, old.TableId, old.Id); err != nil {
			return err
		}
	}

	tryTime = 0
	for hasPeer(rng, removePeer.Id) {
		log.Info("change member by range:[%d] removePeer:[%d]", rng.Id, removePeer.Id)
		leader, err := ns.GetNode(ctx, rng.Leader)
		if err != nil {
			return cblog.LogErrAndReturn(err)
		}
		err = ns.dsClient.ChangeMember(ctx, nodeServerAddr(leader), &dspb.ChangeRaftMemberRequest{
			RangeId:    rng.Id,
			RangeEpoch: rng.RangeEpoch,
			ChangeType: dspb.ChangeRaftMemberRequest_CT_REMOVE,
			TargetPeer: removePeer,
		})

		if err != nil {
			return cblog.LogErrAndReturn(err)
		}
		tryTime++
		if tryTime > 60 {
			return fmt.Errorf("del memeber too long times ")
		}
		time.Sleep(500 * time.Millisecond)
		if rng, err = ns.QueryRange(ctx, old.TableId, old.Id); err != nil {
			return err
		}
	}

	return nil
}

func hasPeer(rng *basepb.Range, removePeer uint64) bool {
	for _, p := range rng.Peers {
		if p.Id == removePeer {
			return true
		}
	}
	return false
}

func (ns *BaseService) SyncDeleteRangeToNode(ctx context.Context, old *basepb.Range, peerID uint64, removeNodeID uint64) error {
	log.Info("delete range to rangeID:[%d] peerID:[%d]", old.Id, peerID)

	rng, err := ns.QueryRange(ctx, old.TableId, old.Id)
	if err != nil {
		return cblog.LogErrAndReturn(err)
	}

	if rng.RangeEpoch.Version < old.RangeEpoch.Version || rng.RangeEpoch.ConfVer < old.RangeEpoch.ConfVer {
		return cblog.LogErrAndReturn(fmt.Errorf("del range has err the version[%d, %d] old:[%d, %d] not same so skip ", rng.RangeEpoch.Version, rng.RangeEpoch.ConfVer, old.RangeEpoch.Version, old.RangeEpoch.ConfVer))
	}

	if err := ns.SyncDelMemeber(ctx, old, peerID); err != nil {
		return cblog.LogErrAndReturn(err)
	}

	if removeNode, err := ns.QueryNode(ctx, removeNodeID); err != nil {
		log.Error("query node has err :[%s]", err.Error())
	} else {
		if err := ns.dsClient.DeleteRange(ctx, nodeServerAddr(removeNode), rng.Id, peerID); err != nil {
			log.Warn("delete range by node:[%d] err :[%s]", removeNode.Id, err.Error())
			return err
		} else {
			log.Info("delete range by node:[%d] peerID:[%d] ok", removeNode.Id, peerID)
		}
	}

	return nil

}

func (rs *BaseService) TransferLeader(ctx context.Context, node *basepb.Node, old *basepb.Range) error {
	log.Info("transfer leader for rangeID:[%d] to node:[%d]", old.Id, node.Id)

	table, err := rs.QueryTableByID(ctx, old.DbId, old.TableId)

	rng, err := rs.QueryRange(ctx, old.TableId, old.Id)
	if err != nil {
		return cblog.LogErrAndReturn(err)
	}

	property, err := utils.ParseTableProperties(table.Properties)
	if err != nil {
		return cblog.LogErrAndReturn(fmt.Errorf("parse table properties err:[%s] content:[%s]", err.Error(), table.Properties))

	}

	if len(rng.Peers) < property.ReplicaNum {
		return cblog.LogErrAndReturn(fmt.Errorf("range:[%d] transferLeader check err replica[%d / %d] num not same ", rng.Id, len(rng.Peers), property.ReplicaNum))
	}

	if rng.RangeEpoch.Version < old.RangeEpoch.Version || rng.RangeEpoch.ConfVer < old.RangeEpoch.ConfVer {
		return cblog.LogErrAndReturn(fmt.Errorf("transfer range has err the version[%d, %d] old:[%d, %d] not same so skip ", rng.RangeEpoch.Version, rng.RangeEpoch.ConfVer, old.RangeEpoch.Version, old.RangeEpoch.ConfVer))
	}

	leader, err := rs.GetNode(ctx, old.Leader)
	if err != nil {
		return cblog.LogErrAndReturn(err)
	}
	nodeInfo, err := rs.NodeInfo(ctx, leader)
	if err != nil {
		return cblog.LogErrAndReturn(err)
	}

	for _, ri := range nodeInfo.RangeInfos {
		if ri.Range.Id == old.Id {
			for _, ps := range ri.PeersStatus {
				if ps.Snapshotting || ps.DownSeconds > 0 || ps.Peer.Type != basepb.PeerType_PeerType_Normal {
					return cblog.LogErrAndReturn(fmt.Errorf("%s because it snapshotting", errs.Error(mspb.ErrorType_NotAllowSplit).Error()))
				}
			}
		}
	}

	return rs.dsClient.TransferLeader(ctx, nodeServerAddr(node), old.Id)
}

func (ns *BaseService) DsClient() client.SchClient {
	return ns.dsClient
}
