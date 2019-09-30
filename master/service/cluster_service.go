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
	"context"
	"fmt"
	"github.com/chubaodb/chubaodb/master/entity"
	"github.com/chubaodb/chubaodb/master/entity/errs"
	"github.com/chubaodb/chubaodb/master/entity/pkg/basepb"
	"github.com/chubaodb/chubaodb/master/entity/pkg/mspb"
	"github.com/chubaodb/chubaodb/master/utils"
	"github.com/chubaodb/chubaodb/master/utils/cblog"
	"github.com/chubaodb/chubaodb/master/utils/hack"
	"github.com/chubaodb/chubaodb/master/utils/log"
	"github.com/spf13/cast"
	"go.etcd.io/etcd/clientv3/concurrency"
	"runtime/debug"
	"sort"
	"time"
)

func (cs *BaseService) GetTable(ctx context.Context, dbID uint64, dbName string, tableID uint64, tableName string) (*basepb.Table, error) {
	var (
		db  *basepb.DataBase
		err error
	)

	if dbID > 0 {
		db, err = cs.QueryDBByID(ctx, dbID)
	} else if dbName != "" {
		db, err = cs.QueryDBByName(ctx, dbName)
	} else {
		err = errs.Error(mspb.ErrorType_InvalidParam)
	}
	if err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}

	if tableID > 0 {
		return cs.QueryTableByID(ctx, db.Id, tableID)
	} else if tableName != "" {
		return cs.QueryTableByName(ctx, db.Id, tableName)
	} else {
		return nil, errs.Error(mspb.ErrorType_InvalidParam)
	}
}

func (cs *BaseService) DelTable(ctx context.Context, dbID uint64, dbName string, tableID uint64, tableName string) (*basepb.Table, error) {

	table, err := cs.GetTable(ctx, dbID, dbName, tableID, tableName)
	if err != nil {
		return nil, err
	}

	//to lock table
	lock := cs.NewLock(ctx, entity.LockTableKey(table.DbId, table.Id), time.Minute*5)
	if err := lock.Lock(); err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}
	defer func() {
		if err := lock.Unlock(); err != nil {
			log.Error("unlock table:[%s/%d] err:[%s] ", table.Id, table.Name, err)
		}
	}()

	table, err = cs.GetTable(ctx, dbID, dbName, tableID, tableName)
	if err != nil {
		return nil, err
	}

	if err = cs.Delete(ctx, entity.TableKey(table.DbId, table.Id)); err != nil {
		return nil, err
	}

	// delete all ranges
	go func() {
		background, cancel := context.WithTimeout(context.Background(), time.Minute*5)
		defer cancel()
		log.Info("to delete all ranges in background")
		ranges, err := cs.QueryRanges(background, table.Id)
		if err != nil {
			log.Error("del table to query ranges err:[%s]", err.Error())
			return
		}
		for _, rng := range ranges {

			for _, pr := range rng.Peers {
				log.Info("delete peer:[%d] in node:[%d]", pr.Id, pr.NodeId)
				if node, err := cs.GetNode(background, pr.NodeId); err != nil {
					log.Error("get node err:[%s]", err.Error())
				} else {
					if err := cs.dsClient.DeleteRange(background, nodeServerAddr(node), rng.Id, pr.Id); err != nil {
						log.Error("delete range to node err:[%s]", err.Error())
					}
				}

			}
		}
	}()

	return table, nil
}

func (cs *BaseService) GetTables(ctx context.Context, dbID uint64, dbName string) ([]*basepb.Table, error) {
	var (
		db  *basepb.DataBase
		err error
	)

	if dbID > 0 {
		db, err = cs.QueryDBByID(ctx, dbID)
	} else if dbName != "" {
		db, err = cs.QueryDBByName(ctx, dbName)
	} else {
		err = errs.Error(mspb.ErrorType_InvalidParam)
	}

	if err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}

	if db == nil {
		return nil, errs.Error(mspb.ErrorType_NotExistDatabase)
	}

	return cs.QueryTablesByDBID(ctx, db.Id)
}

func (cs *BaseService) GetColumns(ctx context.Context, dbID, tableID uint64) ([]*basepb.Column, error) {
	table, err := cs.QueryTableByID(ctx, dbID, tableID)
	if err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}
	return table.Columns, nil
}

func (cs *BaseService) GetColumn(ctx context.Context, dbID, tableID, columnID uint64, columnName string) (*basepb.Column, error) {
	columns, err := cs.GetColumns(ctx, dbID, tableID)
	if err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}
	for _, c := range columns {
		if c.Id == columnID {
			return c, nil
		} else if c.Name == columnName {
			return c, nil
		}
	}
	return nil, errs.Error(mspb.ErrorType_ColumnNotExist)
}

//create database use dbName
func (cs *BaseService) CreateDatabase(ctx context.Context, dbName string) (*basepb.DataBase, error) {
	//validate name has in db is in return err
	if err := utils.ValidateName(dbName); err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}

	if value, err := cs.Get(ctx, entity.DBKeyName(dbName)); err != nil {
		return nil, cblog.LogErrAndReturn(err)
	} else if value != nil {
		return nil, fmt.Errorf("dbname:[%s] already exists", dbName)
	}

	//find db id
	dbId, err := cs.NewIDGenerate(ctx, entity.SequenceDBID, 1, 5*time.Second)
	if err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}

	db := &basepb.DataBase{
		Name:       dbName,
		Id:         uint64(dbId),
		Version:    1,
		CreateTime: time.Now().Unix(),
	}

	err = cs.STM(context.Background(), func(stm concurrency.STM) error {
		dbKey, nameKey := entity.DBKeys(db.Id, db.Name)

		if stm.Get(nameKey) != "" {
			return fmt.Errorf("dbID %d is exists", db.Id)
		}

		if stm.Get(dbKey) != "" {
			return fmt.Errorf("dbname %s is exists", db.Name)
		}

		value, err := db.Marshal()
		if err != nil {
			return err
		}

		stm.Put(nameKey, cast.ToString(db.Id))
		stm.Put(dbKey, string(value))
		log.Info("create database[%s] success", dbName)
		return nil
	})

	if err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}

	return db, nil
}

func (cs *BaseService) DeleteDatabase(ctx context.Context, dbID uint64, dbName string) error {

	//validate name has in db is in return err

	dataBase := &basepb.DataBase{}

	if dbName != "" {
		if value, err := cs.Get(ctx, entity.DBKeyName(dbName)); err != nil {
			return cblog.LogErrAndReturn(err)
		} else if value == nil {
			return fmt.Errorf("dbname:[%s] not found", dbName)
		} else {
			id := cast.ToUint64(string(value))
			if dbID > 0 && id != dbID {
				return fmt.Errorf("delete database param err name and id not same :name:[%s] id:[%d]", dbName, dbID)
			}
			dbID = id
		}
	}

	if value, err := cs.Get(ctx, entity.DBKey(dbID)); err != nil {
		return cblog.LogErrAndReturn(err)
	} else if value == nil {
		return fmt.Errorf("dbID:[%d] not found", dbID)
	} else {
		if err := dataBase.Unmarshal(value); err != nil {
			return cblog.LogErrAndReturn(err)
		}
	}

	//find db has range
	lock := cs.NewLock(ctx, entity.LockDBKey(dataBase.Id), time.Minute*5)
	if err := lock.Lock(); err != nil {
		return cblog.LogErrAndReturn(err)
	}
	defer func() {
		if err := lock.Unlock(); err != nil {
			log.Error("unlock db:[%s/%d] err:[%s] ", dataBase.Id, dataBase.Name, err)
		}
	}()

	tables, err := cs.QueryTablesByDBID(ctx, dataBase.Id)
	if err != nil {
		return cblog.LogErrAndReturn(err)
	}

	if len(tables) > 0 {
		return errs.Error(mspb.ErrorType_DatabaseNotEmpty)
	}

	err = cs.STM(context.Background(), func(stm concurrency.STM) error {
		dbKey, nameKey := entity.DBKeys(dataBase.Id, dataBase.Name)
		stm.Del(dbKey)
		stm.Del(nameKey)
		log.Info("delete database[%s] success", dbName)
		return nil
	})

	return err
}

//  to create table by dbName , when create it will lock by dbID , so you can not to create another table in same database
// if return table is nil , means it record the err
func (cs *BaseService) CreateTable(ctx context.Context, dbName, tableName, properties string, rangeKeys []string) (*basepb.Table, error) {

	tProperty, err := utils.ParseTableProperties(properties)
	if err != nil {
		log.Error("parse cols[%s] failed, err[%v]", properties, err)
		return nil, cblog.LogErrAndReturn(err)
	}

	dataBase, err := cs.QueryDBByName(ctx, dbName)
	if err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}

	//to lock cluster
	lock := cs.NewLock(ctx, entity.LockCluster(), time.Minute*5)
	if err := lock.Lock(); err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}
	defer func() {
		if err := lock.Unlock(); err != nil {
			log.Error("unlock db:[%s/%d] err:[%s] ", dataBase.Id, dataBase.Name, err)
		}
	}()

	if table, err := cs.QueryTableByName(ctx, dataBase.Id, tableName); err != nil && err != errs.Error(mspb.ErrorType_NotExistTable) {
		return nil, cblog.LogErrAndReturn(err)
	} else if table != nil {
		return nil, errs.Error(mspb.ErrorType_DupTable)
	}

	// create table begin

	tableID, err := cs.NewIDGenerate(ctx, entity.SequenceTableID, 1, 5*time.Second)
	if err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}

	for _, secInd := range tProperty.Indexes {
		indexID, err := cs.NewIDGenerate(ctx, entity.SequenceIndexID, 1, 5*time.Second)
		if err != nil {
			return nil, cblog.LogErrAndReturn(err)
		}
		secInd.Id = uint64(indexID)
	}

	t := &basepb.Table{
		Id:         uint64(tableID),
		Name:       tableName,
		DbName:     dbName,
		DbId:       dataBase.GetId(),
		Columns:    tProperty.Columns,
		Epoch:      &basepb.TableEpoch{ConfVer: uint64(1), Version: uint64(1)},
		CreateTime: time.Now().Unix(),
		Indexes:    tProperty.Indexes,
		Properties: properties,
	}

	sharingKeys, err := utils.MakeSharingKeys(uint64(tableID), rangeKeys, tProperty.Columns)
	if err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}

	//get all nodes
	allNodes, err := cs.QueryOnlineNodes(ctx)
	if err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}

	//filter nodes
	nodes := make([]*basepb.Node, 0, len(allNodes))
	for _, node := range allNodes {
		if cs.dsClient.IsAlive(ctx, nodeServerAddr(node)) {
			nodes = append(nodes, node)
		}
	}

	if len(nodes) < tProperty.ReplicaNum {
		log.Error("not enough nodes:[%d], need replica:[%d]", len(nodes), tProperty.ReplicaNum)
		return nil, errs.Error(mspb.ErrorType_NodeNotEnough)
	}

	//save table to etcd
	if tableBytes, err := t.Marshal(); err != nil {
		return nil, cblog.LogErrAndReturn(err)
	} else {
		if err = cs.Update(ctx, entity.TableKey(dataBase.Id, t.Id), tableBytes); err != nil {
			return nil, cblog.LogErrAndReturn(err)
		}
	}

	// if has err so delete table
	defer func() {
		if r := recover(); r != nil {
			debug.PrintStack()
			log.Error("create table err: %v ", r)
		}

		if t.Status != basepb.TableStatus_TableRunning { // if space is still not enabled , so to remove it
			log.Error("table status not running so remove ")
			if e := cs.Delete(context.Background(), entity.TableKey(dataBase.Id, t.Id)); e != nil {
				log.Error("to delete table err :%s", e.Error())
			}
		}
	}()

	var errChain = make(chan error, 1)
	var ranges = make([]*basepb.Range, 0, len(sharingKeys)-1)

	//find all pkColumns
	var pkColumns []*basepb.Column
	for _, c := range t.Columns {
		if c.PrimaryKey > 0 {
			pkColumns = append(pkColumns, c)
		}
	}

	for i := 0; i < len(sharingKeys); i++ {
		rangeID, err := cs.NewIDGenerate(ctx, entity.SequenceRangeID, 1, 5*time.Second)
		if err != nil {
			return nil, cblog.LogErrAndReturn(err)
		}

		targetNodes, err := cs.getNodeByLessRangeNum(ctx, nodes, tProperty.ReplicaNum)
		if err != nil {
			return nil, cblog.LogErrAndReturn(err)
		}

		var peers []*basepb.Peer
		for _, node := range targetNodes {
			peerID, err := cs.NewIDGenerate(ctx, entity.SequencePeerID, 1, 5*time.Second)

			node.RangePeers = append(node.RangePeers, &basepb.RangePeer{RangeId: uint64(rangeID), PeerId: uint64(peerID)}) //all nodes add new range server

			if err != nil {
				return nil, cblog.LogErrAndReturn(err)
			}
			peers = append(peers, &basepb.Peer{
				Id:       uint64(peerID),
				NodeId:   node.Id,
				RaftAddr: nodeRaftAddr(node),
				Type:     basepb.PeerType_PeerType_Normal,
			})
		}

		r := &basepb.Range{
			Id:          uint64(rangeID),
			TableId:     t.Id,
			DbId:        dataBase.Id,
			Peers:       peers,
			PrimaryKeys: pkColumns,
			Leader:      peers[0].NodeId,
			RangeEpoch: &basepb.RangeEpoch{
				ConfVer: 1,
				Version: 1,
			},
		}

		//the last range is index range
		if i == len(sharingKeys)-1 {
			r.StartKey, r.EndKey = utils.EncodeStorePrefix(utils.Store_Prefix_INDEX, t.Id)
		} else {
			r.StartKey = sharingKeys[i]
			r.EndKey = sharingKeys[i+1]
		}

		ranges = append(ranges, r)

		for _, node := range targetNodes {
			go func(node *basepb.Node, r *basepb.Range) {
				defer func() {
					if r := recover(); r != nil {
						err := fmt.Errorf("create range err: %v ", r)
						errChain <- err
						log.Error(err.Error())
					}
				}()
				log.Info("create node:[%d] range:[%d] peer:[%d]", node.Id, r.Id, r.Peers)
				if err := cs.dsClient.CreateRange(ctx, nodeServerAddr(node), r); err != nil {
					err := fmt.Errorf("create range err: %s ", err.Error())
					errChain <- err
					log.Error(err.Error())
				}
			}(node, r)

		}
	}

	//check all range is ok
	for i := 0; i < len(ranges); i++ {
		v := 0
		for {
			v++
			select {
			case err := <-errChain:
				return nil, cblog.LogErrAndReturn(err)
			case <-ctx.Done():
				return nil, fmt.Errorf("create table has error")
			default:

			}

			rg, err := cs.QueryRange(ctx, t.Id, ranges[i].Id)
			if v%5 == 0 {
				log.Debug("check the range :%d status ", ranges[i].Id)
			}
			if err != nil && errs.Code(err) != mspb.ErrorType_NotExistRange {
				return nil, cblog.LogErrAndReturn(err)
			}
			if rg == nil {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			break
		}
	}

	var result *basepb.Table
	t.Status = basepb.TableStatus_TableRunning
	if tableBytes, err := t.Marshal(); err != nil {
		return nil, cblog.LogErrAndReturn(err)
	} else {
		if err = cs.Update(ctx, entity.TableKey(dataBase.Id, t.Id), tableBytes); err != nil {
			t.Status = basepb.TableStatus_TableInvalid
			return nil, cblog.LogErrAndReturn(err)
		} else {
			result = t
		}
	}

	log.Info("create table[%s:%s] success", dbName, tableName)
	return result, nil
}

// update table service
func (cs *BaseService) UpdateTable(ctx context.Context, dbName, tableName, properties string, rangeKeys []string) (*basepb.Table, error) {

	// validate properties is ok
	tProperty, err := utils.ParseTableProperties(properties)
	if err != nil {
		log.Error("parse cols[%s] failed, err[%v]", properties, err)
		return nil, cblog.LogErrAndReturn(err)
	}

	dataBase, err := cs.QueryDBByName(ctx, dbName)
	if err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}

	table, err := cs.QueryTableByName(ctx, dataBase.Id, tableName)
	if err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}

	//to lock table
	lock := cs.NewLock(ctx, entity.LockTableKey(dataBase.Id, table.Id), time.Minute*5)
	if err := lock.Lock(); err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}
	defer func() {
		if err := lock.Unlock(); err != nil {
			log.Error("unlock db:[%s/%d] err:[%s] ", dataBase.Id, dataBase.Name, err)
		}
	}()

	table, err = cs.QueryTableByName(ctx, dataBase.Id, tableName)
	if err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}

	if err := utils.ModifyColumn(table, tProperty.Columns); err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}

	tableBytes, err := table.Marshal()
	if err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}

	if err := cs.Update(ctx, entity.TableKey(dataBase.Id, table.Id), tableBytes); err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}

	return table, nil
}

type kvEntry struct {
	k uint64
	v int
}

func (cs *BaseService) getNodeByLessRangeNum(ctx context.Context, nodes []*basepb.Node, replicaNum int) ([]*basepb.Node, error) {

	if len(nodes) <= replicaNum {
		return nodes, nil
	}

	//get node has ranges
	ranges, err := cs.QueryRanges(ctx, 0)
	if err != nil {
		return nil, err
	}

	nodeCount := make(map[uint64]int)

	for _, n := range nodes {
		nodeCount[n.Id] = 0
	}

	for _, r := range ranges {
		for _, p := range r.Peers {
			nodeCount[p.NodeId] = nodeCount[p.NodeId] + 1
		}
	}

	kvs := make([]*kvEntry, 0, len(nodeCount))

	for k, v := range nodeCount {
		kvs = append(kvs, &kvEntry{k, v})
	}

	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].v < kvs[j].v
	})

	result := make([]*basepb.Node, 0, len(nodes))

	for _, kv := range kvs {
		for _, node := range nodes {
			if kv.k == node.Id {
				result = append(result, node)
			}
		}
	}

	return result[:replicaNum], nil
}

func (cs *BaseService) ConfigAutoSplit(ctx context.Context) bool {
	if config, err := cs.boolConfig(ctx, entity.ConfAutoSplit); err != nil {
		log.Error("get config err:[%s]", err.Error())
		return true
	} else if config == nil {
		return true
	} else {
		return *config
	}
}

func (cs *BaseService) ConfigFailOver(ctx context.Context) bool {
	if config, err := cs.boolConfig(ctx, entity.ConfFailOver); err != nil {
		log.Error("get config err:[%s]", err.Error())
		return true
	} else if config == nil {
		return true
	} else {
		return *config
	}
}

func (cs *BaseService) ConfigBalanced(ctx context.Context) bool {
	if config, err := cs.boolConfig(ctx, entity.ConfBalanced); err != nil {
		log.Error("get config err:[%s]", err.Error())
		return true
	} else if config == nil {
		return true
	} else {
		return *config
	}
}

func (cs *BaseService) boolConfig(ctx context.Context, key string) (*bool, error) {
	bytes, err := cs.Store.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if len(bytes) > 1 {
		return nil, fmt.Errorf("make sure it`s config key?")
	} else if len(bytes) == 0 {
		return nil, nil
	} else {
		return hack.PBool(bytes[0] == 1), nil
	}

}

func (cs *BaseService) PutBoolConfig(ctx context.Context, key string, value bool) error {
	var v byte
	if value {
		v = 1
	}
	return cs.Store.Put(ctx, key, []byte{v})
}

func nodeServerAddr(node *basepb.Node) string {
	return fmt.Sprintf("%s:%d", node.GetIp(), node.GetServerPort())
}

func nodeRaftAddr(node *basepb.Node) string {
	return fmt.Sprintf("%s:%d", node.GetIp(), node.GetRaftPort())
}

func (cs *BaseService) GetDBByName(ctx context.Context, dbName string) (*basepb.DataBase, error) {
	return cs.QueryDBByName(ctx, dbName)
}
