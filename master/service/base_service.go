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
	"github.com/chubaodb/chubaodb/master/client/ds_client"
	"github.com/chubaodb/chubaodb/master/client/store"
	"github.com/chubaodb/chubaodb/master/entity"
	"github.com/chubaodb/chubaodb/master/entity/errs"
	"github.com/chubaodb/chubaodb/master/entity/pkg/basepb"
	"github.com/chubaodb/chubaodb/master/entity/pkg/mspb"
	utilBytes "github.com/chubaodb/chubaodb/master/utils/bytes"
	"github.com/chubaodb/chubaodb/master/utils/cblog"
	"github.com/chubaodb/chubaodb/master/utils/log"
	"github.com/spf13/cast"
	"go.etcd.io/etcd/clientv3/concurrency"
)

func NewBaseService() (*BaseService, error) {
	openStore, err := store.OpenStore("etcd", entity.Conf().Masters.ClientAddress())
	if err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}
	return &BaseService{
		Store:     openStore,
		dsClient:  client.NewSchRPCClient(1),
		admClient: client.NewAdminClient("", 1),
	}, nil
}

type BaseService struct {
	store.Store
	dsClient  client.SchClient
	admClient client.AdminClient
}

func (bs *BaseService) QueryNode(ctx context.Context, nodeID uint64) (*basepb.Node, error) {
	bytes, err := bs.Get(ctx, entity.NodeKey(nodeID))
	if err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}
	if bytes == nil {
		return nil, errs.Error(mspb.ErrorType_NotExistNode)
	}

	node := &basepb.Node{}
	if err := node.Unmarshal(bytes); err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}
	return node, nil
}

func (bs *BaseService) QueryAllNodes(ctx context.Context) ([]*basepb.Node, error) {
	_, value, err := bs.PrefixScan(ctx, entity.PrefixNode)
	if err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}

	nodes := make([]*basepb.Node, 0, len(value))

	for _, nodeByte := range value {
		node := &basepb.Node{}
		if err := node.Unmarshal(nodeByte); err != nil {
			return nil, cblog.LogErrAndReturn(err)
		}
		nodes = append(nodes, node)
	}

	return nodes, nil

}

func (bs *BaseService) QueryOnlineNodes(ctx context.Context) ([]*basepb.Node, error) {
	_, value, err := bs.PrefixScan(ctx, entity.PrefixNodeTTL)
	if err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}

	nodes := make([]*basepb.Node, 0, len(value))

	for _, nodeIDByte := range value {
		node, err := bs.GetNode(ctx, cast.ToUint64(string(nodeIDByte)))
		if err != nil {
			log.Error("query online node by:[%s] id err:[%s]", string(nodeIDByte), err.Error())
		} else {
			nodes = append(nodes, node)
		}

	}

	return nodes, nil

}

func (bs *BaseService) QueryTablesByDBID(ctx context.Context, dbID uint64) ([]*basepb.Table, error) {
	_, values, e := bs.PrefixScan(ctx, entity.TableKeyPre(dbID))
	if e != nil {
		return nil, e
	}

	tables := make([]*basepb.Table, len(values))

	for i := range values {
		tables[i] = &basepb.Table{}
		if e := tables[i].Unmarshal(values[i]); e != nil {
			return nil, e
		}
	}

	return tables, nil
}

func (bs *BaseService) QueryTableByName(ctx context.Context, dbID uint64, tableName string) (*basepb.Table, error) {
	tables, err := bs.QueryTablesByDBID(ctx, dbID)
	if err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}

	for _, t := range tables {
		if t.Name == tableName {
			return t, nil
		}
	}
	return nil, errs.Error(mspb.ErrorType_NotExistTable)
}

func (bs *BaseService) QueryTableByID(ctx context.Context, dbID, tableID uint64) (*basepb.Table, error) {
	bytes, err := bs.Get(ctx, entity.TableKey(dbID, tableID))
	if err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}
	if bytes == nil {
		return nil, errs.Error(mspb.ErrorType_NotExistTable)
	}

	table := &basepb.Table{}
	if err = table.Unmarshal(bytes); err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}
	return table, nil
}

func (bs *BaseService) QueryDatabases(ctx context.Context) ([]*basepb.DataBase, error) {
	_, value, err := bs.PrefixScan(ctx, fmt.Sprintf("%sid/", entity.PrefixDataBase))
	if err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}

	dbs := make([]*basepb.DataBase, 0, len(value))

	for _, dbByte := range value {
		db := &basepb.DataBase{}
		if err := db.Unmarshal(dbByte); err != nil {
			return nil, cblog.LogErrAndReturn(err)
		}
		dbs = append(dbs, db)
	}

	return dbs, nil

}

func (bs *BaseService) QueryDBByName(ctx context.Context, dbName string) (*basepb.DataBase, error) {
	// check the table if exist
	dbIDByte, err := bs.Get(ctx, entity.DBKeyName(dbName))
	if err != nil {
		return nil, cblog.LogErrAndReturn(err)
	} else if dbIDByte == nil {
		return nil, errs.Error(mspb.ErrorType_NotExistDatabase)
	}

	dbID, err := cast.ToUint64E(string(dbIDByte))
	if err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}

	return bs.QueryDBByID(ctx, dbID)
}

func (bs *BaseService) QueryDBByID(ctx context.Context, dbID uint64) (*basepb.DataBase, error) {
	bytes, err := bs.Get(ctx, entity.DBKey(dbID))
	if err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}
	if bytes == nil {
		return nil, errs.Error(mspb.ErrorType_NotExistDatabase)
	}

	base := &basepb.DataBase{}

	if err := base.Unmarshal(bytes); err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}

	return base, nil
}

func (bs *BaseService) QueryRange(ctx context.Context, tableID uint64, rangeID uint64) (*basepb.Range, error) {
	bytes, err := bs.Get(ctx, entity.RangeKey(tableID, rangeID))
	if err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}
	if bytes == nil {
		return nil, errs.Error(mspb.ErrorType_NotExistRange)
	}

	base := &basepb.Range{}

	if err := base.Unmarshal(bytes); err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}

	return base, nil
}

// if tableID ==0 it will return all ranges , if table not exist it not return any error
func (bs *BaseService) QueryRanges(ctx context.Context, tableID uint64) ([]*basepb.Range, error) {

	var (
		values [][]byte
		e      error
	)

	if tableID == 0 {
		_, values, e = bs.PrefixScan(ctx, entity.PrefixRange)
	} else {
		_, values, e = bs.PrefixScan(ctx, entity.RangeKeyPre(tableID))
	}

	if e != nil {
		return nil, e
	}

	ranges := make([]*basepb.Range, len(values))

	for i := range values {
		ranges[i] = &basepb.Range{}
		if e := ranges[i].Unmarshal(values[i]); e != nil {
			return nil, e
		}
	}

	return ranges, nil
}

// create a id
func (bs *BaseService) AutoIncIds(ctx context.Context, dbID, tableID uint64, size uint64) ([]uint64, error) {

	ids := make([]uint64, 0, size)

	if size == 0 {
		return ids, nil
	}

	err := bs.STM(ctx, func(stm concurrency.STM) error {

		v := stm.Get(string(entity.SequenceDocument(dbID, tableID)))
		if len(v) == 0 {
			return cblog.LogErrAndReturn(fmt.Errorf("can not found dbID:[%d] tableID:[%d]", dbID, tableID))
		}

		avg := size / uint64(len(v)/8)

		if avg == 0 {
			return fmt.Errorf("size:[%d] must more than doc_range_num:[%d]", size, uint64(len(v)/8))
		}

		rowKeys := bytes.Buffer{}

		for i := 0; i < len(v)/8; i++ {
			id := utilBytes.ByteArray2UInt64([]byte(v[i*8 : i*8+8]))
			ids = append(ids, id+1, id+avg)
			rowKeys.Write(utilBytes.Uint64ToByte(id + avg))
		}

		stm.Put(entity.SequenceDocument(dbID, tableID), rowKeys.String())

		return nil
	})

	if err != nil {
		return nil, err
	}

	return ids, nil
}
