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
	"context"
	"fmt"
	"github.com/chubaodb/chubaodb/master/entity"
	"github.com/chubaodb/chubaodb/master/entity/pkg/basepb"
	"github.com/chubaodb/chubaodb/master/entity/pkg/mspb"
	"github.com/chubaodb/chubaodb/master/service"
	"github.com/chubaodb/chubaodb/master/utils/bytes"
	"github.com/chubaodb/chubaodb/master/utils/ginutil"
	"github.com/chubaodb/chubaodb/master/utils/log"
	"github.com/chubaodb/chubaodb/master/utils/monitoring"
	"github.com/gin-gonic/gin"
	"net/http"
)

type clusterApi struct {
	router  *gin.Engine
	service *service.BaseService
	monitor monitoring.Monitor
}

func ExportToClusterHandler(router *gin.Engine, service *service.BaseService) {
	m := entity.Monitor()
	c := &clusterApi{router: router, service: service, monitor: m}

	//database handler
	base30 := newBaseHandler(30, m)
	base60 := newBaseHandler(60, m)

	router.Handle(http.MethodGet, "/", base30.TimeOutHandler, base30.PaincHandler(c.clusterInfo), base30.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/", base30.TimeOutHandler, base30.PaincHandler(c.clusterInfo), base30.TimeOutEndHandler)

	router.Handle(http.MethodPost, "/db/list", base30.TimeOutHandler, base30.PaincHandler(c.listDB), base30.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/db/get", base30.TimeOutHandler, base30.PaincHandler(c.getDB), base30.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/db/create", base30.TimeOutHandler, base30.PaincHandler(c.createDatabase), base30.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/db/delete", base30.TimeOutHandler, base30.PaincHandler(c.deleteDatabase), base30.TimeOutEndHandler)

	//table handler
	router.Handle(http.MethodPost, "/table/create", base60.TimeOutHandler, base60.PaincHandler(c.createTable), base30.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/table/update", base60.TimeOutHandler, base60.PaincHandler(c.updateTable), base30.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/table/get", base30.TimeOutHandler, base30.PaincHandler(c.getTable), base30.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/table/list", base30.TimeOutHandler, base30.PaincHandler(c.listTable), base30.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/table/delete", base30.TimeOutHandler, base30.PaincHandler(c.deleteTable), base30.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/table/count", base30.TimeOutHandler, base30.PaincHandler(c.countTable), base30.TimeOutEndHandler)

	//column handler
	router.Handle(http.MethodPost, "/column/list", base30.TimeOutHandler, base30.PaincHandler(c.listColumn), base30.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/column/get", base30.TimeOutHandler, base30.PaincHandler(c.getColumn), base30.TimeOutEndHandler)

	//proxy id handler
	router.Handle(http.MethodPost, "/table/auto_increment_id", base30.TimeOutHandler, base30.PaincHandler(c.docAutoIncrement), base30.TimeOutEndHandler)

	//clean lock
	router.Handle(http.MethodGet, "/clean_lock", base30.TimeOutHandler, base30.PaincHandler(c.cleanLock), base30.TimeOutEndHandler)

}

func (ca *clusterApi) clusterInfo(c *gin.Context) {
	ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.ClusterInfoResponse{
		Header:       entity.OK(),
		BuildVersion: entity.GetBuildVersion(),
		BuildTime:    entity.GetBuildTime(),
		CommitId:     entity.GetCommitID(),
	})
}

//clean lock for admin , when space locked , waring make sure not create space ing , only support json
func (ca *clusterApi) cleanLock(c *gin.Context) {
	ctx, _ := c.Get(Ctx)

	removed := make([]string, 0, 1)

	if keys, _, err := ca.service.PrefixScan(ctx.(context.Context), entity.PrefixLock); err != nil {
		ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.GetDBsResponse{Header: entity.Err(err)})
		return
	} else {
		for _, key := range keys {
			if err := ca.service.Delete(ctx.(context.Context), string(key)); err != nil {
				ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.GetDBsResponse{Header: entity.Err(err)})
				return
			}
			removed = append(removed, string(key))
		}
	}

	result := &struct {
		Header  *mspb.ResponseHeader `json:"header"`
		Removed []string             `json:"removed"`
	}{
		Header:  entity.OK(),
		Removed: removed,
	}

	ginutil.NewAutoMehtodName(c, ca.monitor).SendJson(result)
}

//list db
func (ca *clusterApi) listDB(c *gin.Context) {
	ctx, _ := c.Get(Ctx)
	if dbs, err := ca.service.QueryDatabases(ctx.(context.Context)); err != nil {
		ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.GetDBsResponse{Header: entity.Err(err)})
	} else {
		ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.GetDBsResponse{Header: entity.OK(), Dbs: dbs})
	}
}

// get db
func (ca *clusterApi) getDB(c *gin.Context) {
	ctx, _ := c.Get(Ctx)

	req := &mspb.GetDBRequest{}
	if err := bind(c, req); err != nil {
		ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.GetDBResponse{Header: entity.Err(err)})
		return
	}

	base, err := ca.service.GetDBByName(ctx.(context.Context), req.Name)
	if err != nil {
		ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.GetDBResponse{Header: entity.Err(err)})
		return
	}

	ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.GetDBResponse{Header: entity.OK(), Db: base})
}

// Get table by   GetTableRequest , (DbId OR DBName) (TableId OR TableName)
// response has table struct
func (ca *clusterApi) getTable(c *gin.Context) {
	ctx, _ := c.Get(Ctx)

	req := &mspb.GetTableRequest{}
	if err := bind(c, req); err != nil {
		ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.GetTableResponse{Header: entity.Err(err)})
		return
	}

	table, err := ca.service.GetTable(ctx.(context.Context), req.DbId, req.DbName, req.TableId, req.TableName)
	if err != nil {
		ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.GetTableResponse{Header: entity.Err(err)})
		return
	}

	ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.GetTableResponse{Header: entity.OK(), Table: table})
}

// Get table by   GetTableRequest , (DbId OR DBName) (TableId OR TableName)
// response has table struct
func (ca *clusterApi) listTable(c *gin.Context) {
	ctx, _ := c.Get(Ctx)

	req := &mspb.GetTablesRequest{}
	if err := bind(c, req); err != nil {
		ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.GetTablesResponse{Header: entity.Err(err)})
		return
	}

	tables, err := ca.service.GetTables(ctx.(context.Context), req.DbId, req.DbName)
	if err != nil {
		ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.GetTablesResponse{Header: entity.Err(err)})
		return
	}

	ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.GetTablesResponse{Header: entity.OK(), Tables: tables})
}

func (ca *clusterApi) deleteTable(c *gin.Context) {
	ctx, _ := c.Get(Ctx)

	req := &mspb.DelTableRequest{}
	if err := bind(c, req); err != nil {
		ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.DelTableResponse{Header: entity.Err(err)})
		return
	}

	table, err := ca.service.DelTable(ctx.(context.Context), req.DbId, req.DbName, req.TableId, req.TableName)
	if err != nil {
		ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.DelTableResponse{Header: entity.Err(err)})
		return
	}

	ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.DelTableResponse{Header: entity.OK(), Table: table})
}

func (ca *clusterApi) countTable(c *gin.Context) {
	ctx, _ := c.Get(Ctx)

	req := &mspb.CountTableRequest{}
	if err := bind(c, req); err != nil {
		ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.CountTableResponse{Header: entity.Err(err)})
		return
	}

	dbTableCount, err := ca.service.TableDocNum(ctx.(context.Context))
	if err != nil {
		ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.CountTableResponse{Header: entity.Err(err)})
		return
	}

	resp := &mspb.CountTableResponse{
		Header: entity.OK(),
	}

	for dbID, tableMap := range dbTableCount {
		for tableID, count := range tableMap {
			resp.List = append(resp.List, &mspb.Counter{
				DbId:    dbID,
				TableId: tableID,
				Count:   count,
			})
		}
	}

	ginutil.NewAutoMehtodName(c, ca.monitor).Send(resp)
}

//Get all columns by db_id and table_id
func (ca *clusterApi) listColumn(c *gin.Context) {
	ctx, _ := c.Get(Ctx)

	req := &mspb.GetColumnsRequest{}
	if err := bind(c, req); err != nil {
		ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.GetColumnsResponse{Header: entity.Err(err)})
		return
	}

	columns, err := ca.service.GetColumns(ctx.(context.Context), req.DbId, req.TableId)
	if err != nil {
		ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.GetColumnsResponse{Header: entity.Err(err)})
		return
	}

	ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.GetColumnsResponse{Header: entity.OK(), Columns: columns})
}

//Get column
func (ca *clusterApi) getColumn(c *gin.Context) {
	ctx, _ := c.Get(Ctx)

	req := &mspb.GetColumnRequest{}
	if err := bind(c, req); err != nil {
		ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.GetColumnResponse{Header: entity.Err(err)})
		return
	}

	column, err := ca.service.GetColumn(ctx.(context.Context), req.DbId, req.TableId, req.ColId, req.ColName)
	if err != nil {
		ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.GetColumnResponse{Header: entity.Err(err)})
		return
	}

	ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.GetColumnResponse{Header: entity.OK(), Column: column})
}

// delete database the name is unique
func (ca *clusterApi) deleteDatabase(c *gin.Context) {
	ctx, _ := c.Get(Ctx)

	req := &mspb.DelDatabaseRequest{}
	if err := bind(c, req); err != nil {
		ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.CreateDatabaseResponse{Header: entity.Err(err)})
		return
	}

	err := ca.service.DeleteDatabase(ctx.(context.Context), req.Id, req.GetName())
	if err != nil {
		ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.CreateDatabaseResponse{Header: entity.Err(err)})
		return
	}

	ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.DelDatabaseResponse{Header: entity.OK()})
}

// create database the name is unique
func (ca *clusterApi) createDatabase(c *gin.Context) {
	ctx, _ := c.Get(Ctx)

	req := &mspb.CreateDatabaseRequest{}
	if err := bind(c, req); err != nil {
		ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.CreateDatabaseResponse{Header: entity.Err(err)})
		return
	}

	database, err := ca.service.CreateDatabase(ctx.(context.Context), req.GetName())
	if err != nil {
		ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.CreateDatabaseResponse{Header: entity.Err(err)})
		return
	}

	ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.CreateDatabaseResponse{Header: entity.OK(), Database: database})
}

// create table
func (ca *clusterApi) createTable(c *gin.Context) {
	ctx, _ := c.Get(Ctx)

	req := &mspb.CreateTableRequest{}
	if err := bind(c, req); err != nil {
		ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.CreateTableResponse{Header: entity.Err(err)})
		return
	}

	var storeType basepb.StoreType

	switch req.StoreType {
	case "hot", "":
		storeType = basepb.StoreType_Store_Hot
	case "warm":
		storeType = basepb.StoreType_Store_Warm
	case "mix":
		storeType = basepb.StoreType_Store_Mix
	default:
		ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.CreateTableResponse{Header: entity.Err(fmt.Errorf("store type err:[%s] only support ['hot','warm','min']", req.StoreType))})
		return
	}

	if req.ReplicaNum == 0 {
		req.ReplicaNum = uint64(entity.Conf().Global.ReplicaNum)
	}

	if req.ReplicaNum <= 0 {
		req.ReplicaNum = 3
	}

	if req.DataRangeNum == 0 {
		req.DataRangeNum = 2
	}

	table, err := ca.service.CreateTable(ctx.(context.Context), req.DbName, req.TableName, req.Properties, storeType, req.ReplicaNum, req.DataRangeNum)
	if err != nil {
		ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.CreateTableResponse{Header: entity.Err(err)})
		return
	}

	ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.CreateTableResponse{Header: entity.OK(), Table: table})
}

// update table it use create table request
func (ca *clusterApi) updateTable(c *gin.Context) {
	ctx, _ := c.Get(Ctx)

	req := &mspb.CreateTableRequest{}
	if err := bind(c, req); err != nil {
		ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.CreateTableResponse{Header: entity.Err(err)})
		return
	}

	table, err := ca.service.UpdateTable(ctx.(context.Context), req.DbName, req.TableName, req.Properties, req.RangeKeys)
	if err != nil {
		ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.CreateTableResponse{Header: entity.Err(err)})
		return
	}

	ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.CreateTableResponse{Header: entity.OK(), Table: table})
}

// create table
func (ca *clusterApi) docAutoIncrement(c *gin.Context) {
	ctx, _ := c.Get(Ctx)

	req := &mspb.GetAutoIncIdRequest{}
	if err := bind(c, req); err != nil {
		ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.GetAutoIncIdResponse{Header: entity.Err(err)})
		return
	}

	if req.Size_ > 100000 {
		ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.GetAutoIncIdResponse{Header: entity.Err(fmt.Errorf("to large id size:[%d]", req.Size_))})
		return
	}

	ids, err := ca.service.AutoIncIds(ctx.(context.Context), req.DbId, req.TableId, uint64(req.Size_))
	if err != nil {
		ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.GetAutoIncIdResponse{Header: entity.Err(err)})
		return
	}

	if len(ids)%2 != 0 {
		ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.GetAutoIncIdResponse{Header: entity.Err(fmt.Errorf("id create has err need double num but go :[%v]", ids))})
		return
	}

	pairs := make([]*mspb.IdPair, 0, len(ids)/2)

	for i := 0; i < len(ids); i += 2 {
		log.Debug("auto increment id [%d]/[%d]", ids[i], ids[i+1])
		pairs = append(pairs, &mspb.IdPair{
			IdStart: bytes.Uint64ToByte(ids[i]),
			IdEnd:   bytes.Uint64ToByte(ids[i+1]),
		})
	}

	ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.GetAutoIncIdResponse{Header: entity.OK(), IdPairs: pairs})
}
