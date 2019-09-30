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
	"github.com/chubaodb/chubaodb/master/entity/pkg/mspb"
	"github.com/chubaodb/chubaodb/master/service"
	"github.com/chubaodb/chubaodb/master/utils/ginutil"
	"github.com/chubaodb/chubaodb/master/utils/monitoring"
	"github.com/gin-gonic/gin"
	"net/http"
)

type clusterApi struct {
	router  *gin.Engine
	service *service.BaseService
	monitor monitoring.Monitor
}

func ExportToClusterHandler(router *gin.Engine, service *service.BaseService, monitor monitoring.Monitor) {

	c := &clusterApi{router: router, service: service, monitor: monitor}

	//database handler
	router.Handle(http.MethodPost, "/db/list", base30.PaincHandler, base30.TimeOutHandler, c.listDB, base30.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/db/get", base30.PaincHandler, base30.TimeOutHandler, c.getDB, base30.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/db/create", base30.PaincHandler, base30.TimeOutHandler, c.createDatabase, base30.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/db/delete", base30.PaincHandler, base30.TimeOutHandler, c.deleteDatabase, base30.TimeOutEndHandler)

	//table handler
	router.Handle(http.MethodPost, "/table/create", base30.PaincHandler, base300.TimeOutHandler, c.createTable, base30.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/table/update", base30.PaincHandler, base300.TimeOutHandler, c.updateTable, base30.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/table/get", base30.PaincHandler, base30.TimeOutHandler, c.getTable, base30.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/table/list", base30.PaincHandler, base30.TimeOutHandler, c.listTable, base30.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/table/delete", base30.PaincHandler, base30.TimeOutHandler, c.deleteTable, base30.TimeOutEndHandler)

	//column handler
	router.Handle(http.MethodPost, "/column/list", base30.PaincHandler, base30.TimeOutHandler, c.listColumn, base30.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/column/get", base30.PaincHandler, base30.TimeOutHandler, c.getColumn, base30.TimeOutEndHandler)

	//proxy id handler
	router.Handle(http.MethodPost, "/proxy/id", base30.PaincHandler, base30.TimeOutHandler, c.proxyID, base30.TimeOutEndHandler)

	//clean lock
	router.Handle(http.MethodGet, "/clean_lock", base30.PaincHandler, base30.TimeOutHandler, c.cleanLock, base30.TimeOutEndHandler)

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

	table, err := ca.service.CreateTable(ctx.(context.Context), req.DbName, req.TableName, req.Properties, req.RangeKeys)
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
func (ca *clusterApi) proxyID(c *gin.Context) {
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

	ids, err := ca.service.AutoIncIds(ctx.(context.Context), entity.SequenceProxyID(req.DbId, req.TableId), int(req.Size_))
	if err != nil {
		ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.GetAutoIncIdResponse{Header: entity.Err(err)})
		return
	}

	ginutil.NewAutoMehtodName(c, ca.monitor).Send(&mspb.GetAutoIncIdResponse{Header: entity.OK(), Ids: ids})
}
