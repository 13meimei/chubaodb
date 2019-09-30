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
	"github.com/chubaodb/chubaodb/master/entity"
	"github.com/chubaodb/chubaodb/master/entity/pkg/basepb"
	"github.com/chubaodb/chubaodb/master/entity/pkg/mspb"
	"github.com/chubaodb/chubaodb/master/service"
	"github.com/chubaodb/chubaodb/master/utils/ginutil"
	"github.com/chubaodb/chubaodb/master/utils/log"
	"github.com/chubaodb/chubaodb/master/utils/monitoring"
	"github.com/gin-gonic/gin"
	"net/http"
	"strings"
)

type nodeApi struct {
	router      *gin.Engine
	nodeService *service.BaseService
	monitor     monitoring.Monitor
}

func ExportToNodeHandler(router *gin.Engine, nodeService *service.BaseService, monitor monitoring.Monitor) {

	c := &nodeApi{router: router, nodeService: nodeService, monitor: monitor}

	router.Handle(http.MethodPost, "/node/heartbeat", base30.PaincHandler, base30.TimeOutHandler, c.nodeHeartbeat, base30.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/node/register", base30.PaincHandler, base30.TimeOutHandler, c.registerNode, base30.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/node/get", base30.PaincHandler, base30.TimeOutHandler, c.getNode, base30.TimeOutEndHandler)
	router.Handle(http.MethodPost, "/node/list", base30.PaincHandler, base30.TimeOutHandler, c.listNode, base30.TimeOutEndHandler)

}

func (na *nodeApi) nodeHeartbeat(c *gin.Context) {
	ctx, _ := c.Get(Ctx)

	req := &mspb.NodeHeartbeatRequest{}
	if err := bind(c, req); err != nil {
		ginutil.NewAutoMehtodName(c, na.monitor).Send(&mspb.NodeHeartbeatResponse{Header: entity.Err(err)})
		return
	}

	err := na.nodeService.NodeHeartbeat(ctx.(context.Context), req.NodeId)
	if err != nil {
		ginutil.NewAutoMehtodName(c, na.monitor).Send(&mspb.NodeHeartbeatResponse{Header: entity.Err(err)})
		return
	}

	ginutil.NewAutoMehtodName(c, na.monitor).Send(&mspb.NodeHeartbeatResponse{Header: entity.OK(), NodeId: req.NodeId})
}

// register a node , when new  or restart
func (na *nodeApi) registerNode(c *gin.Context) {
	ctx, _ := c.Get(Ctx)

	req := &mspb.RegisterNodeRequest{}
	if err := bind(c, req); err != nil {
		ginutil.NewAutoMehtodName(c, na.monitor).Send(&mspb.RegisterNodeResponse{Header: entity.Err(err)})
		return
	}

	ip := c.Request.RemoteAddr[0:strings.LastIndex(c.Request.RemoteAddr, ":")]

	node := &basepb.Node{
		Ip:         ip,
		ServerPort: req.ServerPort,
		RaftPort:   req.RaftPort,
		AdminPort:  req.AdminPort,
		State:      basepb.NodeState_N_Initial,
		Version:    1,
	}

	node, err := na.nodeService.RegisterNode(ctx.(context.Context), node)
	if err != nil {
		ginutil.NewAutoMehtodName(c, na.monitor).Send(&mspb.RegisterNodeResponse{Header: entity.Err(err)})
		return
	}

	ginutil.NewAutoMehtodName(c, na.monitor).Send(&mspb.RegisterNodeResponse{Header: entity.OK(), NodeId: node.Id})
}

// to find a node by nodeID
func (na *nodeApi) getNode(c *gin.Context) {
	ctx, _ := c.Get(Ctx)

	req := &mspb.GetNodeRequest{}
	if err := bind(c, req); err != nil {
		ginutil.NewAutoMehtodName(c, na.monitor).Send(&mspb.GetNodeResponse{Header: entity.Err(err)})
		return
	}

	node, err := na.nodeService.GetNode(ctx.(context.Context), req.Id)
	if err != nil {
		ginutil.NewAutoMehtodName(c, na.monitor).Send(&mspb.GetNodeResponse{Header: entity.Err(err)})
		return
	}

	ginutil.NewAutoMehtodName(c, na.monitor).Send(&mspb.GetNodeResponse{Header: entity.OK(), Node: node})
}

// to find all node
func (na *nodeApi) listNode(c *gin.Context) {
	ctx, _ := c.Get(Ctx)

	req := &mspb.GetNodesRequest{}
	if err := bind(c, req); err != nil {
		ginutil.NewAutoMehtodName(c, na.monitor).Send(&mspb.GetNodesResponse{Header: entity.Err(err)})
		return
	}

	nodes, err := na.nodeService.QueryAllNodes(ctx.(context.Context))
	if err != nil {
		ginutil.NewAutoMehtodName(c, na.monitor).Send(&mspb.GetNodesResponse{Header: entity.Err(err)})
		return
	}

	for _, node := range nodes {
		online, err := na.nodeService.Online(ctx.(context.Context), node.Id)
		if err != nil {
			log.Error("get node by ttl key err:[%s]", err.Error())
		}

		if !online {
			node.State = basepb.NodeState_N_Offline
		}
	}

	ginutil.NewAutoMehtodName(c, na.monitor).Send(&mspb.GetNodesResponse{Header: entity.OK(), Nodes: nodes})
}
