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
	"bytes"
	"context"
	"github.com/chubaodb/chubaodb/master/entity"
	"github.com/chubaodb/chubaodb/master/entity/pkg/basepb"
	"github.com/chubaodb/chubaodb/master/entity/pkg/mspb"
	"github.com/chubaodb/chubaodb/master/service"
	"github.com/chubaodb/chubaodb/master/utils/ginutil"
	"github.com/chubaodb/chubaodb/master/utils/monitoring"
	"github.com/gin-gonic/gin"
	"github.com/spf13/cast"
	"net/http"
)

//it not support proto api
type healthApi struct {
	router  *gin.Engine
	service *service.BaseService
	monitor monitoring.Monitor
}

func ExportToHealthHandler(router *gin.Engine, service *service.BaseService, monitor monitoring.Monitor) {

	c := &healthApi{router: router, service: service, monitor: monitor}

	router.Handle(http.MethodGet, "/health/node_info", base30.PaincHandler, base30.TimeOutHandler, c.nodeInfo, base30.TimeOutEndHandler)

	router.Handle(http.MethodGet, "/health/view_info", base30.PaincHandler, base30.TimeOutHandler, c.clusterInfoView, base30.TimeOutEndHandler)

	router.Handle(http.MethodGet, "/health/conf", base30.PaincHandler, base30.TimeOutHandler, c.config, base30.TimeOutEndHandler)

}

//it not support proto api
func (ha *healthApi) nodeInfo(c *gin.Context) {
	ctx, _ := c.Get(Ctx)

	nodes, err := ha.service.QueryOnlineNodes(ctx.(context.Context))
	if err != nil {
		ginutil.NewAutoMehtodName(c, ha.monitor).Send(&mspb.GetNodeInfoResponse{Header: entity.Err(err)})
		return
	}

	nodeInfos := make([]*mspb.GetNodeInfo, 0, len(nodes))

	for _, node := range nodes {
		nodeInfo, err := ha.service.NodeInfo(ctx.(context.Context), node)
		var gnir *mspb.GetNodeInfo
		if err != nil {
			gnir = &mspb.GetNodeInfo{Err: err.Error(), Node: node}
		} else {
			gnir = &mspb.GetNodeInfo{Node: node, Info: nodeInfo}
		}
		nodeInfos = append(nodeInfos, gnir)
	}

	ginutil.NewAutoMehtodName(c, ha.monitor).Send(&mspb.GetNodeInfoResponse{Header: entity.OK(), NodeInfos: nodeInfos})
}

func (ha *healthApi) clusterInfoView(c *gin.Context) {
	ctx, _ := c.Get(Ctx)

	nodes, err := ha.service.QueryOnlineNodes(ctx.(context.Context))
	if err != nil {
		ginutil.NewAutoMehtodName(c, ha.monitor).Send(&mspb.GetNodeInfoResponse{Header: entity.Err(err)})
		return
	}

	nodeInfos := make([]*mspb.GetNodeInfo, 0, len(nodes))

	for _, node := range nodes {
		nodeInfo, err := ha.service.NodeInfo(ctx.(context.Context), node)
		var gnir *mspb.GetNodeInfo
		if err != nil {
			gnir = &mspb.GetNodeInfo{Err: err.Error(), Node: node}
		} else {
			gnir = &mspb.GetNodeInfo{Node: node, Info: nodeInfo}
		}
		nodeInfos = append(nodeInfos, gnir)
	}

	buf := bytes.Buffer{}

	snapshot := make(map[[2]uint64]bool)

	down := make(map[[2]uint64]bool)
	for _, info := range nodeInfos {
		if info == nil || info.GetErr() != "" {
			continue
		}
		for _, r := range info.Info.RangeInfos {
			for _, ps := range r.PeersStatus {
				if ps.DownSeconds > 0 {
					down[[2]uint64{ps.Peer.NodeId, r.Range.Id}] = true
				} else if ps.Snapshotting || ps.Peer.Type != basepb.PeerType_PeerType_Normal {
					snapshot[[2]uint64{ps.Peer.NodeId, r.Range.Id}] = true
				}
			}
		}
	}

	buf.WriteString("<table border=1>")

	for _, info := range nodeInfos {
		if info == nil {
			continue
		}
		buf.WriteString("<tr>")
		buf.WriteString("<td  bgcolor=#99CCCC width=100> Node" + cast.ToString(info.Node.Id) + ":[" + info.Node.Ip + ":" + cast.ToString(info.Node.ServerPort) + "] </td>")

		leaderNum := 0
		if info.Err != "" {
			buf.WriteString("<td width=50 bgcolor=#FF6666>" + info.Err + "</td>")
		} else {
			line := bytes.Buffer{}
			for _, r := range info.Info.RangeInfos {
				bg := "#FFCCCC"
				if r.Range.Leader == info.Node.Id {
					leaderNum++
					bg = "#FF6666"
				}

				line.WriteString("<td  width=50 bgcolor=" + bg)
				if down[[2]uint64{info.Node.Id, r.Range.Id}] {
					line.WriteString(` style="color:#FF0000" `)
				} else if snapshot[[2]uint64{info.Node.Id, r.Range.Id}] {
					line.WriteString(` style="color:#FFF" `)
				}
				line.WriteString(">")
				line.WriteString(cast.ToString(r.Range.Id))
				line.WriteString("</td>")
			}
			buf.WriteString("<td width=50 bgcolor=#99CC00>" + cast.ToString(leaderNum) + "/" + cast.ToString(len(info.Info.RangeInfos)) + "</td>")
			buf.Write(line.Bytes())
		}

		buf.WriteString("</tr>\n")
	}
	buf.WriteString("</table>\n")
	_, _ = c.Writer.WriteString(buf.String())
}

type ConfigResult struct {
	Error     string `json:"error,omitempty"`
	AutoSplit bool   `json:"auto_split"`
	FailOver  bool   `json:"fail_over"`
	Balanced  bool   `json:"balanced"`
}

func (ha *healthApi) config(c *gin.Context) {
	cc, _ := c.Get(Ctx)
	var ctx = cc.(context.Context)

	autoSplit := c.Query("auto_split")
	failOver := c.Query("fail_over")
	balanced := c.Query("balanced")

	result := &ConfigResult{}

	if autoSplit != "" {
		ha.configOK(ctx, autoSplit, entity.ConfAutoSplit, result)
	}

	if failOver != "" {
		ha.configOK(ctx, failOver, entity.ConfFailOver, result)
	}

	if balanced != "" {
		ha.configOK(ctx, balanced, entity.ConfBalanced, result)
	}

	result.AutoSplit = ha.service.ConfigAutoSplit(ctx)
	result.FailOver = ha.service.ConfigFailOver(ctx)
	result.Balanced = ha.service.ConfigBalanced(ctx)

	ginutil.NewAutoMehtodName(c, ha.monitor).SendJson(result)
}

func (ha *healthApi) configOK(ctx context.Context, val, key string, result *ConfigResult) {
	if val == "" {
		return
	}
	b, err := cast.ToBoolE(val)
	if err != nil {
		result.Error = err.Error()
		return
	}
	err = ha.service.PutBoolConfig(ctx, key, b)
	if err != nil {
		result.Error = err.Error()
	}

}
