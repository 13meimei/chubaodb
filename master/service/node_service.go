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
	"github.com/chubaodb/chubaodb/master/entity"
	"github.com/chubaodb/chubaodb/master/entity/errs"
	"github.com/chubaodb/chubaodb/master/entity/pkg/basepb"
	"github.com/chubaodb/chubaodb/master/entity/pkg/dspb"
	"github.com/chubaodb/chubaodb/master/entity/pkg/mspb"
	"github.com/chubaodb/chubaodb/master/utils/cblog"
	"github.com/spf13/cast"
	"time"
)

func (ns *BaseService) NodeHeartbeat(ctx context.Context, nodeID uint64) error {
	return ns.CreateWithTTL(ctx, entity.NodeTTLKey(nodeID), []byte(cast.ToString(nodeID)), 350*time.Second)
}

func (ns *BaseService) GetNode(ctx context.Context, nodeID uint64) (*basepb.Node, error) {
	return ns.QueryNode(ctx, nodeID)
}

func (ns *BaseService) RegisterNode(ctx context.Context, node *basepb.Node) (*basepb.Node, error) {
	if node.Ip == "" {
		return nil, errs.Error(mspb.ErrorType_ClientIPNotSet)
	}

	nodes, err := ns.QueryAllNodes(ctx)
	if err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}

	var nodeID int64

	for _, n := range nodes {
		if n.Ip == node.Ip && node.ServerPort == n.ServerPort {
			nodeID = int64(n.Id)
			node.Version = n.Version + 1
		}
	}

	if nodeID == 0 {
		if nodeID, err = ns.NewIDGenerate(ctx, entity.SequenceNodeID, 1, 5*time.Second); err != nil {
			return nil, cblog.LogErrAndReturn(err)
		}
	}

	node.Id = uint64(nodeID)

	nodeByte, err := node.Marshal()
	if err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}

	if err := ns.Put(ctx, entity.NodeKey(node.Id), nodeByte); err != nil {
		return nil, cblog.LogErrAndReturn(err)
	}

	return node, nil
}

func (bs *BaseService) NodeInfo(ctx context.Context, node *basepb.Node) (*dspb.NodeInfoResponse, error) {
	return bs.dsClient.NodeInfo(ctx, nodeServerAddr(node))
}

func (ns *BaseService) Online(ctx context.Context, nodeID uint64) (bool, error) {
	bytes, err := ns.Get(ctx, entity.NodeTTLKey(nodeID))
	return bytes != nil, err
}
