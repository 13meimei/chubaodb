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

package entity

import (
	"fmt"
	"github.com/chubaodb/chubaodb/master/entity/errs"
	"github.com/chubaodb/chubaodb/master/entity/pkg/basepb"
	"github.com/chubaodb/chubaodb/master/entity/pkg/mspb"
)

//ids sequence key for etcd
const (
	SequenceNodeID  = "/sequence/node/"
	SequenceRangeID = "/sequence/range/"
	SequenceTableID = "/sequence/table/"
	SequencePeerID  = "/sequence/peer/"
	SequenceDBID    = "/sequence/db/"
	SequenceIndexID = "/sequence/index/"
	sequenceProxyID = "/sequence/proxy/"
)

const (
	PrefixLock        = "/lock/"
	PrefixLockCluster = "/lock/_cluster/"
	PrefixNode        = "/node/"
	PrefixNodeTTL     = "/ttl/node/"
	PrefixTable       = "/table/"
	PrefixRange       = "/range/"
	PrefixDataBase    = "/db/"
)

const (
	ConfAutoSplit = "/config/auto_split"
	ConfFailOver  = "/config/fail_over"
	ConfBalanced  = "/config/balanced"
)

//when master runing clean job , it will set value to this key,
//when other got key , now time less than this they will skip this job
const ClusterCleanJobKey = "/cluster/cleanjob"

func DBKey(id uint64) string {
	return fmt.Sprintf("%sid/%d", PrefixDataBase, id)
}

func DBKeyName(name string) string {
	return fmt.Sprintf("%sname/%s", PrefixDataBase, name)
}

func LockCluster() string {
	return fmt.Sprintf("%s", PrefixLockCluster)
}

func LockDBKey(dbID uint64) string {
	return fmt.Sprintf("%s/%d", PrefixLockCluster, dbID)
}

func LockTableKey(dbID, tableID uint64) string {
	return fmt.Sprintf("%s%d/%d", PrefixLock, dbID, tableID)
}

func NodeKey(nodeID uint64) string {
	return fmt.Sprintf("%s%d", PrefixNode, nodeID)
}

func NodeTTLKey(nodeID uint64) string {
	return fmt.Sprintf("%s%d", PrefixNodeTTL, nodeID)
}

func TableKey(dbID, tableID uint64) string {
	return fmt.Sprintf("%s%d/%d", PrefixTable, dbID, tableID)
}

func TableKeyPre(dbID uint64) string {
	return fmt.Sprintf("%s%d/", PrefixTable, dbID)
}

func RangeKey(tableID, rangeID uint64) string {
	return fmt.Sprintf("%s%d/%d", PrefixRange, tableID, rangeID)
}

func RangeKeyPre(tableID uint64) string {
	return fmt.Sprintf("%s%d/", PrefixRange, tableID)
}

func DBKeys(id uint64, name string) (dbKey, nameKey string) {
	dbKey = DBKey(id)
	nameKey = DBKeyName(name)
	return
}

func SequenceProxyID(dbID, tableID uint64) string {
	return fmt.Sprintf("%s%d/%d", sequenceProxyID, dbID, tableID)
}

func OK() *mspb.ResponseHeader {
	return &mspb.ResponseHeader{ClusterId: Conf().Global.ClusterID}
}

func Err(e error) *mspb.ResponseHeader {
	if e == nil {
		e = errs.Error(mspb.ErrorType_UnDefine)
	}
	return &mspb.ResponseHeader{ClusterId: Conf().Global.ClusterID, Error: &mspb.Error{Code: uint32(errs.Code(e)), Message: e.Error()}}
}

type TableProperty struct {
	Columns    []*basepb.Column `json:"columns"`
	Indexes    []*basepb.Index  `json:"indexes"`
	ReplicaNum int              `json:"replica_num"`
}
