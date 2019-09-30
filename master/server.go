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
	"os"
	"strings"
	"time"
	"context"
	"fmt"
	client "github.com/chubaodb/chubaodb/master/client/ds_client"
	"github.com/chubaodb/chubaodb/master/entity"
	"github.com/chubaodb/chubaodb/master/schedule"
	"github.com/chubaodb/chubaodb/master/service"
	"github.com/chubaodb/chubaodb/master/utils/cblog"
	"github.com/chubaodb/chubaodb/master/utils/log"
	"github.com/chubaodb/chubaodb/master/utils/monitoring"
	"github.com/gin-gonic/gin"
	"github.com/spf13/cast"
	"go.etcd.io/etcd/embed"
)

type Server struct {
	etcCfg     *embed.Config
	etcdServer *embed.Etcd
	ctx        context.Context
	cancel     context.CancelFunc

	monitor monitoring.Monitor
}

func NewServer(ctx context.Context, cancel context.CancelFunc) (*Server, error) {

	conf := entity.Conf()

	log.Regist(cblog.NewBaudLog(conf.Global.Log, "master", conf.Global.Level, strings.EqualFold(conf.Global.Level, "debug")))

	if err := log.RegistLog(client.DefLog, cblog.NewBaudLog(conf.Global.Log, "dscli", conf.Global.Level, strings.EqualFold(conf.Global.Level, "debug"))); err != nil {
		panic(err)
	}

	//Logically, this code should not be executed, because if the local master is not found, it will panic
	if conf.Masters.Self() == nil {
		return nil, fmt.Errorf("master not init please your address or master name ")
	}

	cfg, err := conf.GetEmbed()

	if err != nil {
		return nil, err
	}

	if err := os.MkdirAll(cfg.Dir, os.ModePerm); err != nil {
		return nil, err
	}

	return &Server{etcCfg: cfg, ctx: ctx, cancel: cancel}, nil
}

func (s *Server) Start() (err error) {

	//start etc server
	s.etcdServer, err = embed.StartEtcd(s.etcCfg)
	if err != nil {
		log.Error(err.Error())
		return err
	}

	defer s.etcdServer.Close()

	select {
	case <-s.etcdServer.Server.ReadyNotify():
		log.Info("Server is ready!")
	case <-time.After(60 * time.Second):
		s.etcdServer.Server.Stop() // trigger a shutdown
		log.Error("Server took too long to start!")
		return fmt.Errorf("etcd start timeout")
	case <-s.ctx.Done():
		log.Info("master context is cancel")
	}

	// start http server
	gin.SetMode(gin.ReleaseMode)
	engine := gin.Default()

	baseService, err := service.NewBaseService()
	if err != nil {
		panic(err)
	}

	ExportToClusterHandler(engine, baseService, s.monitor)
	ExportToHealthHandler(engine, baseService, s.monitor)
	ExportToNodeHandler(engine, baseService, s.monitor)
	ExportToRangeHandler(engine, baseService, s.monitor)

	go func() {
		if err := engine.Run(":" + cast.ToString(entity.Conf().Masters.Self().ApiPort)); err != nil {
			panic(err)
		}
	}()

	schedule.StartScheduleJob(s.ctx, baseService)

	return <-s.etcdServer.Err()
}

func (s *Server) Stop() {
	log.Info("master shutdown... start")
	s.etcdServer.Server.Stop()
	s.cancel()
	log.Info("master shutdown... end")
}
