package master

import (
	"bytes"
	"context"
	"fmt"
	"github.com/chubaodb/chubaodb/master/entity"
	"github.com/chubaodb/chubaodb/master/entity/errs"
	"github.com/chubaodb/chubaodb/master/entity/pkg/mspb"
	"github.com/chubaodb/chubaodb/master/service"
	"github.com/chubaodb/chubaodb/master/utils/ginutil"
	"github.com/chubaodb/chubaodb/master/utils/log"
	"github.com/gin-gonic/gin"
	"io/ioutil"
	"net/http"
)

type watcherApi struct {
	router  *gin.Engine
	service *service.WatcherService
}

func ExportToWatcherHandler(router *gin.Engine, bs *service.BaseService) *service.WatcherService {

	watcherService := service.NewWatcherService(bs)

	watcherService.StartWatcher()
	c := &watcherApi{router: router, service: watcherService}

	//database handler
	base60 := newBaseHandler(60, entity.Monitor())
	router.Handle(http.MethodPost, "/watcher", base60.TimeOutHandler, base60.PaincHandler(c.watcher), base60.TimeOutEndHandler)
	return watcherService

}

// watcher event by etcd , it will block 10`s when no result
func (wa *watcherApi) watcher(c *gin.Context) {
	ctx, _ := c.Get(Ctx)

	req := &mspb.WatcherEventRequest{}
	if err := bind(c, req); err != nil {
		ginutil.NewAutoMehtodName(c, entity.Monitor()).Send(&mspb.WatcherEventResponse{Header: entity.Err(err)})
		return
	}

	if entity.Conf().Masters.Self().Name == "" {
		ginutil.NewAutoMehtodName(c, entity.Monitor()).Send(&mspb.WatcherEventResponse{Header: entity.Err(fmt.Errorf("master param can not empty string"))})
		return
	}

	if entity.Conf().Masters.Self().Name != req.Master {
		if resp, err := wa.redirect(c, req); err != nil {
			ginutil.NewAutoMehtodName(c, entity.Monitor()).Send(&mspb.WatcherEventResponse{Header: entity.Err(err)})
		} else {
			ginutil.NewAutoMehtodName(c, entity.Monitor()).Send(resp)
		}
		return
	}

	events := wa.service.Watcher(ctx.(context.Context), req.Version)

	ginutil.NewAutoMehtodName(c, entity.Monitor()).Send(&mspb.WatcherEventResponse{Header: entity.OK(), Events: events})
}

func (wa *watcherApi) redirect(c *gin.Context, request *mspb.WatcherEventRequest) (*mspb.WatcherEventResponse, error) {
	var masterCfg *entity.MasterCfg
	for _, m := range entity.Conf().Masters {
		if m.Name == request.Master {
			masterCfg = m
		}
	}

	if masterCfg == nil {
		return nil, fmt.Errorf("can not found master by name:[%s]", request.Master)
	}

	dAtA, err := request.Marshal()
	if err != nil {
		return nil, err
	}

	resp, err := http.Post(masterCfg.ApiUrl()+"/watcher", ProtoContentType, bytes.NewReader(dAtA))
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		log.Error("redirect to master:[%s] status code not 200 is :[%d]", request.Master, resp.StatusCode)
		return nil, errs.Error(mspb.ErrorType_WatcherMasterHashErr)
	}

	all, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	response := &mspb.WatcherEventResponse{}

	if err := response.Unmarshal(all); err != nil {
		return nil, err
	}

	return response, nil

}
