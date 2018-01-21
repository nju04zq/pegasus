package main

import (
	"fmt"
	"net/http"
	"net/url"
	"pegasus/cfgmgr"
	"pegasus/log"
	"pegasus/route"
	"pegasus/server"
	"pegasus/uri"
	"pegasus/util"
	"time"
)

var cfgServerIP = "127.0.0.1"

type Worker struct {
	Ip           string
	ListenPort   int
	Key          string
	workerServer *server.Server
	workerAddr   string
	masterIp     string
	masterPort   int
}

var workerSelf = new(Worker)

func waitForMasterReady() {
	log.Info("Wait for master ready")
	var ip string
	var port int
	url := &util.HttpUrl{
		IP:   cfgServerIP,
		Port: cfgmgr.CfgServerPort,
		Uri:  uri.CfgMasterUri,
	}
	sleepTime := 5 * time.Second
	for {
		addr, err := util.HttpGet(url)
		if err == nil && addr != "" {
			ip, port, err = util.SplitAddr(addr)
			if err != nil {
				log.Error("Fail to split master addr %q, %v", addr, err)
			} else {
				break
			}
		} else if err != nil {
			log.Error("Fail to get master addr, %v", err)
		} else {
			log.Info("Master not ready")
		}
		ip, port, err = util.SplitAddr(addr)
		if err == nil {
			break
		} else {
			log.Error("Fail to split addr %q, err %v", addr, err)
		}
		time.Sleep(sleepTime)
	}
	workerSelf.masterIp = ip
	workerSelf.masterPort = port
	log.Info("Get master addr as %s:%d", ip, port)
}

func discoverIp() error {
	log.Info("Discover self ip address")
	ip, err := cfgmgr.DiscoverIpFromCfg(cfgServerIP)
	if err != nil {
		return err
	}
	workerSelf.Ip = ip
	log.Info("Discover self ip address as %s", ip)
	return nil
}

func prepareNetwork() error {
	log.Info("Prepare network stuff")
	s := new(server.Server)
	if err := discoverIp(); err != nil {
		return err
	}
	if err := s.Listen(workerSelf.Ip); err != nil {
		return fmt.Errorf("Fail to listen, %v", err)
	}
	workerSelf.workerServer = s
	workerSelf.workerAddr = s.GetListenAddr()
	_, port, err := util.SplitAddr(workerSelf.workerAddr)
	if err != nil {
		return err
	}
	workerSelf.ListenPort = port
	log.Info("Listen on %d", workerSelf.ListenPort)
	return nil
}

func registerOnMaster() (err error) {
	log.Info("Register on master")
	u := &util.HttpUrl{
		IP:   workerSelf.masterIp,
		Port: workerSelf.masterPort,
		Uri:  uri.MasterRegisterWokerUri,
	}
	sleepTime := 5 * time.Second
	var key string
	for {
		key, err = util.HttpGet(u)
		if err == nil {
			break
		}
		log.Error("Fail to register/get key, %v", err)
		time.Sleep(sleepTime)
	}
	log.Info("Get worker's key as %s", key)
	u.Query = make(url.Values)
	u.Query.Add(uri.MasterWorkerQueryKey, key)
	_, err = util.HttpPostStr(u, workerSelf.workerAddr)
	if err != nil {
		return fmt.Errorf("Fail to verify, %v", err)
	}
	workerSelf.Key = key
	log.Info("Register on master done")
	return
}

func testHandler(w http.ResponseWriter, r *http.Request) {
	log.Info("Handle test request")
	s, err := util.HttpReadRequestTextBody(r)
	log.Info("Handle test request, get %q, err %v", s, err)
	server.FmtResp(w, err, s)
	log.Info("Handle test request done")
}

func registerRoutes() {
	route.RegisterRoute(&route.Route{
		Name:    "taskRecipiantHandler",
		Method:  http.MethodPost,
		Path:    uri.WorkerTaskUri,
		Handler: taskRecipiantHandler,
	})
	route.RegisterRoute(&route.Route{
		Name:    "testHandler",
		Method:  http.MethodPost,
		Path:    uri.WorkerTestUri,
		Handler: testHandler,
	})
}

func initLogger() error {
	consoleLogger := &log.ConsoleLogger{
		Level: log.LevelInfo,
	}
	if err := log.RegisterLogger(consoleLogger); err != nil {
		return err
	}
	log.Info("=====Start Worker server=====")
	return nil
}

func main() {
	if err := initLogger(); err != nil {
		panic(fmt.Errorf("Fail to init logger, %v", err))
	}
	registerRoutes()
	cfgmgr.WaitForCfgServerUp(cfgServerIP)
	waitForMasterReady()
	if err := prepareNetwork(); err != nil {
		panic(err)
	}
	if err := registerOnMaster(); err != nil {
		panic(err)
	}
	if err := startHb(); err != nil {
		panic(err)
	}
	panic(workerSelf.workerServer.Serve())
}
