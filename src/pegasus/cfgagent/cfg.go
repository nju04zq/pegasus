package main

import (
	"fmt"
	"net/http"
	"pegasus/cfgmgr"
	"pegasus/dummypkg"
	"pegasus/log"
	"pegasus/route"
	"pegasus/server"
	"pegasus/uri"
	"pegasus/util"

	"github.com/gorilla/mux"
)

func getCfg(cfgPath string) (interface{}, error) {
	return cfgmgr.GetCfg(cfgPath)
}

func getCfgHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	cfgPath := vars["cfgPath"]
	log.Info("Get cfg from %s for %s", r.RemoteAddr, cfgPath)
	s, err := getCfg(cfgPath)
	server.FmtResp(w, err, s)
}

func cfgPingHandler(w http.ResponseWriter, r *http.Request) {
	log.Info("Get ping from %s", r.RemoteAddr)
	server.FmtResp(w, nil, cfgmgr.PingResp)
}

func testPostHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	key := r.Form.Get("key")
	log.Info("Get key as %s", key)
}

func echoIpHandler(w http.ResponseWriter, r *http.Request) {
	log.Info("Echo IP for %s", r.RemoteAddr)
	ip, _, err := util.SplitAddr(r.RemoteAddr)
	server.FmtResp(w, err, ip)
}

func registerRoutes() {
	route.RegisterRoute(&route.Route{
		Name:    "getCfgHandler",
		Method:  http.MethodGet,
		Path:    uri.CfgUriRoot + "{cfgPath}",
		Handler: getCfgHandler,
	})
	route.RegisterRoute(&route.Route{
		Name:    "cfgPingHandler",
		Method:  http.MethodGet,
		Path:    uri.CfgPingUri,
		Handler: cfgPingHandler,
	})
	route.RegisterRoute(&route.Route{
		Name:    "getMasterAddrHandler",
		Method:  http.MethodGet,
		Path:    uri.CfgMasterUri,
		Handler: getMasterAddrHandler,
	})
	route.RegisterRoute(&route.Route{
		Name:    "postMasterHandler",
		Method:  http.MethodPost,
		Path:    uri.CfgMasterUri,
		Handler: postMasterHandler,
	})
	route.RegisterRoute(&route.Route{
		Name:    "testPost",
		Method:  http.MethodPost,
		Path:    uri.CfgTestUri,
		Handler: testPostHandler,
	})
	route.RegisterRoute(&route.Route{
		Name:    "echoIp",
		Method:  http.MethodGet,
		Path:    uri.CfgEchoIpUri,
		Handler: echoIpHandler,
	})
}

func initLogger() error {
	consoleLogger := &log.ConsoleLogger{
		Level: log.LevelInfo,
	}
	if err := log.RegisterLogger(consoleLogger); err != nil {
		return err
	}
	fileLogger := &log.FileLogger{
		Path:       "./temp/cfg.log",
		RotateSize: 1024 * 1024,
		Level:      log.LevelInfo,
	}
	if err := log.RegisterLogger(fileLogger); err != nil {
		return err
	}
	log.Info("=====Start CFG server=====")
	return nil
}

func registerCfg() {
	dummypkg.RegisterCfg()
}

func loadCfgFromFile() {
	if err := cfgmgr.LoadCfgFromFile("./cfg.json"); err != nil {
		panic(err)
	}
}

func main() {
	if err := initLogger(); err != nil {
		panic(fmt.Errorf("Fail to init logger, %v", err))
	}
	registerCfg()
	registerRoutes()
	loadCfgFromFile()
	s := new(server.Server)
	if err := s.ListenAndServe(cfgmgr.CfgServerPort); err != nil {
		log.Error("Server fault, %v", err)
	}
}
