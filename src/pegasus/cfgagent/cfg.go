package main

import (
	"fmt"
	"net/http"
	"pegasus/cfgmgr"
	"pegasus/dummypkg"
	"pegasus/log"
	"pegasus/route"
	"pegasus/server"

	"github.com/gorilla/mux"
)

func getCfg(cfgPath string) (interface{}, error) {
	return cfgmgr.GetCfg(cfgPath)
}

func getCfgHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	cfgPath := vars["cfgPath"]
	s, err := getCfg(cfgPath)
	server.FmtResp(w, err, s)
}

func cfgPingHandler(w http.ResponseWriter, r *http.Request) {
	server.FmtResp(w, nil, cfgmgr.PingResp)
}

func registerRoutes() {
	route.RegisterRoute(&route.Route{
		Name:    "getCfgHandler",
		Method:  http.MethodGet,
		Path:    cfgmgr.CfgUriRoot + "{cfgPath}",
		Handler: getCfgHandler,
	})
	route.RegisterRoute(&route.Route{
		Name:    "cfgPingHandler",
		Method:  http.MethodGet,
		Path:    cfgmgr.PingUri,
		Handler: cfgPingHandler,
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
	s := server.Server{
		ListenPort: cfgmgr.CfgServerPort,
	}
	if err := s.ListenAndServe(); err != nil {
		log.Error("Server fault, %v", err)
	}
}
