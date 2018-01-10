package main

import (
	"fmt"
	"net/url"
	"pegasus/cfgmgr"
	"pegasus/dummypkg"
	"pegasus/log"
	"pegasus/uri"
	"pegasus/util"
)

var cfgServerIP = "127.0.0.1"

func initLogger() error {
	consoleLogger := &log.ConsoleLogger{
		Level: log.LevelInfo,
	}
	if err := log.RegisterLogger(consoleLogger); err != nil {
		return err
	}
	log.Info("=====Start CFG server=====")
	return nil
}

//func registerCfg() {
//	dummypkg.RegisterCfg()
//}

func getCfg() {
	c := dummypkg.DummyCfg
	cfgmgr.PullCfg(cfgServerIP, dummypkg.DummyCfg)
	fmt.Printf("DummyCfg, Field1 %d, Field2 %s\n", c.Field1, c.Field2)
}

func registerAsMaster() {
	url := &util.HttpUrl{
		IP:   cfgServerIP,
		Port: 10086,
		Uri:  uri.CfgMasterUri,
	}
	if _, err := util.HttpPostStr(url, ""); err != nil {
		log.Error("Fail to register as master, %v", err)
	} else {
		log.Info("Register as master succeed!")
	}
}

func getMasterAddr() string {
	url := &util.HttpUrl{
		IP:   cfgServerIP,
		Port: 10086,
		Uri:  uri.CfgMasterUri,
	}
	if s, err := util.HttpGet(url); err != nil {
		log.Error("Fail to get master addr, %v", err)
		return ""
	} else {
		log.Info("Get master addr as %s", s)
		return s
	}
}

func postData() {
	u := &util.HttpUrl{
		IP:   cfgServerIP,
		Port: 10086,
		Uri:  uri.CfgTestUri,
	}
	u.Query = make(url.Values)
	u.Query.Add("key", "12345")
	util.HttpPostStr(u, "")
}

func echoIp() {
	u := &util.HttpUrl{
		IP:   cfgServerIP,
		Port: 10086,
		Uri:  uri.CfgEchoIpUri,
	}
	if s, err := util.HttpGet(u); err != nil {
		log.Error("Fail to echo ip, %v", err)
	} else {
		log.Info("Get local ip as %s", s)
	}
}

func main() {
	initLogger()
	cfgmgr.WaitForCfgServerUp(cfgServerIP)
	registerAsMaster()
	getMasterAddr()
	postData()
	echoIp()
	//registerCfg()
	//cfgmgr.SaveDefaultCfgToJson("./cfg.json")
	//cfgmgr.LoadCfgFromFile("./cfg.json")
	//cfgmgr.SaveCfgToJson("./cfg_dump.json")
}
