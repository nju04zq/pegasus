package main

import (
	"fmt"
	"pegasus/cfgmgr"
	"pegasus/dummypkg"
	"pegasus/log"
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

func main() {
	initLogger()
	cfgmgr.WaitForCfgServerUp(cfgServerIP)
	getCfg()
	//registerCfg()
	//cfgmgr.SaveDefaultCfgToJson("./cfg.json")
	//cfgmgr.LoadCfgFromFile("./cfg.json")
	//cfgmgr.SaveCfgToJson("./cfg_dump.json")
}
