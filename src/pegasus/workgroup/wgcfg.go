package workgroup

import (
	"pegasus/cfgmgr"
	"pegasus/log"
)

type WorkgroupCfg struct {
	DataPath          string
	LogPath           string
	WorkerExecutorCnt int
}

var WgCfg = new(WorkgroupCfg)
var WgCfgDef = &WorkgroupCfg{
	DataPath:          "/tmp",
	LogPath:           "/tmp",
	WorkerExecutorCnt: 2,
}

func RegisterCfg() {
	cfgmgr.RegisterCfgEntry(WgCfg, WgCfgDef)
}

func InitWorkgroup(cfgserver string) error {
	if err := cfgmgr.PullCfg(cfgserver, WgCfg); err != nil {
		return err
	}
	log.Info("workgroup cfg %v", WgCfg)
	return nil
}
