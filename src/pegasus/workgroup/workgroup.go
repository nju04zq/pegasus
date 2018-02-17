package workgroup

import "pegasus/cfgmgr"

type WorkerRegForm struct {
	Name string
	IP   string
	Port int
}

type WorkgroupCfg struct {
	DataPath string
	LogPath  string
}

var WgCfg = new(WorkgroupCfg)
var WgCfgDef = &WorkgroupCfg{
	DataPath: "/tmp",
	LogPath:  "/tmp",
}

func RegisterCfg() {
	cfgmgr.RegisterCfgEntry(WgCfg, WgCfgDef)
}
