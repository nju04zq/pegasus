package dummypkg

import (
	"pegasus/cfgmgr"
)

type Dummy struct {
	Field1 int    `json: field1`
	Field2 string `json: field2`
}

var DummyCfg = &Dummy{}
var DummyCfgDef = &Dummy{
	Field1: 34,
	Field2: "efg",
}

func RegisterCfg() {
	cfgmgr.RegisterCfgEntry(DummyCfg, DummyCfgDef)
}
