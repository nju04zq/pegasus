package main

import (
	"net/http"
	"pegasus/log"
	"pegasus/server"
	"pegasus/util"
	"sync"
)

type meta struct {
	mutex      sync.Mutex
	masterAddr string
}

var cfgmeta = meta{}

func registerMaster(addr string) (err error) {
	log.Info("Handle register master request as %s", addr)
	cfgmeta.mutex.Lock()
	defer func() {
		cfgmeta.mutex.Unlock()
		if err == nil {
			log.Info("Register master as %s", addr)
		}
	}()
	// TODO comment out for test convenience
	//if cfgmeta.masterAddr != "" {
	//	err = fmt.Errorf("Master already registered as %s", cfgmeta.masterAddr)
	//	return
	//}
	cfgmeta.masterAddr = addr
	return
}

func getMasterAddr() (addr string) {
	cfgmeta.mutex.Lock()
	defer func() {
		cfgmeta.mutex.Unlock()
		log.Info("Get master addr as %s", addr)
	}()
	addr = cfgmeta.masterAddr
	return
}

func getMasterAddrHandler(w http.ResponseWriter, r *http.Request) {
	server.FmtResp(w, nil, getMasterAddr())
}

func postMasterHandler(w http.ResponseWriter, r *http.Request) {
	addr, err := util.HttpReadRequestTextBody(r)
	if err != nil {
		server.FmtResp(w, err, "")
		return
	}
	err = registerMaster(addr)
	server.FmtResp(w, err, "")
}
