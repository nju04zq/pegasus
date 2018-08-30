package main

import (
	"encoding/json"
	"net/url"
	"pegasus/log"
	"pegasus/uri"
	"pegasus/util"
	"time"
)

func getHbInterval() (interval time.Duration, err error) {
	u := workerSelf.makeMasterUrl(uri.MasterWorkerHbIntervalUri)
	s, err := util.HttpGet(u)
	if err != nil {
		return
	}
	err = json.Unmarshal([]byte(s), &interval)
	return
}

func hbMain(args interface{}) {
	log.Debug("Post heartbeat...")
	u := args.(*util.HttpUrl)
	ts := time.Now()
	if _, err := util.HttpPostData(u, ts); err != nil {
		log.Error("Fail to post heartbeat, %v", err)
	} else {
		log.Debug("Post heartbeat successfully")
	}
}

func startHb() error {
	interval, err := getHbInterval()
	if err != nil {
		log.Error("Fail to get heartbeat interval, %v", err)
		return err
	}
	u := &util.HttpUrl{
		IP:   workerSelf.masterIp,
		Port: workerSelf.masterPort,
		Uri:  uri.MasterWorkerHbUri,
	}
	u.Query = make(url.Values)
	u.Query.Add(uri.MasterWorkerQueryKey, workerSelf.Key)
	go util.PeriodicalRoutine(false, interval, hbMain, u)
	return nil
}
