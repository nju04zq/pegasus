package cfgmgr

import (
	"fmt"
	"pegasus/log"
	"pegasus/uri"
	"pegasus/util"
	"time"
)

const (
	CfgServerPort = 10086
	PingResp      = "Pong"
)

func pingCfgServer(ip string) error {
	u := &util.HttpUrl{
		IP:   ip,
		Port: CfgServerPort,
		Uri:  uri.CfgPingUri,
	}
	s, err := util.HttpGet(u)
	if err != nil {
		log.Info("Fail to ping %s, %v", u.String(), err)
		return err
	}
	if s != PingResp {
		return fmt.Errorf("Ping failed, get %q, expect %q", s, PingResp)
	}
	log.Info("Ping cfg server %s:%d succeed!", u.IP, u.Port)
	return nil
}

func WaitForCfgServerUp(ip string) {
	sleepTime, maxSleepTime := time.Second, 32*time.Second
	for {
		if err := pingCfgServer(ip); err == nil {
			return
		}
		log.Info("Cfg server not up, wait for %v to retry", sleepTime)
		time.Sleep(sleepTime)
		sleepTime *= 2
		if sleepTime > maxSleepTime {
			sleepTime = maxSleepTime
		}
	}
}

func DiscoverIpFromCfg(ip string) (string, error) {
	u := &util.HttpUrl{
		IP:   ip,
		Port: CfgServerPort,
		Uri:  uri.CfgEchoIpUri,
	}
	s, err := util.HttpGet(u)
	if err != nil {
		log.Error("Fail to echo ip, %v", err)
	}
	return s, err
}
