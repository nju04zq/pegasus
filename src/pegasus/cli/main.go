package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"pegasus/lianjia"
	"pegasus/uri"
	"pegasus/util"
	"strconv"
	"strings"
)

func getMasterAddr() (ip string, port int, err error) {
	url := &util.HttpUrl{
		IP:   "127.0.0.1",
		Port: 10086,
		Uri:  uri.CfgMasterUri,
	}
	addr, err := util.HttpGet(url)
	if err != nil {
		return
	}
	toks := strings.Split(addr, ":")
	if len(toks) != 2 {
		err = fmt.Errorf("master addr %s invalid", addr)
		return
	}
	ip = toks[0]
	port, err = strconv.Atoi(toks[1])
	if err != nil {
		err = fmt.Errorf("master addr %s invalid", addr)
		return
	}
	return
}

type Receipt struct {
	ErrMsg string
	ProjId string
}

func startProj(ip string, port int) (string, error) {
	u := &util.HttpUrl{
		IP:    ip,
		Port:  port,
		Uri:   uri.MasterProjectUri,
		Query: make(url.Values),
	}
	u.Query.Add(uri.MasterProjNameKey, lianjia.PROJ_LIANJIA)
	data := make(map[string]map[string][]string)
	/*
		data := map[string]map[string][]string{
			"districts": {
				"闵行": []string{"梅陇"},
				//"闵行": []string{"七宝", "梅陇", "莘庄", "春申", "古美"},
			},
		}
	*/
	resp, err := util.HttpPostData(u, data)
	if err != nil {
		return "", err
	}
	receipt := new(Receipt)
	if err := json.Unmarshal([]byte(resp), receipt); err != nil {
		return "", fmt.Errorf("Proj receipt with wrong format, %v", err)
	}
	if receipt.ErrMsg != "" {
		return "", errors.New(receipt.ErrMsg)
	}
	fmt.Println("Start project succeed!")
	return receipt.ProjId, nil
}

func main() {
	masterIP, masterPort, err := getMasterAddr()
	if err != nil {
		panic(err)
	}
	projId, err := startProj(masterIP, masterPort)
	if err != nil {
		panic(err)
	}
	mgr := new(progressMgr).init(masterIP, masterPort, projId)
	mgr.run()
}
