package main

import (
	"pegasus/util"
	"time"
)

const (
	MONITOR_INTERVAL = 1 * time.Second
)

func monitorMain(args interface{}) {
	reportTaskStatus()
}

func startMonitor() {
	go util.PeriodicalRoutine(true, MONITOR_INTERVAL, monitorMain, nil)
}

func init() {
	startMonitor()
}
