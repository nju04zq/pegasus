package lianjia

import (
	"pegasus/db"
	"sync"
	"time"

	"github.com/go-gorp/gorp"
)

const (
	DBMGR_MONITOR_INTERVAL = 30 * time.Second
	DBMAP_ALIVE_INTERVAL   = 2 * time.Minute

	LIANJIA_DBNAME = "lianjia_pegasus"
	//LIANJIA_DBNAME = "lianjia_pegasus_test"
)

type dbMgr struct {
	mutex    sync.Mutex
	lastUse  time.Time
	refcnt   int
	database *db.Database
}

func (mgr *dbMgr) newDatabase() error {
	database, err := db.OpenMysqlDatabase(LIANJIA_DBNAME)
	if err != nil {
		return err
	}
	mgr.database = database
	return nil
}

func (mgr *dbMgr) getDbmap() (*gorp.DbMap, error) {
	mgr.mutex.Lock()
	defer mgr.mutex.Unlock()
	if mgr.database == nil {
		if err := mgr.newDatabase(); err != nil {
			return nil, err
		}
	}
	mgr.refcnt++
	return mgr.database.GetDbmap(), nil
}

func (mgr *dbMgr) putDbmap() {
	mgr.mutex.Lock()
	defer mgr.mutex.Unlock()
	mgr.refcnt--
	mgr.lastUse = time.Now()
}

func (mgr *dbMgr) monitor() {
	ticker := time.NewTicker(DBMGR_MONITOR_INTERVAL)
	for _ = range ticker.C {
		mgr.checkDbUsage()
	}
}

func (mgr *dbMgr) checkDbUsage() {
	mgr.mutex.Lock()
	defer mgr.mutex.Unlock()
	if mgr.database == nil || mgr.refcnt > 0 {
		return
	}
	deadline := mgr.lastUse.Add(DBMAP_ALIVE_INTERVAL)
	if deadline.After(time.Now()) {
		return
	}
	mgr.database.Close()
	mgr.database = nil
}

var databaseMgr = dbMgr{}

func getDbmap() (*gorp.DbMap, error) {
	return databaseMgr.getDbmap()
}

func putDbmap() {
	databaseMgr.putDbmap()
}

func init() {
	go databaseMgr.monitor()
}
