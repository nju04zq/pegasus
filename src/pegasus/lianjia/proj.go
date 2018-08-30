package lianjia

import (
	"encoding/json"
	"fmt"
	"pegasus/log"
	"pegasus/rate"
	"pegasus/task"
)

const (
	PROJ_LIANJIA = "Lianjia-Crawler"

	UPDATE_HISTORY_TABLE_NAME = "update_history"
)

type ProjLianjiaConf struct {
	Districts map[string][]string
}

type ProjLianjiaEnv struct {
	Conf    *ProjLianjiaConf
	regions map[string]*Region
}

func (env *ProjLianjiaEnv) init() *ProjLianjiaEnv {
	env.Conf = new(ProjLianjiaConf)
	env.regions = make(map[string]*Region)
	return env
}

type ProjLianjia struct {
	err  error
	jobs []task.Job
	env  *ProjLianjiaEnv
}

func (proj *ProjLianjia) Init(config string) error {
	proj.env = new(ProjLianjiaEnv).init()
	if err := json.Unmarshal([]byte(config), proj.env.Conf); err != nil {
		return fmt.Errorf("Fail to unmarshal project config, %v", err)
	}
	proj.InitJobs()
	log.Info("Target districts %v", proj.env.Conf.Districts)
	return nil
}

func (proj *ProjLianjia) InitJobs() {
	j0 := new(JobDistricts)
	j1 := new(JobRegions)
	j2 := new(JobRegionMaxpage)
	j3 := new(JobGetApartments)
	j4 := new(JobUpdateDb)
	j0.nextJobs = []*JobRegions{j1}
	j1.nextJobs = []*JobRegionMaxpage{j2}
	j2.nextJobs = []*JobGetApartments{j3}
	j3.nextJobs = []*JobUpdateDb{j4}
	proj.jobs = []task.Job{j0, j1, j2, j3, j4}
}

func (proj *ProjLianjia) GetEnv() interface{} {
	return proj.env
}

func (proj *ProjLianjia) GetName() string {
	return PROJ_LIANJIA
}

func (proj *ProjLianjia) GetJobs() []task.Job {
	return proj.jobs
}

func (proj *ProjLianjia) SetErr(err error) {
	proj.err = err
}

func (proj *ProjLianjia) GetErr() error {
	return proj.err
}

func (proj *ProjLianjia) Finish(stats *task.ProjStats) error {
	log.Info(rate.Summary())
	if err := proj.insertUpdateHistory(stats); err != nil {
		log.Error("Fail to insert update history, %v", err)
		return err
	}
	return nil
}

type UpdateHistory struct {
	StartTs int64  `db:"start,size:64"`
	EndTs   int64  `db:"end,size:64"`
	Log     string `db:"log,size:8192"`
	Result  string `db:"result,size:8192"`
}

func (proj *ProjLianjia) insertUpdateHistory(stats *task.ProjStats) error {
	tblName := UPDATE_HISTORY_TABLE_NAME
	dbmap, err := getDbmap()
	if err != nil {
		return err
	}
	defer putDbmap()
	dbmap.AddTableWithName(UpdateHistory{}, tblName)
	if err := dbmap.CreateTablesIfNotExists(); err != nil {
		return err
	}
	logMsg, err := json.Marshal(stats.Series)
	if err != nil {
		return err
	}
	updateDbJob := proj.jobs[len(proj.jobs)-1]
	result, err := json.Marshal(updateDbJob.GetOutput())
	if err != nil {
		return err
	}
	history := &UpdateHistory{
		StartTs: stats.StartTs,
		EndTs:   stats.EndTs,
		Log:     string(logMsg),
		Result:  string(result),
	}
	if err := dbmap.Insert(history); err != nil {
		return err
	}
	return nil
}
