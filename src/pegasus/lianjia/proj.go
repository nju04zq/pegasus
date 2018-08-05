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
)

type ProjLianjiaConf struct {
	Districts map[string][]string
}

type ProjLianjiaEnv struct {
	Conf *ProjLianjiaConf
}

func (env *ProjLianjiaEnv) init() *ProjLianjiaEnv {
	env.Conf = new(ProjLianjiaConf)
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

func (proj *ProjLianjia) Finish() error {
	log.Info(rate.Summary())
	return nil
}
