package lianjia

import (
	"fmt"
	"pegasus/log"
	"pegasus/rate"
	"pegasus/task"
	"pegasus/util"
	"pegasus/workgroup"
	"strings"

	"github.com/anaskhan96/soup"
)

const (
	JOB_KIND_REGIONS  = "Lianjia Crawler: Get regions"
	TASK_KIND_REGIONS = JOB_KIND_REGIONS
)

type Region struct {
	Name    string
	Uri     string
	Abbr    string
	MaxPage int
	Dists   []*District
}

func (r *Region) String() string {
	names := make([]string, len(r.Dists))
	for i, d := range r.Dists {
		names[i] = d.Name
	}
	return fmt.Sprintf("%q, %q, %q, %d, %s",
		r.Name, r.Uri, r.Abbr, r.MaxPage, strings.Join(names, ","))
}

type JobRegions struct {
	env       *ProjLianjiaEnv
	districts []*District
	taskSize  int
	grpSize   int
	nextDist  int
	regions   []*Region
	regionTbl map[string]*Region
	nextJobs  []*JobRegionMaxpage
}

func (job *JobRegions) AppendInput(input interface{}) {
	a := input.([]*District)
	if job.districts == nil {
		job.districts = make([]*District, 0)
	}
	for _, d := range a {
		if d.Abbr != "shanghaizhoubian" {
			job.districts = append(job.districts, d)
		}
	}
}

func (job *JobRegions) Init(env interface{}) error {
	var ok bool
	if job.env, ok = env.(*ProjLianjiaEnv); !ok {
		return fmt.Errorf("Fail to get proj env on init")
	}
	job.taskSize = len(job.districts)
	job.grpSize = workgroup.WgCfg.WorkerExecutorCnt
	if job.grpSize <= 0 {
		return fmt.Errorf("WorkerExecutorCnt <= 0")
	}
	job.regionTbl = make(map[string]*Region)
	return nil
}

func (job *JobRegions) GetKind() string {
	return JOB_KIND_REGIONS
}

func (job *JobRegions) CalcTaskCnt() int {
	return (job.taskSize + job.grpSize - 1) / job.grpSize
}

func (job *JobRegions) GetNextTask(tid string) *task.TaskSpec {
	if job.nextDist >= job.taskSize {
		return nil
	}
	districts := make([]*District, 0, job.grpSize)
	i := job.nextDist
	end := util.Min(i+job.grpSize, job.taskSize)
	for ; i < end; i++ {
		districts = append(districts, job.districts[i])
	}
	spec := &TspecRegions{
		Districts: districts,
	}
	job.nextDist = end
	return &task.TaskSpec{
		Tid:  tid,
		Kind: TASK_KIND_REGIONS,
		Spec: spec,
	}
}

func (job *JobRegions) ReduceTasks(reports []*task.TaskReport) error {
	for _, report := range reports {
		regions := make([]*Region, 0)
		if err := util.FitDataInto(report.Output, &regions); err != nil {
			return err
		}
		for _, r := range regions {
			if len(r.Dists) == 0 {
				log.Error("Get region %s, %s without district", r.Name, r.Abbr)
				continue
			}
			if !job.needRegion(r) {
				continue
			}
			log.Info("Need region %s", r.Name)
			region, ok := job.regionTbl[r.Abbr]
			if ok {
				region.Dists = append(region.Dists, r.Dists...)
			} else {
				job.regions = append(job.regions, r)
				job.regionTbl[r.Abbr] = r
			}
		}
	}
	return nil
}

func (job *JobRegions) needRegion(r *Region) bool {
	dname, target := r.Dists[0].Name, job.env.Conf.Districts
	if len(target) == 0 {
		return true
	}
	rnames, ok := target[dname]
	if !ok {
		return false
	}
	for _, rname := range rnames {
		if rname == r.Name {
			return true
		}
	}
	return false
}

func (job *JobRegions) GetOutput() interface{} {
	return job.regions
}

func (job *JobRegions) GetNextJobs() []task.Job {
	jobs := make([]task.Job, 0, len(job.nextJobs))
	for _, j := range job.nextJobs {
		jobs = append(jobs, j)
	}
	return jobs
}

func (job *JobRegions) GetTaskGen() task.TaskGenerator {
	return TaskGenRegions
}

type TspecRegions struct {
	Districts []*District
}

func TaskGenRegions(tspec *task.TaskSpec) (task.Task, error) {
	tsk := new(taskRegions)
	tsk.tid = tspec.Tid
	tsk.kind = tspec.Kind
	spec := new(TspecRegions)
	task.DecodeSpec(tspec, spec)
	tsk.districts = spec.Districts
	names := make([]string, len(tsk.districts))
	for i, d := range tsk.districts {
		names[i] = d.Name
	}
	tsk.desc = fmt.Sprintf("Regions from %s", strings.Join(names, ","))
	return tsk, nil
}

type taskRegions struct {
	err       error
	tid       string
	kind      string
	desc      string
	districts []*District
	nextDist  int
	totalDist int
	regions   []*Region
}

func (tsk *taskRegions) Init(executorCnt int) error {
	tsk.totalDist = len(tsk.districts)
	return nil
}

func (tsk *taskRegions) NewTaskletCtx() task.TaskletCtx {
	return nil
}

func (tsk *taskRegions) GetTaskId() string {
	return tsk.tid
}

func (tsk *taskRegions) GetKind() string {
	return tsk.kind
}

func (tsk *taskRegions) GetDesc() string {
	return tsk.desc
}

func (tsk *taskRegions) GetTaskletCnt() int {
	return tsk.totalDist
}

func (tsk *taskRegions) GetNextTasklet(taskletid string) task.Tasklet {
	if tsk.nextDist >= tsk.totalDist {
		return nil
	}
	t := &taskletRegions{
		tid:      taskletid,
		district: tsk.districts[tsk.nextDist],
	}
	tsk.nextDist++
	return t
}

func (tsk *taskRegions) ReduceTasklets(tasklets []task.Tasklet) {
	for _, t := range tasklets {
		tasklet := t.(*taskletRegions)
		tsk.regions = append(tsk.regions, tasklet.regions...)
	}
}

func (tsk *taskRegions) SetError(err error) {
	tsk.err = err
}

func (tsk *taskRegions) GetError() error {
	return tsk.err
}

func (tsk *taskRegions) GetOutput() interface{} {
	return tsk.regions
}

type taskletRegions struct {
	tid      string
	district *District
	regions  []*Region
}

func (t *taskletRegions) GetTaskletId() string {
	return t.tid
}

func (t *taskletRegions) parse(resp string) error {
	doc := soup.HTMLParse(resp)
	tags, err := findAll(&doc, 1, 1, "div", "data-role", "ershoufang")
	if err != nil {
		return err
	}
	tags, err = findAll(&tags[0], 2, 2, "div")
	if err != nil {
		return err
	}
	tags, err = findAll(&tags[1], 1, -1, "a")
	if err != nil {
		return err
	}
	for _, tag := range tags {
		uri, err := tagAttr(&tag, "href")
		if err != nil {
			return err
		}
		abbr, err := parseAbbr(uri)
		if err != nil {
			return err
		}
		r := &Region{
			Name:  tag.Text(),
			Uri:   uri,
			Abbr:  abbr,
			Dists: []*District{t.district},
		}
		log.Info("Get region %q, %q", r.Name, r.Uri)
		t.regions = append(t.regions, r)
	}
	return nil
}

func (t *taskletRegions) Execute(ctx task.TaskletCtx) error {
	link := distLink(t.district)
	log.Info("Get regions from link %q", link)
	resp, err := rate.GetHtml(link)
	if err != nil {
		return fmt.Errorf("Fail to get regions from %q, %v", link, err)
	}
	if err := t.parse(resp); err != nil {
		return fmt.Errorf("Fail to get regions from %q, %v", link, err)
	}
	return nil
}
