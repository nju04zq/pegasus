package lianjia

import (
	"fmt"
	"pegasus/log"
	"pegasus/task"
	"pegasus/util"

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
	DistUri string
}

func (r *Region) str() string {
	return fmt.Sprintf("%q, %q, %q, %q", r.Name, r.Uri, r.Abbr, r.DistUri)
}

type JobRegions struct {
	districts []*District
	taskSize  int
	nextDist  int
	regions   []*Region
}

func (job *JobRegions) AppendInput(input interface{}) {
	a := input.([]*District)
	if job.districts == nil {
		job.districts = make([]*District, 0)
	}
	for _, d := range a {
		if d.Uri != "/ershoufang/shanghaizhoubian/" {
			job.districts = append(job.districts, d)
		}
	}
}

func (job *JobRegions) Init() error {
	job.taskSize = len(job.districts)
	return nil
}

func (job *JobRegions) GetKind() string {
	return JOB_KIND_REGIONS
}

func (job *JobRegions) CalcTaskCnt() int {
	return job.taskSize
}

func (job *JobRegions) GetNextTask(tid string) *task.TaskSpec {
	if job.nextDist >= job.taskSize {
		return nil
	}
	spec := &tspecRegions{
		DistUri: job.districts[job.nextDist].Uri,
	}
	job.nextDist++
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
		job.regions = append(job.regions, regions...)
	}
	return nil
}

func (job *JobRegions) GetOutput() interface{} {
	log.Info("Get %d regions as:", len(job.regions))
	for _, r := range job.regions {
		log.Info(r.str())
	}
	return nil
}

func (job *JobRegions) GetNextJobs() []task.Job {
	return nil
}

func (job *JobRegions) GetTaskGen() task.TaskGenerator {
	return TaskGenRegions
}

type tspecRegions struct {
	DistUri string
}

func TaskGenRegions(tspec *task.TaskSpec) (task.Task, error) {
	tsk := new(taskRegions)
	tsk.tid = tspec.Tid
	tsk.kind = tspec.Kind
	spec := new(tspecRegions)
	task.DecodeSpec(tspec, spec)
	tsk.distUri = spec.DistUri
	tsk.desc = fmt.Sprintf("Regions %s", tsk.distUri)
	return tsk, nil
}

type taskRegions struct {
	err     error
	tid     string
	kind    string
	desc    string
	distUri string
	done    bool
	regions []*Region
}

func (tsk *taskRegions) Init(executorCnt int) error {
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
	return 1
}

func (tsk *taskRegions) GetNextTasklet(taskletid string) task.Tasklet {
	if tsk.done {
		return nil
	}
	tsk.done = true
	return &taskletRegions{
		distUri: tsk.distUri,
	}
}

func (tsk *taskRegions) ReduceTasklets(tasklets []task.Tasklet) {
	for _, t := range tasklets {
		tasklet := t.(*taskletRegions)
		tsk.regions = tasklet.regions
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
	tid     string
	distUri string
	regions []*Region
}

func (t *taskletRegions) GetTaskletId() string {
	return t.tid
}

func (t *taskletRegions) Execute(ctx task.TaskletCtx) error {
	link := distLink(t.distUri)
	log.Info("Get regions from link %q", link)
	resp, err := soup.Get(link)
	if err != nil {
		return fmt.Errorf("Fail to get from %q, %v", link, err)
	}
	doc := soup.HTMLParse(resp)
	tags := doc.FindAll("div", "data-role", "ershoufang")
	if len(tags) == 0 {
		return fmt.Errorf("No districts detected!")
	} else if len(tags) > 1 {
		return fmt.Errorf("Too many <div data-role> found!")
	}
	tags = tags[0].FindAll("div")
	if len(tags) == 0 {
		return fmt.Errorf("No districts <div> detected!")
	} else if len(tags) != 2 {
		return fmt.Errorf("districts <div> mismatch count 2!")
	}
	tags = tags[1].FindAll("a")
	if len(tags) == 0 {
		return fmt.Errorf("No regions found for %q", link)
	}
	for _, tag := range tags {
		r := &Region{
			Name:    tag.Text(),
			Uri:     tag.Attrs()["href"],
			Abbr:    "",
			DistUri: t.distUri,
		}
		log.Info("Get region %q, %q", r.Name, r.Uri)
		t.regions = append(t.regions, r)
	}
	return nil
}
