package lianjia

import (
	"fmt"
	"pegasus/log"
	"pegasus/rate"
	"pegasus/task"
	"pegasus/util"
	"pegasus/workgroup"
	"regexp"
	"sort"
	"strconv"

	"github.com/anaskhan96/soup"
)

const (
	JOB_KIND_REGION_MAXPAGE  = "Lianjia Crawler: Get region maxpage"
	TASK_KIND_REGION_MAXPAGE = JOB_KIND_REGION_MAXPAGE
)

type JobRegionMaxpage struct {
	regions    []*Region
	regionTbl  map[string]*Region
	taskSize   int
	grpSize    int
	nextRegion int
	totalPages int
	nextJobs   []*JobGetApartments
}

func (job *JobRegionMaxpage) AppendInput(input interface{}) {
	a := input.([]*Region)
	job.regions = a
}

func (job *JobRegionMaxpage) Init(env interface{}) error {
	job.taskSize = len(job.regions)
	job.grpSize = workgroup.WgCfg.WorkerExecutorCnt
	if job.grpSize <= 0 {
		return fmt.Errorf("WorkerExecutorCnt <= 0")
	}
	job.regionTbl = make(map[string]*Region, len(job.regions))
	for _, r := range job.regions {
		job.regionTbl[r.Abbr] = r
	}
	return nil
}

func (job *JobRegionMaxpage) GetKind() string {
	return JOB_KIND_REGION_MAXPAGE
}

func (job *JobRegionMaxpage) CalcTaskCnt() int {
	return (job.taskSize + job.grpSize - 1) / job.grpSize
}

func (job *JobRegionMaxpage) GetNextTask(tid string) *task.TaskSpec {
	if job.nextRegion >= job.taskSize {
		return nil
	}
	regions := make([]*Region, 0, job.grpSize)
	i := job.nextRegion
	end := util.Min(i+job.grpSize, job.taskSize)
	for ; i < end; i++ {
		regions = append(regions, job.regions[i])
	}
	spec := &TspecRegionMaxpage{
		Regions: regions,
	}
	job.nextRegion = end
	return &task.TaskSpec{
		Tid:  tid,
		Kind: TASK_KIND_REGION_MAXPAGE,
		Spec: spec,
	}
}

func (job *JobRegionMaxpage) ReduceTasks(reports []*task.TaskReport) error {
	for _, report := range reports {
		regions := make([]*Region, 0)
		if err := util.FitDataInto(report.Output, &regions); err != nil {
			return err
		}
		for _, r := range regions {
			region, ok := job.regionTbl[r.Abbr]
			if !ok {
				return fmt.Errorf("Region %q not found on reduce\n%v", r.Name, job.regionTbl)
			}
			region.MaxPage = r.MaxPage
			job.totalPages += r.MaxPage
		}
	}
	return nil
}

func (job *JobRegionMaxpage) GetOutput() interface{} {
	sort.Slice(job.regions, func(i, j int) bool {
		if job.regions[i].MaxPage > job.regions[j].MaxPage {
			return true
		} else {
			return false
		}
	})
	log.Info("Total regions' pages %d", job.totalPages)
	log.Info("Get %d regions as:", len(job.regions))
	for _, r := range job.regions {
		log.Info(r.String())
	}
	return job.regions
}

func (job *JobRegionMaxpage) GetNextJobs() []task.Job {
	jobs := make([]task.Job, 0, len(job.nextJobs))
	for _, j := range job.nextJobs {
		jobs = append(jobs, j)
	}
	return jobs
}

func (job *JobRegionMaxpage) GetTaskGen() task.TaskGenerator {
	return TaskGenRegionMaxpage
}

func (job *JobRegionMaxpage) GetReport() string {
	return fmt.Sprintf("Get %d regions, total pages %d.", len(job.regions), job.totalPages)
}

type TspecRegionMaxpage struct {
	Regions []*Region
}

func TaskGenRegionMaxpage(tspec *task.TaskSpec) (task.Task, error) {
	tsk := new(taskRegionMaxpage)
	tsk.tid = tspec.Tid
	tsk.kind = tspec.Kind
	spec := new(TspecRegionMaxpage)
	task.DecodeSpec(tspec, spec)
	tsk.regions = spec.Regions
	s := make([]string, len(tsk.regions))
	for i, r := range tsk.regions {
		s[i] = r.Name
	}
	tsk.desc = fmt.Sprintf("Region maxpage for %q", s)
	return tsk, nil
}

type taskRegionMaxpage struct {
	err         error
	tid         string
	kind        string
	desc        string
	regions     []*Region
	nextRegion  int
	totalRegion int
}

func (tsk *taskRegionMaxpage) Init(executorCnt int) error {
	tsk.totalRegion = len(tsk.regions)
	return nil
}

func (tsk *taskRegionMaxpage) NewTaskletCtx() task.TaskletCtx {
	return nil
}

func (tsk *taskRegionMaxpage) GetTaskId() string {
	return tsk.tid
}

func (tsk *taskRegionMaxpage) GetKind() string {
	return tsk.kind
}

func (tsk *taskRegionMaxpage) GetDesc() string {
	return tsk.desc
}

func (tsk *taskRegionMaxpage) GetTaskletCnt() int {
	return tsk.totalRegion
}

func (tsk *taskRegionMaxpage) GetNextTasklet(taskletid string) task.Tasklet {
	if tsk.nextRegion >= tsk.totalRegion {
		return nil
	}
	t := &taskletRegionMaxpage{
		tid:    taskletid,
		region: tsk.regions[tsk.nextRegion],
	}
	tsk.nextRegion++
	return t
}

func (tsk *taskRegionMaxpage) ReduceTasklets(tasklets []task.Tasklet) {
	return
}

func (tsk *taskRegionMaxpage) SetError(err error) {
	tsk.err = err
}

func (tsk *taskRegionMaxpage) GetError() error {
	return tsk.err
}

func (tsk *taskRegionMaxpage) GetOutput() interface{} {
	return tsk.regions
}

type taskletRegionMaxpage struct {
	tid    string
	region *Region
}

func (t *taskletRegionMaxpage) GetTaskletId() string {
	return t.tid
}

func (t *taskletRegionMaxpage) Execute(ctx task.TaskletCtx) error {
	link := regionLink(t.region)
	log.Info("Get region maxpage from link %q", link)
	resp, err := rate.GetHtml(link)
	if err != nil {
		return fmt.Errorf("Fail to get region maxpage from %q, %v", link, err)
	}
	//fpath := fmt.Sprintf("/tmp/lianjia/%s", t.region.Abbr)
	//fp, _ := os.OpenFile(fpath, os.O_RDWR|os.O_CREATE, 0755)
	//defer fp.Close()
	//fp.Write([]byte(resp))
	if err := t.parse(resp); err != nil {
		return fmt.Errorf("Fail to get region maxpage from %q, %v", link, err)
	}
	return nil
}

func (t *taskletRegionMaxpage) parse(resp string) error {
	doc := soup.HTMLParse(resp)
	tags, err := findAll(&doc, 0, 1, "div", "class", "page-box house-lst-page-box")
	if err != nil {
		return err
	} else if len(tags) == 0 {
		// no maxpage tag for region without apartments
		//return fmt.Errorf("No maxpage tag found")
		return nil
	}
	log.Info("Pasre region maxpage1")
	data, err := tagAttr(&tags[0], "page-data")
	if err != nil {
		return err
	}
	re := regexp.MustCompile(`"totalPage":(\d+)`)
	res := re.FindStringSubmatch(data)
	if len(res) == 0 {
		return fmt.Errorf("maxpage not found")
	}
	maxpage, err := strconv.ParseInt(res[1], 10, 32)
	if err != nil {
		return fmt.Errorf("maxpage %q not int", res[1])
	}
	t.region.MaxPage = int(maxpage)
	log.Info("Get maxpage for %q as %d", t.region.Abbr, t.region.MaxPage)
	return nil
}
