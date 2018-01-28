package taskreg

import (
	"fmt"
	"pegasus/mergesort"
	"pegasus/task"
	"reflect"
)

var projs = make(map[string]reflect.Type)

var taskGens = make(map[string]task.TaskGenerator)

func register(proj task.Project) {
	if err := proj.Init(); err != nil {
		panic(err)
	}
	name := proj.GetName()
	if _, ok := projs[name]; ok {
		panic(fmt.Errorf("proj %q already registered", name))
	}
	projs[name] = reflect.ValueOf(proj).Type()
	if err := registerTasks(proj); err != nil {
		panic(err)
	}
}

func registerTasks(proj task.Project) error {
	for _, job := range proj.GetJobs() {
		kind := job.GetKind()
		if _, ok := taskGens[kind]; ok {
			return fmt.Errorf("Job %q already registered", kind)
		}
		taskGens[kind] = job.GetTaskGen()
	}
	return nil
}

func GetProj(name string) task.Project {
	projType, ok := projs[name]
	if !ok {
		return nil
	}
	return reflect.New(projType).Interface().(task.Project)
}

func GetTaskGenerator(kind string) task.TaskGenerator {
	gen, ok := taskGens[kind]
	if !ok {
		return nil
	}
	return gen
}

func init() {
	register(new(mergesort.ProjMergesort))
}
