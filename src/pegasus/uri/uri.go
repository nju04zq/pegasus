package uri

// URIs for CFG server
const (
	CfgUriRoot                = "/cfg"
	CfgPingUri                = "/ping"
	CfgMasterUri              = "/master"
	CfgEchoIpUri              = "/echoip"
	CfgTestUri                = "/test"
	MasterRegisterWokerUri    = "/worker"
	MasterWorkerHbUri         = "/worker/heartbeat"
	MasterWorkerHbIntervalUri = "/worker/heartbeat/interval"
	MasterWorkerTaskReportUri = "/worker/task/report"
	MasterProjectUri          = "/project"
	MasterTestUri             = "/test"
	WorkerTaskUri             = "/task"
	WorkerTestUri             = "/test"
)

const (
	MasterWorkerQueryKey = "key"
	MasterProjNameKey    = "proj"
)
