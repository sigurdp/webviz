package tasks

const (
	TaskType_SampleInPoints       = "sample_in_points"
	TaskType_BatchSamplePointSets = "batch_sample_point_sets"
	TaskType_Dummy                = "dummy"
)

var AllTaskTypeStrings = []string{
	TaskType_SampleInPoints,
	TaskType_BatchSamplePointSets,
	TaskType_Dummy,
}

func IsValidTaskType(taskTypeString string) bool {
	for _, t := range AllTaskTypeStrings {
		if taskTypeString == t {
			return true
		}
	}

	return false
}
