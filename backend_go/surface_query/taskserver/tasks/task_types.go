package tasks

const (
	TaskType_SampleInPoints = "sample_in_points"
	TaskType_Dummy          = "dummy"
)

var AllTaskTypeStrings = []string{
	TaskType_SampleInPoints,
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
