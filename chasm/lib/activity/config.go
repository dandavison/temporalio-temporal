package activity

import enumspb "go.temporal.io/api/enums/v1"

// Config holds the settings that the activity component reads.
type Config struct {
	BreakdownMetricsByTaskQueue               func(namespace string, taskQueue string, taskQueueType enumspb.TaskQueueType) bool
	MutableStateActivityFailureSizeLimitError func(namespace string) int
	StartDelayEnabled                         func(namespace string) bool
}
