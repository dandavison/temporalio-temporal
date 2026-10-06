package activity

import enumspb "go.temporal.io/api/enums/v1"

// Config holds the settings the activity component reads at runtime. The server populates it from
// dynamic config in its wiring package; the component does not import dynamicconfig.
type Config struct {
	BreakdownMetricsByTaskQueue               func(namespace string, taskQueue string, taskQueueType enumspb.TaskQueueType) bool
	MutableStateActivityFailureSizeLimitError func(namespace string) int
}
