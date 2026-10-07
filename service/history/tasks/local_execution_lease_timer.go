package tasks

import (
	"fmt"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

var _ Task = (*LocalExecutionLeaseTimerTask)(nil)

type (
	// LocalExecutionLeaseTimerTask fires when a local server's lease on a workflow, as renewed by
	// a sync, expires. Each sync that renews the lease adds one; those for leases renewed again
	// since do nothing.
	LocalExecutionLeaseTimerTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64
	}
)

func (t *LocalExecutionLeaseTimerTask) GetKey() Key {
	return NewKey(t.VisibilityTimestamp, t.TaskID)
}

func (t *LocalExecutionLeaseTimerTask) GetTaskID() int64 {
	return t.TaskID
}

func (t *LocalExecutionLeaseTimerTask) SetTaskID(id int64) {
	t.TaskID = id
}

func (t *LocalExecutionLeaseTimerTask) GetVisibilityTime() time.Time {
	return t.VisibilityTimestamp
}

func (t *LocalExecutionLeaseTimerTask) SetVisibilityTime(visibilityTime time.Time) {
	t.VisibilityTimestamp = visibilityTime
}

func (t *LocalExecutionLeaseTimerTask) GetCategory() Category {
	return CategoryTimer
}

func (t *LocalExecutionLeaseTimerTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_LOCAL_EXECUTION_LEASE_TIMER
}

func (t *LocalExecutionLeaseTimerTask) String() string {
	return fmt.Sprintf("LocalExecutionLeaseTimerTask{WorkflowKey: %s, VisibilityTimestamp: %v, TaskID: %v}",
		t.WorkflowKey.String(),
		t.VisibilityTimestamp,
		t.TaskID,
	)
}
