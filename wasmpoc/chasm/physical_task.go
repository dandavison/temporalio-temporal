package chasm

import (
	"math"
	"reflect"
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
)

// TaskCategory is the queue a physical task is routed to. The history service maps these onto its
// own task categories; CHASM does not depend on the history service's task types.
type TaskCategory int

const (
	TaskCategoryTransfer TaskCategory = iota
	TaskCategoryTimer
	TaskCategoryOutbound
	TaskCategoryVisibility
)

var maximumPureTaskFireTime = time.Unix(0, math.MaxInt64)

// PhysicalTask is a task emitted by CloseTransaction for the backend to persist and schedule.
type PhysicalTask interface {
	isPhysicalTask()
}

// PhysicalSideEffectTask is the backend's record of one side-effect task.
type PhysicalSideEffectTask struct {
	VisibilityTimestamp time.Time
	Category            TaskCategory
	Destination         string
	Info                *persistencespb.ChasmTaskInfo

	// In-memory only.
	DeserializedTask reflect.Value
	Attempt          int
}

// PhysicalPureTask is the backend's record of the earliest pending pure task of an execution.
type PhysicalPureTask struct {
	VisibilityTimestamp time.Time
	ArchetypeID         uint32
}

func (*PhysicalSideEffectTask) isPhysicalTask() {}
func (*PhysicalPureTask) isPhysicalTask()       {}

// scheduledTaskMinPrecision matches common.ScheduledTaskMinPrecision in the server.
const scheduledTaskMinPrecision = time.Millisecond
