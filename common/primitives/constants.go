package primitives

import (
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/debug"
)

const (
	// DefaultLongPollTimeout is the default context timeout for a long poll request.
	DefaultLongPollTimeout = time.Second * 60
	// DefaultLongPollBuffer is the buffer used to adjust a long poll request timeout.
	// Specifically, long poll requests are timed out at a time which leaves at least the buffer's duration
	// remaining before the caller's deadline, if permitted by the caller's deadline.
	DefaultLongPollBuffer = time.Second
)

const (
	// FailureReasonActivityTimeout is failureReason for when an activity times out, with %v as the timeout type.
	FailureReasonActivityTimeout = "activity %v timeout"
	// FailureReasonActivityRetryScheduleToCloseTimeout is failureReason for when an activity retry cannot be scheduled before its schedule-to-close timeout.
	FailureReasonActivityRetryScheduleToCloseTimeout = "Not enough time to schedule next retry before activity ScheduleToClose timeout, giving up retrying"
	// FailureReasonFailureExceedsLimit is failureReason for failure details exceeds limit
	FailureReasonFailureExceedsLimit = "Failure exceeds size limit."
)

// ErrBlobSizeExceedsLimit is error for event blob size exceeds limit
var ErrBlobSizeExceedsLimit = serviceerror.NewInvalidArgument("Blob data size exceeds limit.")

const (
	// ScheduledTaskMinPrecision is the precision of scheduled history task fire times.
	ScheduledTaskMinPrecision = time.Millisecond
)

const (
	// DefaultTransactionSizeLimit is the largest allowed transaction size to persistence
	DefaultTransactionSizeLimit = 4 * 1024 * 1024
)

const (
	// DefaultWorkflowTaskTimeout sets the Default Workflow Task timeout for a Workflow
	DefaultWorkflowTaskTimeout = 10 * time.Second * debug.TimeoutMultiplier
)

const (
	// GetHistoryMaxPageSize is the max page size for get history
	GetHistoryMaxPageSize = 256
	// ReadDLQMessagesPageSize is the max page size for read DLQ messages
	ReadDLQMessagesPageSize = 1000
)

const (
	DefaultHistoryMaxAutoResetPoints = 20
)

const (
	ScheduleWorkflowIDPrefix = "temporal-sys-scheduler:"
)
