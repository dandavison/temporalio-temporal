package common

import (
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/util"
	"google.golang.org/protobuf/proto"
)

// WorkflowIDToHistoryShard remains here so that code outside this repository that uses it keeps
// building.
//
// Deprecated: use primitives.WorkflowIDToHistoryShard.
var WorkflowIDToHistoryShard = primitives.WorkflowIDToHistoryShard

// ScheduledTaskMinPrecision remains here so that code outside this repository that uses it keeps
// building.
//
// Deprecated: use primitives.ScheduledTaskMinPrecision.
const ScheduledTaskMinPrecision = primitives.ScheduledTaskMinPrecision

// DefaultLongPollTimeout remains here so that code outside this repository that uses it keeps
// building.
//
// Deprecated: use primitives.DefaultLongPollTimeout.
const DefaultLongPollTimeout = primitives.DefaultLongPollTimeout

// DefaultLongPollBuffer remains here so that code outside this repository that uses it keeps
// building.
//
// Deprecated: use primitives.DefaultLongPollBuffer.
const DefaultLongPollBuffer = primitives.DefaultLongPollBuffer

// FailureReasonActivityTimeout remains here so that code outside this repository that uses it keeps
// building.
//
// Deprecated: use primitives.FailureReasonActivityTimeout.
const FailureReasonActivityTimeout = primitives.FailureReasonActivityTimeout

// FailureReasonActivityRetryScheduleToCloseTimeout remains here so that code outside this repository that uses it keeps
// building.
//
// Deprecated: use primitives.FailureReasonActivityRetryScheduleToCloseTimeout.
const FailureReasonActivityRetryScheduleToCloseTimeout = primitives.FailureReasonActivityRetryScheduleToCloseTimeout

// FailureReasonFailureExceedsLimit remains here so that code outside this repository that uses it keeps
// building.
//
// Deprecated: use primitives.FailureReasonFailureExceedsLimit.
const FailureReasonFailureExceedsLimit = primitives.FailureReasonFailureExceedsLimit

// ErrBlobSizeExceedsLimit remains here so that code outside this repository that uses it keeps
// building.
//
// Deprecated: use primitives.ErrBlobSizeExceedsLimit.
var ErrBlobSizeExceedsLimit = primitives.ErrBlobSizeExceedsLimit

// CloneProtoMap remains here so that code outside this repository that uses it keeps
// building.
//
// Deprecated: use util.CloneProtoMap.
func CloneProtoMap[K comparable, T proto.Message](src map[K]T) map[K]T {
	return util.CloneProtoMap(src)
}

// CloneProtoSlice remains here so that code outside this repository that uses it keeps
// building.
//
// Deprecated: use util.CloneProtoSlice.
func CloneProtoSlice[T proto.Message](src []T) []T {
	return util.CloneProtoSlice(src)
}
