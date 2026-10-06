package activity

import (
	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/priorities"
	"go.temporal.io/server/common/retrypolicy"
	"go.temporal.io/server/common/tqid"
	"google.golang.org/protobuf/types/known/durationpb"
)

// ValidateAndNormalizeEmbeddedActivity validates and normalizes the attributes for an embedded activity.
func ValidateAndNormalizeEmbeddedActivity(
	activityID string,
	activityType string,
	defaultActivityRetrySettings retrypolicy.DefaultRetrySettings,
	maxIDLengthLimit int,
	options *activitypb.ActivityOptions,
	priority *commonpb.Priority,
	runTimeout *durationpb.Duration,
	workflowTaskQueueName string,
) error {
	if err := tqid.NormalizeAndValidateUserDefined(options.TaskQueue, "", workflowTaskQueueName, maxIDLengthLimit); err != nil {
		return err
	}

	return validateAndNormalizeActivityAttributes(
		activityID,
		activityType,
		defaultActivityRetrySettings,
		maxIDLengthLimit,
		options,
		priority,
		runTimeout)
}

// validateAndNormalizeActivityAttributes validates and normalizes the common activity request
// attributes. It mutates options.
//
// The timeout normalization logic is as follows:
// 1. If ScheduleToClose is set, fill in missing ScheduleToStart and StartToClose from ScheduleToClose
// 2. If StartToClose is set but ScheduleToClose is not set, set ScheduleToClose to runTimeout, and fill in missing ScheduleToStart from runTimeout
// 3. If neither ScheduleToClose nor StartToClose is set, return error
// 4. Ensure all timeouts do not exceed runTimeout if runTimeout is set (>0)
// 5. Ensure HeartbeatTimeout does not exceed StartToClose
func validateAndNormalizeActivityAttributes(
	activityID string,
	activityType string,
	defaultActivityRetrySettings retrypolicy.DefaultRetrySettings,
	maxIDLengthLimit int,
	options *activitypb.ActivityOptions,
	priority *commonpb.Priority,
	runTimeout *durationpb.Duration,
) error {
	if activityID == "" {
		return serviceerror.NewInvalidArgument("activityId is not set")
	}
	if activityType == "" {
		return serviceerror.NewInvalidArgument("activityType is not set")
	}

	if err := validateActivityRetryPolicy(options.RetryPolicy, defaultActivityRetrySettings); err != nil {
		return err
	}

	if len(activityID) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgumentf("activityId exceeds length limit. Length=%d Limit=%d",
			len(activityID), maxIDLengthLimit)
	}
	if len(activityType) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgumentf("activityType exceeds length limit. Length=%d Limit=%d",
			len(activityType), maxIDLengthLimit)
	}

	if err := priorities.Validate(priority); err != nil {
		return serviceerror.NewInvalidArgumentf("invalid priorities: %v", err)
	}

	return validateAndNormalizeTimeouts(activityID,
		activityType,
		runTimeout,
		options)
}

func validateActivityRetryPolicy(
	retryPolicy *commonpb.RetryPolicy,
	defaultActivityRetrySettings retrypolicy.DefaultRetrySettings,
) error {
	if retryPolicy == nil {
		return nil
	}
	retrypolicy.EnsureDefaults(retryPolicy, defaultActivityRetrySettings)
	return retrypolicy.Validate(retryPolicy)
}

func validateAndNormalizeTimeouts(
	activityID string,
	activityType string,
	runTimeout *durationpb.Duration,
	options *activitypb.ActivityOptions,
) error {
	// Only attempt to deduce and fill in unspecified timeouts only when all timeouts are non-negative.
	if err := timestamp.ValidateAndCapProtoDuration(options.GetScheduleToCloseTimeout()); err != nil {
		return serviceerror.NewInvalidArgumentf("invalid ScheduleToCloseTimeout: %v", err)
	}
	if err := timestamp.ValidateAndCapProtoDuration(options.GetScheduleToStartTimeout()); err != nil {
		return serviceerror.NewInvalidArgumentf("invalid ScheduleToStartTimeout: %v", err)
	}
	if err := timestamp.ValidateAndCapProtoDuration(options.GetStartToCloseTimeout()); err != nil {
		return serviceerror.NewInvalidArgumentf("invalid StartToCloseTimeout: %v", err)
	}
	if err := timestamp.ValidateAndCapProtoDuration(options.GetHeartbeatTimeout()); err != nil {
		return serviceerror.NewInvalidArgumentf("invalid HeartbeatTimeout: %v", err)
	}

	scheduleToCloseSet := options.GetScheduleToCloseTimeout().AsDuration() > 0
	scheduleToStartSet := options.GetScheduleToStartTimeout().AsDuration() > 0
	startToCloseSet := options.GetStartToCloseTimeout().AsDuration() > 0

	if scheduleToCloseSet {
		if scheduleToStartSet {
			options.ScheduleToStartTimeout = timestamp.MinDurationPtr(options.ScheduleToStartTimeout, options.ScheduleToCloseTimeout)
		} else {
			options.ScheduleToStartTimeout = options.ScheduleToCloseTimeout
		}
		if startToCloseSet {
			options.StartToCloseTimeout = timestamp.MinDurationPtr(options.StartToCloseTimeout, options.ScheduleToCloseTimeout)
		} else {
			options.StartToCloseTimeout = options.ScheduleToCloseTimeout
		}
	} else if startToCloseSet {
		// We are in !validScheduleToClose due to the first if above
		options.ScheduleToCloseTimeout = runTimeout
		if !scheduleToStartSet {
			options.ScheduleToStartTimeout = runTimeout
		}
	} else {
		// Deduction failed as there's not enough information to fill in missing timeouts.
		return serviceerror.NewInvalidArgumentf("a valid StartToCloseTimeout or ScheduleToCloseTimeout must be set on the activity. ActivityId=%s ActivityType=%s",
			activityID, activityType)
	}
	// ensure activity timeout never exceeds runTimeout
	if runTimeout.AsDuration() > 0 {
		runTimeoutDur := runTimeout.AsDuration()
		if options.ScheduleToCloseTimeout.AsDuration() > runTimeoutDur {
			options.ScheduleToCloseTimeout = runTimeout
		}
		if options.ScheduleToStartTimeout.AsDuration() > runTimeoutDur {
			options.ScheduleToStartTimeout = runTimeout
		}
		if options.StartToCloseTimeout.AsDuration() > runTimeoutDur {
			options.StartToCloseTimeout = runTimeout
		}
		if options.HeartbeatTimeout.AsDuration() > runTimeoutDur {
			options.HeartbeatTimeout = runTimeout
		}
	}

	options.HeartbeatTimeout = timestamp.MinDurationPtr(options.HeartbeatTimeout, options.StartToCloseTimeout)

	return nil
}
