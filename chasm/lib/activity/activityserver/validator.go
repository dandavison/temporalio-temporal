package activityserver

import (
	"strings"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/priorities"
	"go.temporal.io/server/common/retrypolicy"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/tqid"
	"go.temporal.io/server/common/util"
	"google.golang.org/protobuf/types/known/durationpb"
)

func validateAndNormalizeIDPolicy(req *workflowservice.StartActivityExecutionRequest) error {
	if req.GetIdReusePolicy() == enumspb.ACTIVITY_ID_REUSE_POLICY_UNSPECIFIED {
		req.IdReusePolicy = enumspb.ACTIVITY_ID_REUSE_POLICY_ALLOW_DUPLICATE
	}

	if req.GetIdConflictPolicy() == enumspb.ACTIVITY_ID_CONFLICT_POLICY_UNSPECIFIED {
		req.IdConflictPolicy = enumspb.ACTIVITY_ID_CONFLICT_POLICY_FAIL
	}

	return nil
}

// validateOnConflictOptions validates the on_conflict_options of a start request:
//   - attach_completion_callbacks requires attach_request_id. A completion callback is recorded
//     against the request ID (see addCompletionCallbacks, which keys the callback by request ID).
//   - attach_request_id requires at least one completion callback or link, since attaching a
//     request ID is only meaningful alongside something to attach.
//
// attach_links is independent and may be set on its own.
func validateOnConflictOptions(req *workflowservice.StartActivityExecutionRequest) error {
	onConflictOptions := req.GetOnConflictOptions()
	if onConflictOptions == nil {
		return nil
	}
	if onConflictOptions.GetAttachCompletionCallbacks() && !onConflictOptions.GetAttachRequestId() {
		return serviceerror.NewInvalidArgument(
			"on_conflict_options: attach_completion_callbacks requires attach_request_id to be set")
	}
	if onConflictOptions.GetAttachRequestId() &&
		len(req.GetCompletionCallbacks()) == 0 &&
		len(req.GetLinks()) == 0 {
		return serviceerror.NewInvalidArgument(
			"on_conflict_options: attach_request_id requires at least one completion callback or link")
	}
	return nil
}

func validateBlobSize(
	activityID string,
	blobSizeViolationTagValue string,
	blobSizeLimitError dynamicconfig.IntPropertyFnWithNamespaceFilter,
	blobSizeLimitWarn dynamicconfig.IntPropertyFnWithNamespaceFilter,
	blobSize int,
	logger log.Logger,
	namespaceName string,
) error {
	sizeWarnLimit := blobSizeLimitWarn(namespaceName)
	sizeErrorLimit := blobSizeLimitError(namespaceName)

	if blobSize > sizeWarnLimit {
		logger.Warn("Activity blob size exceeds the warning limit.",
			tag.WorkflowNamespace(namespaceName),
			tag.ActivityID(activityID),
			tag.ActivitySize(int64(blobSize)),
			tag.BlobSizeViolationOperation(blobSizeViolationTagValue))
	}

	if blobSize > sizeErrorLimit {
		return common.ErrBlobSizeExceedsLimit
	}

	return nil
}

func validateAndNormalizeSearchAttributes(
	req *workflowservice.StartActivityExecutionRequest,
	saMapperProvider searchattribute.MapperProvider,
	saValidator *searchattribute.Validator,
) error {
	namespaceName := req.GetNamespace()

	// Unalias search attributes for validation.
	saToValidate := req.SearchAttributes
	if saMapperProvider != nil && saToValidate != nil {
		var err error
		saToValidate, err = searchattribute.UnaliasFields(saMapperProvider, saToValidate, namespaceName)
		if err != nil {
			return err
		}
	}

	if err := saValidator.Validate(saToValidate, namespaceName); err != nil {
		return err
	}

	return saValidator.ValidateSize(saToValidate, namespaceName)
}

func validateAndNormalizeDescribeActivityExecutionRequest(
	req *workflowservice.DescribeActivityExecutionRequest,
	maxIDLengthLimit int,
) error {
	if req.GetActivityId() == "" {
		return serviceerror.NewInvalidArgument("activity ID is required")
	}
	if len(req.GetActivityId()) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgumentf("activity ID exceeds length limit. Length=%d Limit=%d",
			len(req.GetActivityId()), maxIDLengthLimit)
	}
	hasRunID := req.GetRunId() != ""
	hasLongPollToken := len(req.GetLongPollToken()) > 0

	if hasLongPollToken && !hasRunID {
		return serviceerror.NewInvalidArgument("run id is required when long poll token is provided")
	}
	if hasRunID {
		_, err := uuid.Parse(req.GetRunId())
		if err != nil {
			return serviceerror.NewInvalidArgument("invalid run id: must be a valid UUID")
		}
	}
	return nil
}

func validateAndNormalizePollActivityExecutionRequest(
	req *workflowservice.PollActivityExecutionRequest,
	maxIDLengthLimit int,
) error {
	if req.GetActivityId() == "" {
		return serviceerror.NewInvalidArgument("activity ID is required")
	}
	if len(req.GetActivityId()) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgumentf("activity ID exceeds length limit. Length=%d Limit=%d",
			len(req.GetActivityId()), maxIDLengthLimit)
	}
	if runID := req.GetRunId(); runID != "" {
		_, err := uuid.Parse(runID)
		if err != nil {
			return serviceerror.NewInvalidArgument("invalid run id: must be a valid UUID")
		}
	}
	return nil
}

func validateAndNormalizeStartRequest(
	req *workflowservice.StartActivityExecutionRequest,
	config *Config,
	logger log.Logger,
	saMapperProvider searchattribute.MapperProvider,
	saValidator *searchattribute.Validator,
) error {
	maxIDLengthLimit := config.MaxIDLengthLimit()
	if len(req.GetRequestId()) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgumentf("request ID exceeds length limit. Length=%d Limit=%d",
			len(req.GetRequestId()), maxIDLengthLimit)
	}

	if len(req.GetIdentity()) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgumentf("identity exceeds length limit. Length=%d Limit=%d",
			len(req.GetIdentity()), maxIDLengthLimit)
	}

	if err := validateAndNormalizeIDPolicy(req); err != nil {
		return err
	}

	if err := validateBlobSize(
		req.GetActivityId(),
		"StartActivityExecution",
		config.BlobSizeLimitError,
		config.BlobSizeLimitWarn,
		req.Input.Size(),
		logger,
		req.GetNamespace()); err != nil {
		return serviceerror.NewInvalidArgument("input exceeds length limit")
	}
	if err := validateUserMetadata(
		req.GetNamespace(),
		req.GetUserMetadata(),
		config,
	); err != nil {
		return err
	}

	if req.GetSearchAttributes() != nil {
		if err := validateAndNormalizeSearchAttributes(req, saMapperProvider, saValidator); err != nil {
			return err
		}
	}

	return nil
}

func validateUserMetadata(
	namespaceName string,
	metadata *sdkpb.UserMetadata,
	config *Config,
) error {
	summarySize := metadata.GetSummary().Size()
	if limit := config.MaxUserMetadataSummarySize(namespaceName); summarySize > limit {
		return serviceerror.NewInvalidArgumentf(
			"user_metadata.summary exceeds size limit. Length=%d Limit=%d", summarySize, limit)
	}
	detailsSize := metadata.GetDetails().Size()
	if limit := config.MaxUserMetadataDetailsSize(namespaceName); detailsSize > limit {
		return serviceerror.NewInvalidArgumentf(
			"user_metadata.details exceeds size limit. Length=%d Limit=%d", detailsSize, limit)
	}
	return nil
}

func validateAndNormalizeRequestCancelActivityExecutionRequest(
	req *workflowservice.RequestCancelActivityExecutionRequest,
	maxIDLengthLimit int,
	blobSizeLimitError dynamicconfig.IntPropertyFnWithNamespaceFilter,
	blobSizeLimitWarn dynamicconfig.IntPropertyFnWithNamespaceFilter,
	logger log.Logger,
) error {
	if req.GetActivityId() == "" {
		return serviceerror.NewInvalidArgument("activity ID is required")
	}

	if len(req.GetActivityId()) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgumentf("activity ID exceeds length limit. Length=%d Limit=%d",
			len(req.GetActivityId()), maxIDLengthLimit)
	}

	if err := validateAndNormalizeRequestID(&req.RequestId, maxIDLengthLimit); err != nil {
		return err
	}

	if len(req.GetIdentity()) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgumentf("identity exceeds length limit. Length=%d Limit=%d",
			len(req.GetIdentity()), maxIDLengthLimit)
	}

	if runID := req.GetRunId(); runID != "" {
		_, err := uuid.Parse(runID)
		if err != nil {
			return serviceerror.NewInvalidArgument("invalid run id: must be a valid UUID")
		}
	}

	err := validateBlobSize(
		req.GetActivityId(),
		"RequestCancelActivityExecution",
		blobSizeLimitError,
		blobSizeLimitWarn,
		len(req.GetReason()),
		logger,
		req.GetNamespace())
	if err != nil {
		return serviceerror.NewInvalidArgument("reason exceeds length limit")
	}

	return nil
}

// supportedActivityOptionsUpdatePaths is the allowlist of update_mask paths (in camelCase JSON
// form, as produced by util.ConvertPathToCamel) that UpdateActivityExecutionOptions supports.
// It must stay in sync with the fields handled in MergeActivityOptions.
var supportedActivityOptionsUpdatePaths = map[string]struct{}{
	"taskQueue.name":                 {},
	"scheduleToCloseTimeout":         {},
	"scheduleToStartTimeout":         {},
	"startToCloseTimeout":            {},
	"heartbeatTimeout":               {},
	"priority":                       {},
	"priority.priorityKey":           {},
	"priority.fairnessKey":           {},
	"priority.fairnessWeight":        {},
	"retryPolicy":                    {},
	"retryPolicy.initialInterval":    {},
	"retryPolicy.backoffCoefficient": {},
	"retryPolicy.maximumInterval":    {},
	"retryPolicy.maximumAttempts":    {},
	"startDelay":                     {},
}

//nolint:revive // cyclomatic: per-field validation of a field-mask update requires explicit handling of each field
func validateAndNormalizeUpdateActivityExecutionOptionsRequest(
	req *workflowservice.UpdateActivityExecutionOptionsRequest,
	getDefaultActivityRetrySettings dynamicconfig.TypedPropertyFnWithNamespaceFilter[retrypolicy.DefaultRetrySettings],
	maxIDLengthLimit int,
) error {
	if err := validateAndNormalizeRequestID(&req.RequestId, maxIDLengthLimit); err != nil {
		return err
	}

	if req.GetActivityId() == "" {
		return serviceerror.NewInvalidArgument("activity ID is required")
	}

	if len(req.GetActivityId()) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgumentf("activity ID exceeds length limit. Length=%d Limit=%d",
			len(req.GetActivityId()), maxIDLengthLimit)
	}

	if len(req.GetIdentity()) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgumentf("identity exceeds length limit. Length=%d Limit=%d",
			len(req.GetIdentity()), maxIDLengthLimit)
	}

	if runID := req.GetRunId(); runID != "" {
		_, err := uuid.Parse(runID)
		if err != nil {
			return serviceerror.NewInvalidArgument("invalid run id: must be a valid UUID")
		}
	}

	if len(req.GetUpdateMask().GetPaths()) > 0 && req.GetRestoreOriginal() {
		return serviceerror.NewInvalidArgument("Both UpdateMask and RestoreOriginal are provided")
	}

	if req.GetRestoreOriginal() {
		return nil
	}

	if req.GetActivityOptions() == nil {
		return serviceerror.NewInvalidArgument("ActivityOptions are not provided")
	}
	if req.GetUpdateMask() == nil {
		return serviceerror.NewInvalidArgument("UpdateMask is not provided")
	}
	if len(req.GetUpdateMask().GetPaths()) == 0 {
		return serviceerror.NewInvalidArgument("UpdateMask must specify at least one path")
	}
	for _, path := range req.GetUpdateMask().GetPaths() {
		jsonPath := strings.Join(util.ConvertPathToCamel(path), ".")
		if _, ok := supportedActivityOptionsUpdatePaths[jsonPath]; !ok {
			return serviceerror.NewInvalidArgumentf("unsupported update_mask path: %q", path)
		}
	}

	opts := req.GetActivityOptions()
	updateFields := util.ParseFieldMask(req.GetUpdateMask())

	// TaskQueue: enforce user-defined task queue to prevent scheduling on reserved queues
	// (e.g. the internal per-namespace-worker task queue).
	if _, ok := updateFields["taskQueue.name"]; ok {
		if err := tqid.NormalizeAndValidateUserDefined(opts.GetTaskQueue(), "", "", maxIDLengthLimit); err != nil {
			return err
		}
	}

	// Timeouts: validate each timeout value that is being updated.
	if _, ok := updateFields["scheduleToCloseTimeout"]; ok {
		if err := timestamp.ValidateAndCapProtoDuration(opts.GetScheduleToCloseTimeout()); err != nil {
			return serviceerror.NewInvalidArgumentf("invalid ScheduleToCloseTimeout: %v", err)
		}
	}
	if _, ok := updateFields["scheduleToStartTimeout"]; ok {
		if err := timestamp.ValidateAndCapProtoDuration(opts.GetScheduleToStartTimeout()); err != nil {
			return serviceerror.NewInvalidArgumentf("invalid ScheduleToStartTimeout: %v", err)
		}
	}
	if _, ok := updateFields["startToCloseTimeout"]; ok {
		if err := timestamp.ValidateAndCapProtoDuration(opts.GetStartToCloseTimeout()); err != nil {
			return serviceerror.NewInvalidArgumentf("invalid StartToCloseTimeout: %v", err)
		}
	}
	if _, ok := updateFields["heartbeatTimeout"]; ok {
		if err := timestamp.ValidateAndCapProtoDuration(opts.GetHeartbeatTimeout()); err != nil {
			return serviceerror.NewInvalidArgumentf("invalid HeartbeatTimeout: %v", err)
		}
	}

	// Priority: validate the full priority when replacing it, or validate individual sub-fields.
	if _, ok := updateFields["priority"]; ok {
		if err := priorities.Validate(opts.GetPriority()); err != nil {
			return err
		}
	}
	if _, ok := updateFields["priority.priorityKey"]; ok {
		if opts.GetPriority().GetPriorityKey() < 0 {
			return priorities.ErrInvalidPriority
		}
	}
	if _, ok := updateFields["priority.fairnessKey"]; ok {
		if err := priorities.ValidateFairnessKey(opts.GetPriority().GetFairnessKey()); err != nil {
			return err
		}
	}
	if _, ok := updateFields["priority.fairnessWeight"]; ok {
		if opts.GetPriority().GetFairnessWeight() < 0 {
			return priorities.ErrInvalidFairnessWeight
		}
	}

	// RetryPolicy: validate the full policy when replacing it, or validate individual sub-fields.
	if _, ok := updateFields["retryPolicy"]; ok {
		if opts.RetryPolicy == nil {
			opts.RetryPolicy = &commonpb.RetryPolicy{}
		}
		retrypolicy.EnsureDefaults(opts.RetryPolicy, getDefaultActivityRetrySettings(req.GetNamespace()))
		if err := retrypolicy.Validate(opts.GetRetryPolicy()); err != nil {
			return err
		}
	}
	if _, ok := updateFields["retryPolicy.initialInterval"]; ok {
		if err := timestamp.ValidateAndCapProtoDuration(opts.GetRetryPolicy().GetInitialInterval()); err != nil {
			return serviceerror.NewInvalidArgumentf("invalid InitialInterval set on retry policy: %v", err)
		}
	}
	if _, ok := updateFields["retryPolicy.backoffCoefficient"]; ok {
		if opts.GetRetryPolicy().GetBackoffCoefficient() < 1 {
			return serviceerror.NewInvalidArgument("BackoffCoefficient cannot be less than 1 on retry policy.")
		}
	}
	if _, ok := updateFields["retryPolicy.maximumInterval"]; ok {
		if err := timestamp.ValidateAndCapProtoDuration(opts.GetRetryPolicy().GetMaximumInterval()); err != nil {
			return serviceerror.NewInvalidArgumentf("invalid MaximumInterval set on retry policy: %v", err)
		}
	}
	if _, ok := updateFields["retryPolicy.maximumAttempts"]; ok {
		if opts.GetRetryPolicy().GetMaximumAttempts() < 0 {
			return serviceerror.NewInvalidArgument("MaximumAttempts cannot be negative on retry policy.")
		}
	}

	return nil
}

func validateAndNormalizeDeleteActivityExecutionRequest(
	req *workflowservice.DeleteActivityExecutionRequest,
	maxIDLengthLimit int,
) error {
	if req.GetActivityId() == "" {
		return serviceerror.NewInvalidArgument("activity ID is required")
	}

	if len(req.GetActivityId()) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgumentf("activity ID exceeds length limit. Length=%d Limit=%d",
			len(req.GetActivityId()), maxIDLengthLimit)
	}

	if runID := req.GetRunId(); runID != "" {
		_, err := uuid.Parse(runID)
		if err != nil {
			return serviceerror.NewInvalidArgument("invalid run id: must be a valid UUID")
		}
	}

	return nil
}

func validateAndNormalizeTerminateActivityExecutionRequest(
	req *workflowservice.TerminateActivityExecutionRequest,
	maxIDLengthLimit int,
	blobSizeLimitError dynamicconfig.IntPropertyFnWithNamespaceFilter,
	blobSizeLimitWarn dynamicconfig.IntPropertyFnWithNamespaceFilter,
	logger log.Logger,
) error {
	if req.GetActivityId() == "" {
		return serviceerror.NewInvalidArgument("activity ID is required")
	}

	if len(req.GetActivityId()) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgumentf("activity ID exceeds length limit. Length=%d Limit=%d",
			len(req.GetActivityId()), maxIDLengthLimit)
	}

	if err := validateAndNormalizeRequestID(&req.RequestId, maxIDLengthLimit); err != nil {
		return err
	}

	if len(req.GetIdentity()) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgumentf("identity exceeds length limit. Length=%d Limit=%d",
			len(req.GetIdentity()), maxIDLengthLimit)
	}

	if runID := req.GetRunId(); runID != "" {
		_, err := uuid.Parse(runID)
		if err != nil {
			return serviceerror.NewInvalidArgument("invalid run id: must be a valid UUID")
		}
	}

	err := validateBlobSize(
		req.GetActivityId(),
		"TerminateActivityExecution",
		blobSizeLimitError,
		blobSizeLimitWarn,
		len(req.GetReason()),
		logger,
		req.GetNamespace())
	if err != nil {
		return serviceerror.NewInvalidArgument("reason exceeds length limit")
	}

	return nil
}

func validateAndNormalizePauseActivityExecutionRequest(
	req *workflowservice.PauseActivityExecutionRequest,
	maxIDLengthLimit int,
	blobSizeLimitError dynamicconfig.IntPropertyFnWithNamespaceFilter,
	blobSizeLimitWarn dynamicconfig.IntPropertyFnWithNamespaceFilter,
	logger log.Logger,
) error {
	if err := validateAndNormalizeRequestID(&req.RequestId, maxIDLengthLimit); err != nil {
		return err
	}
	if req.GetActivityId() == "" {
		return serviceerror.NewInvalidArgument("activity ID is required")
	}
	if len(req.GetActivityId()) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgumentf("activity ID exceeds length limit. Length=%d Limit=%d",
			len(req.GetActivityId()), maxIDLengthLimit)
	}
	if len(req.GetIdentity()) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgumentf("identity exceeds length limit. Length=%d Limit=%d",
			len(req.GetIdentity()), maxIDLengthLimit)
	}
	if runID := req.GetRunId(); runID != "" {
		_, err := uuid.Parse(runID)
		if err != nil {
			return serviceerror.NewInvalidArgument("invalid run id: must be a valid UUID")
		}
	}
	if err := validateBlobSize(
		req.GetActivityId(),
		"PauseActivityExecution",
		blobSizeLimitError,
		blobSizeLimitWarn,
		len(req.GetReason()),
		logger,
		req.GetNamespace()); err != nil {
		return serviceerror.NewInvalidArgument("reason exceeds length limit")
	}
	return nil
}

func validateAndNormalizeResetActivityExecutionRequest(
	req *workflowservice.ResetActivityExecutionRequest,
	maxIDLengthLimit int,
) error {
	if err := validateAndNormalizeRequestID(&req.RequestId, maxIDLengthLimit); err != nil {
		return err
	}

	if req.GetActivityId() == "" {
		return serviceerror.NewInvalidArgument("activity ID is required")
	}
	if len(req.GetActivityId()) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgumentf("activity ID exceeds length limit. Length=%d Limit=%d",
			len(req.GetActivityId()), maxIDLengthLimit)
	}
	if len(req.GetIdentity()) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgumentf("identity exceeds length limit. Length=%d Limit=%d",
			len(req.GetIdentity()), maxIDLengthLimit)
	}
	if runID := req.GetRunId(); runID != "" {
		_, err := uuid.Parse(runID)
		if err != nil {
			return serviceerror.NewInvalidArgument("invalid run id: must be a valid UUID")
		}
	}
	return validateJitter(req.GetJitter())
}

func validateAndNormalizeUnpauseActivityExecutionRequest(
	req *workflowservice.UnpauseActivityExecutionRequest,
	maxIDLengthLimit int,
) error {
	if err := validateAndNormalizeRequestID(&req.RequestId, maxIDLengthLimit); err != nil {
		return err
	}

	if req.GetActivityId() == "" {
		return serviceerror.NewInvalidArgument("activity ID is required")
	}
	if len(req.GetActivityId()) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgumentf("activity ID exceeds length limit. Length=%d Limit=%d",
			len(req.GetActivityId()), maxIDLengthLimit)
	}
	if len(req.GetIdentity()) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgumentf("identity exceeds length limit. Length=%d Limit=%d",
			len(req.GetIdentity()), maxIDLengthLimit)
	}
	if runID := req.GetRunId(); runID != "" {
		_, err := uuid.Parse(runID)
		if err != nil {
			return serviceerror.NewInvalidArgument("invalid run id: must be a valid UUID")
		}
	}
	return validateJitter(req.GetJitter())
}

func validateAndNormalizeRequestID(requestID *string, maxIDLengthLimit int) error {
	if *requestID == "" {
		*requestID = uuid.NewString()
	}
	if len(*requestID) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgumentf("request ID exceeds length limit. Length=%d Limit=%d",
			len(*requestID), maxIDLengthLimit)
	}
	return nil
}

func validateJitter(jitter *durationpb.Duration) error {
	if err := timestamp.ValidateAndCapProtoDuration(jitter); err != nil {
		return serviceerror.NewInvalidArgumentf("invalid jitter: %v", err)
	}
	return nil
}
