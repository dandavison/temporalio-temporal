package activityserver

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/callbacks"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/retrypolicy"
	test "go.temporal.io/server/common/testing"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	defaultActivityID       = "test-activity-id"
	defaultActivityType     = "test-activity-type"
	defaultTaskQueue        = "test-task-queue"
	defaultMaxIDLengthLimit = 1000
	defaultNamespaceID      = "default"
)

var (
	defaultActivityOptions = activitypb.ActivityOptions{
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval: durationpb.New(1 * time.Second),
		},
		ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
		TaskQueue:              &taskqueuepb.TaskQueue{Name: defaultTaskQueue},
	}

	defaultPriority = commonpb.Priority{FairnessKey: "normal"}

	defaultBlobSizeLimitError = func(ns string) int {
		return 64
	}
	defaultBlobSizeLimitWarn = func(ns string) int {
		return 32
	}
	defaultMaxUserMetadataSummarySize = func(ns string) int {
		return 400
	}
	defaultMaxUserMetadataDetailsSize = func(ns string) int {
		return 20000
	}
	defaultMaxLinksPerRequest = func(ns string) int {
		return 10
	}
	defaultLinkMaxSize = func(ns string) int {
		return 4000
	}
)

func newTestFrontendHandler(
	blobSizeLimitError func(string) int,
	blobSizeLimitWarn func(string) int,
	maxIDLengthLimit int,
) *frontendHandler {
	return &frontendHandler{
		config: &Config{
			BlobSizeLimitError:         blobSizeLimitError,
			BlobSizeLimitWarn:          blobSizeLimitWarn,
			MaxIDLengthLimit:           func() int { return maxIDLengthLimit },
			MaxUserMetadataDetailsSize: defaultMaxUserMetadataDetailsSize,
			MaxUserMetadataSummarySize: defaultMaxUserMetadataSummarySize,
		},
		logger: log.NewNoopLogger(),
	}
}

// TestRequestIDGeneratedWhenMissing verifies that the server generates a non-empty UUID request ID
// for every standalone activity API that carries a request_id field, when the client omits it.
// This prevents "" == "" false-positive idempotency matches in the state machine.
func TestRequestIDGeneratedWhenMissing(t *testing.T) {
	maxIDLengthLimit := defaultMaxIDLengthLimit
	blobLimit := defaultBlobSizeLimitError
	logger := log.NewNoopLogger()

	t.Run("StartActivityExecution", func(t *testing.T) {
		h := &frontendHandler{
			config: &Config{
				BlobSizeLimitError:         blobLimit,
				BlobSizeLimitWarn:          defaultBlobSizeLimitWarn,
				MaxIDLengthLimit:           func() int { return maxIDLengthLimit },
				DefaultActivityRetryPolicy: dynamicconfig.GetTypedPropertyFnFilteredByNamespace(getDefaultRetrySettings("")),
				MaxUserMetadataDetailsSize: defaultMaxUserMetadataDetailsSize,
				MaxUserMetadataSummarySize: defaultMaxUserMetadataSummarySize,
			},
			linkValidator: newLinkValidator(
				defaultMaxLinksPerRequest,
				func(string) int { return 2000 },
				defaultLinkMaxSize,
			),
			logger: logger,
		}
		req := &workflowservice.StartActivityExecutionRequest{
			ActivityId:          defaultActivityID,
			ActivityType:        &commonpb.ActivityType{Name: defaultActivityType},
			TaskQueue:           &taskqueuepb.TaskQueue{Name: defaultTaskQueue},
			StartToCloseTimeout: durationpb.New(10 * time.Second),
			RetryPolicy:         &commonpb.RetryPolicy{},
		}
		_, err := h.validateAndPopulateStartRequest(t.Context(), req, namespace.ID(defaultNamespaceID))
		require.NoError(t, err)
		require.NotEmpty(t, req.GetRequestId(), "server must generate a request ID when client omits it")
		require.NoError(t, validateUUID(req.GetRequestId()), "generated request ID must be a valid UUID")
	})

	t.Run("RequestCancelActivityExecution", func(t *testing.T) {
		req := &workflowservice.RequestCancelActivityExecutionRequest{
			ActivityId: defaultActivityID,
		}
		err := validateAndNormalizeRequestCancelActivityExecutionRequest(req, maxIDLengthLimit, blobLimit, blobLimit, logger)
		require.NoError(t, err)
		require.NotEmpty(t, req.GetRequestId(), "server must generate a request ID when client omits it")
		require.NoError(t, validateUUID(req.GetRequestId()), "generated request ID must be a valid UUID")
	})

	t.Run("TerminateActivityExecution", func(t *testing.T) {
		req := &workflowservice.TerminateActivityExecutionRequest{
			ActivityId: defaultActivityID,
		}
		err := validateAndNormalizeTerminateActivityExecutionRequest(req, maxIDLengthLimit, blobLimit, blobLimit, logger)
		require.NoError(t, err)
		require.NotEmpty(t, req.GetRequestId(), "server must generate a request ID when client omits it")
		require.NoError(t, validateUUID(req.GetRequestId()), "generated request ID must be a valid UUID")
	})

	t.Run("PauseActivityExecution", func(t *testing.T) {
		req := &workflowservice.PauseActivityExecutionRequest{
			ActivityId: defaultActivityID,
		}
		err := validateAndNormalizePauseActivityExecutionRequest(req, maxIDLengthLimit, blobLimit, blobLimit, logger)
		require.NoError(t, err)
		require.NotEmpty(t, req.GetRequestId(), "server must generate a request ID when client omits it")
		require.NoError(t, validateUUID(req.GetRequestId()), "generated request ID must be a valid UUID")
	})

	t.Run("UnpauseActivityExecution", func(t *testing.T) {
		req := &workflowservice.UnpauseActivityExecutionRequest{
			ActivityId: defaultActivityID,
		}
		err := validateAndNormalizeUnpauseActivityExecutionRequest(req, maxIDLengthLimit)
		require.NoError(t, err)
		require.NotEmpty(t, req.GetRequestId(), "server must generate a request ID when client omits it")
		require.NoError(t, validateUUID(req.GetRequestId()), "generated request ID must be a valid UUID")
	})

	t.Run("ResetActivityExecution", func(t *testing.T) {
		req := &workflowservice.ResetActivityExecutionRequest{
			ActivityId: defaultActivityID,
		}
		err := validateAndNormalizeResetActivityExecutionRequest(req, maxIDLengthLimit)
		require.NoError(t, err)
		require.NotEmpty(t, req.GetRequestId(), "server must generate a request ID when client omits it")
		require.NoError(t, validateUUID(req.GetRequestId()), "generated request ID must be a valid UUID")
	})

	t.Run("UpdateActivityExecutionOptions", func(t *testing.T) {
		req := &workflowservice.UpdateActivityExecutionOptionsRequest{
			ActivityId:      defaultActivityID,
			RestoreOriginal: true,
		}
		err := validateAndNormalizeUpdateActivityExecutionOptionsRequest(req, getDefaultRetrySettings, maxIDLengthLimit)
		require.NoError(t, err)
		require.NotEmpty(t, req.GetRequestId(), "server must generate a request ID when client omits it")
		require.NoError(t, validateUUID(req.GetRequestId()), "generated request ID must be a valid UUID")
	})
}

// TestValidateAndPopulateStartRequest_CombinesRequestAndCallbackLinks verifies that both
// the request's own links and those embedded in its completion callbacks reach the link
// validator. Link shape and limit semantics are covered in common/links.
func TestValidateAndPopulateStartRequest_CombinesRequestAndCallbackLinks(t *testing.T) {
	callbackValidator, err := callbacks.NewValidator(test.NewCallbacksValidatorConfig(), nil)
	require.NoError(t, err)

	h := &frontendHandler{
		config: &Config{
			BlobSizeLimitError:         defaultBlobSizeLimitError,
			BlobSizeLimitWarn:          defaultBlobSizeLimitWarn,
			DefaultActivityRetryPolicy: getDefaultRetrySettings,
			EnableCallbacks:            func(string) bool { return true },
			EnabledCallbackKinds: func(string) []callbacks.Kind {
				return []callbacks.Kind{callbacks.KindNexus}
			},
			MaxIDLengthLimit:           func() int { return defaultMaxIDLengthLimit },
			MaxUserMetadataDetailsSize: defaultMaxUserMetadataDetailsSize,
			MaxUserMetadataSummarySize: defaultMaxUserMetadataSummarySize,
		},
		callbackValidator: callbackValidator,
		linkValidator: newLinkValidator(
			func(string) int { return 1 },
			func(string) int { return 2000 },
			defaultLinkMaxSize,
		),
		logger: log.NewNoopLogger(),
	}
	// The per-request limit is 1, so a single link from each source only trips the
	// validator if both sources are forwarded.
	req := &workflowservice.StartActivityExecutionRequest{
		Namespace:           defaultNamespaceID,
		ActivityId:          defaultActivityID,
		ActivityType:        &commonpb.ActivityType{Name: defaultActivityType},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: defaultTaskQueue},
		StartToCloseTimeout: durationpb.New(10 * time.Second),
		Links: []*commonpb.Link{{
			Variant: &commonpb.Link_BatchJob_{
				BatchJob: &commonpb.Link_BatchJob{JobId: "request-job"},
			},
		}},
		CompletionCallbacks: []*commonpb.Callback{{
			Variant: &commonpb.Callback_Nexus_{
				Nexus: &commonpb.Callback_Nexus{
					Url: "http://localhost/cb",
				},
			},
			Links: []*commonpb.Link{{
				Variant: &commonpb.Link_BatchJob_{
					BatchJob: &commonpb.Link_BatchJob{JobId: "callback-job"},
				},
			}},
		}},
	}

	_, err = h.validateAndPopulateStartRequest(t.Context(), req, namespace.ID(defaultNamespaceID))
	require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
	require.ErrorContains(t, err, "cannot attach more than 1 links per request, got 2")
}

func validateUUID(s string) error {
	_, err := uuid.Parse(s)
	return err
}

// TestRequestIDTooLong verifies that every standalone activity API enforces the request ID
// length limit when the client supplies a value that exceeds it.
func TestRequestIDTooLong(t *testing.T) {
	maxIDLengthLimit := defaultMaxIDLengthLimit
	blobLimit := defaultBlobSizeLimitError
	logger := log.NewNoopLogger()
	tooLong := string(make([]byte, maxIDLengthLimit+1))

	t.Run("StartActivityExecution", func(t *testing.T) {
		h := newTestFrontendHandler(blobLimit, defaultBlobSizeLimitWarn, maxIDLengthLimit)
		req := &workflowservice.StartActivityExecutionRequest{
			ActivityId:          defaultActivityID,
			ActivityType:        &commonpb.ActivityType{Name: defaultActivityType},
			TaskQueue:           &taskqueuepb.TaskQueue{Name: defaultTaskQueue},
			StartToCloseTimeout: durationpb.New(10 * time.Second),
			RetryPolicy:         &commonpb.RetryPolicy{},
			RequestId:           tooLong,
		}
		err := validateAndNormalizeStartRequest(req, h.config, h.logger, h.saMapperProvider, h.saValidator)
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
	})

	t.Run("RequestCancelActivityExecution", func(t *testing.T) {
		req := &workflowservice.RequestCancelActivityExecutionRequest{
			ActivityId: defaultActivityID,
			RequestId:  tooLong,
		}
		err := validateAndNormalizeRequestCancelActivityExecutionRequest(req, maxIDLengthLimit, blobLimit, blobLimit, logger)
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
	})

	t.Run("TerminateActivityExecution", func(t *testing.T) {
		req := &workflowservice.TerminateActivityExecutionRequest{
			ActivityId: defaultActivityID,
			RequestId:  tooLong,
		}
		err := validateAndNormalizeTerminateActivityExecutionRequest(req, maxIDLengthLimit, blobLimit, blobLimit, logger)
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
	})

	t.Run("PauseActivityExecution", func(t *testing.T) {
		req := &workflowservice.PauseActivityExecutionRequest{
			ActivityId: defaultActivityID,
			RequestId:  tooLong,
		}
		err := validateAndNormalizePauseActivityExecutionRequest(req, maxIDLengthLimit, blobLimit, blobLimit, logger)
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
	})

	t.Run("UnpauseActivityExecution", func(t *testing.T) {
		req := &workflowservice.UnpauseActivityExecutionRequest{
			ActivityId: defaultActivityID,
			RequestId:  tooLong,
		}
		err := validateAndNormalizeUnpauseActivityExecutionRequest(req, maxIDLengthLimit)
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
	})

	t.Run("ResetActivityExecution", func(t *testing.T) {
		req := &workflowservice.ResetActivityExecutionRequest{
			ActivityId: defaultActivityID,
			RequestId:  tooLong,
		}
		err := validateAndNormalizeResetActivityExecutionRequest(req, maxIDLengthLimit)
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
	})

	t.Run("UpdateActivityExecutionOptions", func(t *testing.T) {
		req := &workflowservice.UpdateActivityExecutionOptionsRequest{
			ActivityId:      defaultActivityID,
			RestoreOriginal: true,
			RequestId:       tooLong,
		}
		err := validateAndNormalizeUpdateActivityExecutionOptionsRequest(req, getDefaultRetrySettings, maxIDLengthLimit)
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
	})
}

func TestValidateStandAloneRequestIDTooLong(t *testing.T) {
	req := &workflowservice.StartActivityExecutionRequest{
		ActivityId:   defaultActivityID,
		ActivityType: &commonpb.ActivityType{Name: defaultActivityType},
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval: durationpb.New(1 * time.Second),
		},
		ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
		TaskQueue:              &taskqueuepb.TaskQueue{Name: defaultTaskQueue},
		Namespace:              "default",
		RequestId:              string(make([]byte, 1001)),
		Input:                  payloads.EncodeString("test-input"),
	}

	h := newTestFrontendHandler(defaultBlobSizeLimitError, defaultBlobSizeLimitWarn, defaultMaxIDLengthLimit)
	err := validateAndNormalizeStartRequest(req, h.config, h.logger, h.saMapperProvider, h.saValidator)
	var invalidArgErr *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalidArgErr)
}

func TestValidateStandAloneInputTooLarge(t *testing.T) {
	req := &workflowservice.StartActivityExecutionRequest{
		ActivityId:   defaultActivityID,
		ActivityType: &commonpb.ActivityType{Name: defaultActivityType},
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval: durationpb.New(1 * time.Second),
		},
		ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
		TaskQueue:              &taskqueuepb.TaskQueue{Name: defaultTaskQueue},
		Namespace:              "default",
		RequestId:              "test-request-id",
		Input:                  payloads.EncodeString(string(make([]byte, 1000))),
	}

	h := newTestFrontendHandler(defaultBlobSizeLimitError, defaultBlobSizeLimitWarn, defaultMaxIDLengthLimit)
	err := validateAndNormalizeStartRequest(req, h.config, h.logger, h.saMapperProvider, h.saValidator)
	var invalidArgErr *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalidArgErr)
}

func TestValidateStandAloneInputWarningSizeShouldSucceed(t *testing.T) {
	payload := payloads.EncodeString("test-input")
	payloadSize := payload.Size()

	req := &workflowservice.StartActivityExecutionRequest{
		ActivityId:   defaultActivityID,
		ActivityType: &commonpb.ActivityType{Name: defaultActivityType},
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval: durationpb.New(1 * time.Second),
		},
		ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
		TaskQueue:              &taskqueuepb.TaskQueue{Name: defaultTaskQueue},
		Namespace:              "default",
		RequestId:              "test-request-id",
		Input:                  payload,
	}

	h := newTestFrontendHandler(
		func(ns string) int { return payloadSize + 1 },
		func(ns string) int { return payloadSize },
		defaultMaxIDLengthLimit,
	)
	err := validateAndNormalizeStartRequest(req, h.config, h.logger, h.saMapperProvider, h.saValidator)
	require.NoError(t, err)
}

func TestValidateStandaloneUserMetadata(t *testing.T) {
	summary := &commonpb.Payload{Data: []byte("summary")}
	details := &commonpb.Payload{Data: []byte("details")}

	tests := []struct {
		name         string
		metadata     *sdkpb.UserMetadata
		summaryLimit int
		detailsLimit int
		errContains  string
	}{
		{
			name:         "summary exceeds size limit",
			metadata:     &sdkpb.UserMetadata{Summary: summary},
			summaryLimit: summary.Size() - 1,
			detailsLimit: details.Size() + 1000,
			errContains:  "user_metadata.summary exceeds size limit",
		},
		{
			name:         "details exceeds size limit",
			metadata:     &sdkpb.UserMetadata{Details: details},
			summaryLimit: summary.Size() + 1000,
			detailsLimit: details.Size() - 1,
			errContains:  "user_metadata.details exceeds size limit",
		},
		{
			name:         "payloads at size limits",
			metadata:     &sdkpb.UserMetadata{Summary: summary, Details: details},
			summaryLimit: summary.Size(),
			detailsLimit: details.Size(),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := &frontendHandler{
				config: &Config{
					BlobSizeLimitError:         defaultBlobSizeLimitError,
					BlobSizeLimitWarn:          defaultBlobSizeLimitWarn,
					DefaultActivityRetryPolicy: getDefaultRetrySettings,
					MaxIDLengthLimit:           func() int { return defaultMaxIDLengthLimit },
					MaxUserMetadataSummarySize: func(string) int { return tc.summaryLimit },
					MaxUserMetadataDetailsSize: func(string) int { return tc.detailsLimit },
				},
				linkValidator: newLinkValidator(
					defaultMaxLinksPerRequest,
					func(string) int { return 2000 },
					defaultLinkMaxSize,
				),
				logger: log.NewNoopLogger(),
			}
			req := &workflowservice.StartActivityExecutionRequest{
				Namespace:           defaultNamespaceID,
				ActivityId:          defaultActivityID,
				ActivityType:        &commonpb.ActivityType{Name: defaultActivityType},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: defaultTaskQueue},
				StartToCloseTimeout: durationpb.New(10 * time.Second),
				UserMetadata:        tc.metadata,
			}
			_, err := h.validateAndPopulateStartRequest(t.Context(), req, namespace.ID(defaultNamespaceID))
			if tc.errContains == "" {
				require.NoError(t, err)
				return
			}
			var invalidArgument *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgument)
			require.ErrorContains(t, err, tc.errContains)
		})
	}
}

func TestValidateStandAlone_IDPolicyShouldDefault(t *testing.T) {
	req := &workflowservice.StartActivityExecutionRequest{
		ActivityId:   defaultActivityID,
		ActivityType: &commonpb.ActivityType{Name: defaultActivityType},
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval: durationpb.New(1 * time.Second),
		},
		ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
		TaskQueue:              &taskqueuepb.TaskQueue{Name: defaultTaskQueue},
		Namespace:              "default",
		RequestId:              "test-request-id",
	}

	h := newTestFrontendHandler(defaultBlobSizeLimitError, defaultBlobSizeLimitWarn, defaultMaxIDLengthLimit)
	err := validateAndNormalizeStartRequest(req, h.config, h.logger, h.saMapperProvider, h.saValidator)

	require.NoError(t, err)
	require.Equal(t, enumspb.ACTIVITY_ID_REUSE_POLICY_ALLOW_DUPLICATE, req.IdReusePolicy)
	require.Equal(t, enumspb.ACTIVITY_ID_CONFLICT_POLICY_FAIL, req.IdConflictPolicy)
}

func TestValidateDeleteActivityExecutionRequest(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		req := &workflowservice.DeleteActivityExecutionRequest{
			ActivityId: defaultActivityID,
		}
		err := validateAndNormalizeDeleteActivityExecutionRequest(req, defaultMaxIDLengthLimit)
		require.NoError(t, err)
	})

	t.Run("SuccessWithRunID", func(t *testing.T) {
		req := &workflowservice.DeleteActivityExecutionRequest{
			ActivityId: defaultActivityID,
			RunId:      "f47ac10b-58cc-4372-a567-0e02b2c3d479",
		}
		err := validateAndNormalizeDeleteActivityExecutionRequest(req, defaultMaxIDLengthLimit)
		require.NoError(t, err)
	})

	t.Run("EmptyActivityID", func(t *testing.T) {
		req := &workflowservice.DeleteActivityExecutionRequest{
			ActivityId: "",
		}
		err := validateAndNormalizeDeleteActivityExecutionRequest(req, defaultMaxIDLengthLimit)
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
	})

	t.Run("ActivityIDTooLong", func(t *testing.T) {
		req := &workflowservice.DeleteActivityExecutionRequest{
			ActivityId: string(make([]byte, defaultMaxIDLengthLimit+1)),
		}
		err := validateAndNormalizeDeleteActivityExecutionRequest(req, defaultMaxIDLengthLimit)
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
	})

	t.Run("InvalidRunID", func(t *testing.T) {
		req := &workflowservice.DeleteActivityExecutionRequest{
			ActivityId: defaultActivityID,
			RunId:      "not-a-valid-uuid",
		}
		err := validateAndNormalizeDeleteActivityExecutionRequest(req, defaultMaxIDLengthLimit)
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
	})
}

func TestValidateOnConflictOptions(t *testing.T) {
	t.Run("NilOptions", func(t *testing.T) {
		require.NoError(t, validateOnConflictOptions(&workflowservice.StartActivityExecutionRequest{}))
	})

	t.Run("AttachRequestIdAndCallbacksWithCallback", func(t *testing.T) {
		req := &workflowservice.StartActivityExecutionRequest{
			CompletionCallbacks: []*commonpb.Callback{{}},
			OnConflictOptions: &commonpb.OnConflictOptions{
				AttachRequestId:           true,
				AttachCompletionCallbacks: true,
			},
		}
		require.NoError(t, validateOnConflictOptions(req))
	})

	t.Run("AttachLinksOnly", func(t *testing.T) {
		// Links do not require a request ID; this combination must remain valid.
		req := &workflowservice.StartActivityExecutionRequest{
			OnConflictOptions: &commonpb.OnConflictOptions{AttachLinks: true},
		}
		require.NoError(t, validateOnConflictOptions(req))
	})

	t.Run("AttachRequestIdWithLink", func(t *testing.T) {
		// A request ID alongside a link is valid (request ID has something to attach to).
		req := &workflowservice.StartActivityExecutionRequest{
			Links: []*commonpb.Link{{}},
			OnConflictOptions: &commonpb.OnConflictOptions{
				AttachRequestId: true,
				AttachLinks:     true,
			},
		}
		require.NoError(t, validateOnConflictOptions(req))
	})

	t.Run("AttachCallbacksWithoutRequestId", func(t *testing.T) {
		req := &workflowservice.StartActivityExecutionRequest{
			CompletionCallbacks: []*commonpb.Callback{{}},
			OnConflictOptions:   &commonpb.OnConflictOptions{AttachCompletionCallbacks: true},
		}
		err := validateOnConflictOptions(req)
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Contains(t, invalidArgErr.Message, "attach_completion_callbacks requires attach_request_id")
	})

	t.Run("AttachRequestIdWithoutCallbackOrLink", func(t *testing.T) {
		req := &workflowservice.StartActivityExecutionRequest{
			OnConflictOptions: &commonpb.OnConflictOptions{AttachRequestId: true},
		}
		err := validateOnConflictOptions(req)
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Contains(t, invalidArgErr.Message, "attach_request_id requires at least one completion callback or link")
	})

	t.Run("AttachRequestIdAndCallbacksWithoutCallbackProvided", func(t *testing.T) {
		req := &workflowservice.StartActivityExecutionRequest{
			OnConflictOptions: &commonpb.OnConflictOptions{
				AttachRequestId:           true,
				AttachCompletionCallbacks: true,
			},
		}
		err := validateOnConflictOptions(req)
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Contains(t, invalidArgErr.Message, "attach_request_id requires at least one completion callback or link")
	})
}

func TestValidatePauseActivityExecutionRequest(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		req := &workflowservice.PauseActivityExecutionRequest{
			ActivityId: defaultActivityID,
			Identity:   "test-identity",
			Reason:     "test-reason",
		}
		err := validateAndNormalizePauseActivityExecutionRequest(req, defaultMaxIDLengthLimit, defaultBlobSizeLimitError, defaultBlobSizeLimitWarn, log.NewNoopLogger())
		require.NoError(t, err)
	})

	t.Run("SuccessWithRunID", func(t *testing.T) {
		req := &workflowservice.PauseActivityExecutionRequest{
			ActivityId: defaultActivityID,
			RunId:      "f47ac10b-58cc-4372-a567-0e02b2c3d479",
			Identity:   "test-identity",
		}
		err := validateAndNormalizePauseActivityExecutionRequest(req, defaultMaxIDLengthLimit, defaultBlobSizeLimitError, defaultBlobSizeLimitWarn, log.NewNoopLogger())
		require.NoError(t, err)
	})

	t.Run("EmptyActivityID", func(t *testing.T) {
		req := &workflowservice.PauseActivityExecutionRequest{
			ActivityId: "",
			Identity:   "test-identity",
		}
		err := validateAndNormalizePauseActivityExecutionRequest(req, defaultMaxIDLengthLimit, defaultBlobSizeLimitError, defaultBlobSizeLimitWarn, log.NewNoopLogger())
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Equal(t, "activity ID is required", invalidArgErr.Message)
	})

	t.Run("ActivityIDTooLong", func(t *testing.T) {
		req := &workflowservice.PauseActivityExecutionRequest{
			ActivityId: string(make([]byte, defaultMaxIDLengthLimit+1)),
			Identity:   "test-identity",
		}
		err := validateAndNormalizePauseActivityExecutionRequest(req, defaultMaxIDLengthLimit, defaultBlobSizeLimitError, defaultBlobSizeLimitWarn, log.NewNoopLogger())
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Equal(t, fmt.Sprintf("activity ID exceeds length limit. Length=%d Limit=%d",
			defaultMaxIDLengthLimit+1, defaultMaxIDLengthLimit), invalidArgErr.Message)
	})

	t.Run("IdentityTooLong", func(t *testing.T) {
		req := &workflowservice.PauseActivityExecutionRequest{
			ActivityId: defaultActivityID,
			Identity:   string(make([]byte, defaultMaxIDLengthLimit+1)),
		}
		err := validateAndNormalizePauseActivityExecutionRequest(req, defaultMaxIDLengthLimit, defaultBlobSizeLimitError, defaultBlobSizeLimitWarn, log.NewNoopLogger())
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Equal(t, fmt.Sprintf("identity exceeds length limit. Length=%d Limit=%d",
			defaultMaxIDLengthLimit+1, defaultMaxIDLengthLimit), invalidArgErr.Message)
	})

	t.Run("ReasonTooLong", func(t *testing.T) {
		req := &workflowservice.PauseActivityExecutionRequest{
			ActivityId: defaultActivityID,
			Identity:   "test-identity",
			Reason:     string(make([]byte, defaultBlobSizeLimitError("default")+1)),
		}
		err := validateAndNormalizePauseActivityExecutionRequest(req, defaultMaxIDLengthLimit, defaultBlobSizeLimitError, defaultBlobSizeLimitWarn, log.NewNoopLogger())
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Equal(t, "reason exceeds length limit", invalidArgErr.Message)
	})

	t.Run("InvalidRunID", func(t *testing.T) {
		req := &workflowservice.PauseActivityExecutionRequest{
			ActivityId: defaultActivityID,
			RunId:      "not-a-valid-uuid",
			Identity:   "test-identity",
		}
		err := validateAndNormalizePauseActivityExecutionRequest(req, defaultMaxIDLengthLimit, defaultBlobSizeLimitError, defaultBlobSizeLimitWarn, log.NewNoopLogger())
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Equal(t, "invalid run id: must be a valid UUID", invalidArgErr.Message)
	})
}

func TestValidateUnpauseActivityExecutionRequest(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		req := &workflowservice.UnpauseActivityExecutionRequest{
			ActivityId: defaultActivityID,
			Identity:   "test-identity",
		}
		err := validateAndNormalizeUnpauseActivityExecutionRequest(req, defaultMaxIDLengthLimit)
		require.NoError(t, err)
	})

	t.Run("SuccessWithRunID", func(t *testing.T) {
		req := &workflowservice.UnpauseActivityExecutionRequest{
			ActivityId: defaultActivityID,
			RunId:      "f47ac10b-58cc-4372-a567-0e02b2c3d479",
			Identity:   "test-identity",
		}
		err := validateAndNormalizeUnpauseActivityExecutionRequest(req, defaultMaxIDLengthLimit)
		require.NoError(t, err)
	})

	t.Run("EmptyActivityID", func(t *testing.T) {
		req := &workflowservice.UnpauseActivityExecutionRequest{
			ActivityId: "",
			Identity:   "test-identity",
		}
		err := validateAndNormalizeUnpauseActivityExecutionRequest(req, defaultMaxIDLengthLimit)
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Equal(t, "activity ID is required", invalidArgErr.Message)
	})

	t.Run("ActivityIDTooLong", func(t *testing.T) {
		req := &workflowservice.UnpauseActivityExecutionRequest{
			ActivityId: string(make([]byte, defaultMaxIDLengthLimit+1)),
			Identity:   "test-identity",
		}
		err := validateAndNormalizeUnpauseActivityExecutionRequest(req, defaultMaxIDLengthLimit)
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Equal(t, fmt.Sprintf("activity ID exceeds length limit. Length=%d Limit=%d",
			defaultMaxIDLengthLimit+1, defaultMaxIDLengthLimit), invalidArgErr.Message)
	})

	t.Run("IdentityTooLong", func(t *testing.T) {
		req := &workflowservice.UnpauseActivityExecutionRequest{
			ActivityId: defaultActivityID,
			Identity:   string(make([]byte, defaultMaxIDLengthLimit+1)),
		}
		err := validateAndNormalizeUnpauseActivityExecutionRequest(req, defaultMaxIDLengthLimit)
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Equal(t, fmt.Sprintf("identity exceeds length limit. Length=%d Limit=%d",
			defaultMaxIDLengthLimit+1, defaultMaxIDLengthLimit), invalidArgErr.Message)
	})

	t.Run("InvalidRunID", func(t *testing.T) {
		req := &workflowservice.UnpauseActivityExecutionRequest{
			ActivityId: defaultActivityID,
			RunId:      "not-a-valid-uuid",
			Identity:   "test-identity",
		}
		err := validateAndNormalizeUnpauseActivityExecutionRequest(req, defaultMaxIDLengthLimit)
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Equal(t, "invalid run id: must be a valid UUID", invalidArgErr.Message)
	})

	t.Run("NegativeJitter", func(t *testing.T) {
		req := &workflowservice.UnpauseActivityExecutionRequest{
			ActivityId: defaultActivityID,
			Jitter:     durationpb.New(-time.Second),
		}
		err := validateAndNormalizeUnpauseActivityExecutionRequest(req, defaultMaxIDLengthLimit)
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Equal(t, "invalid jitter: negative duration", invalidArgErr.Message)
	})
}

func TestValidateResetActivityExecutionRequest(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		req := &workflowservice.ResetActivityExecutionRequest{
			ActivityId: defaultActivityID,
			Identity:   "test-identity",
		}
		err := validateAndNormalizeResetActivityExecutionRequest(req, defaultMaxIDLengthLimit)
		require.NoError(t, err)
	})

	t.Run("SuccessWithRunID", func(t *testing.T) {
		req := &workflowservice.ResetActivityExecutionRequest{
			ActivityId: defaultActivityID,
			RunId:      "f47ac10b-58cc-4372-a567-0e02b2c3d479",
			Identity:   "test-identity",
		}
		err := validateAndNormalizeResetActivityExecutionRequest(req, defaultMaxIDLengthLimit)
		require.NoError(t, err)
	})

	t.Run("EmptyActivityID", func(t *testing.T) {
		req := &workflowservice.ResetActivityExecutionRequest{
			ActivityId: "",
			Identity:   "test-identity",
		}
		err := validateAndNormalizeResetActivityExecutionRequest(req, defaultMaxIDLengthLimit)
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Equal(t, "activity ID is required", invalidArgErr.Message)
	})

	t.Run("ActivityIDTooLong", func(t *testing.T) {
		req := &workflowservice.ResetActivityExecutionRequest{
			ActivityId: string(make([]byte, defaultMaxIDLengthLimit+1)),
			Identity:   "test-identity",
		}
		err := validateAndNormalizeResetActivityExecutionRequest(req, defaultMaxIDLengthLimit)
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Equal(t, fmt.Sprintf("activity ID exceeds length limit. Length=%d Limit=%d",
			defaultMaxIDLengthLimit+1, defaultMaxIDLengthLimit), invalidArgErr.Message)
	})

	t.Run("IdentityTooLong", func(t *testing.T) {
		req := &workflowservice.ResetActivityExecutionRequest{
			ActivityId: defaultActivityID,
			Identity:   string(make([]byte, defaultMaxIDLengthLimit+1)),
		}
		err := validateAndNormalizeResetActivityExecutionRequest(req, defaultMaxIDLengthLimit)
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Equal(t, fmt.Sprintf("identity exceeds length limit. Length=%d Limit=%d",
			defaultMaxIDLengthLimit+1, defaultMaxIDLengthLimit), invalidArgErr.Message)
	})

	t.Run("InvalidRunID", func(t *testing.T) {
		req := &workflowservice.ResetActivityExecutionRequest{
			ActivityId: defaultActivityID,
			RunId:      "not-a-valid-uuid",
			Identity:   "test-identity",
		}
		err := validateAndNormalizeResetActivityExecutionRequest(req, defaultMaxIDLengthLimit)
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Equal(t, "invalid run id: must be a valid UUID", invalidArgErr.Message)
	})

	t.Run("NegativeJitter", func(t *testing.T) {
		req := &workflowservice.ResetActivityExecutionRequest{
			ActivityId: defaultActivityID,
			Jitter:     durationpb.New(-time.Second),
		}
		err := validateAndNormalizeResetActivityExecutionRequest(req, defaultMaxIDLengthLimit)
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Equal(t, "invalid jitter: negative duration", invalidArgErr.Message)
	})
}

func getDefaultRetrySettings(_ string) retrypolicy.DefaultRetrySettings {
	return retrypolicy.DefaultRetrySettings{
		InitialInterval:            time.Second,
		MaximumIntervalCoefficient: 100.0,
		BackoffCoefficient:         2.0,
		MaximumAttempts:            0,
	}
}
