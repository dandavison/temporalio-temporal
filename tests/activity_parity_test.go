package tests

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/retrypolicy"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

// A StartToClose or Heartbeat timeout whose type is listed in the retry policy's NonRetryableErrorTypes
// must fail the activity terminally (TimedOut) when it fires, rather than retrying.
func (s *standaloneActivityTestSuite) TestParityNonRetryableTimeout() {
	env := s.newTestEnv()

	both := func(t *testing.T, drive func(driver, *testing.T) enumspb.ActivityExecutionStatus) {
		t.Run("WorkflowActivity", func(t *testing.T) {
			require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, drive(&wfaDriver{s: s, env: env}, t),
				"a non-retryable timeout must fail the activity terminally, not retry it")
		})
		t.Run("StandaloneActivity", func(t *testing.T) {
			require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, drive(&saaDriver{s: s, env: env}, t),
				"a non-retryable timeout must fail the activity terminally, not retry it")
		})
	}

	s.T().Run("StartToClose", func(t *testing.T) {
		both(t, func(d driver, t *testing.T) enumspb.ActivityExecutionStatus {
			return d.start_Poll_StartToCloseTimeoutElapses(t)
		})
	})
	s.T().Run("Heartbeat", func(t *testing.T) {
		both(t, func(d driver, t *testing.T) enumspb.ActivityExecutionStatus {
			return d.start_Poll_HeartbeatTimeoutElapses(t)
		})
	})
}

// When a timeout closes the activity terminally — retries exhausted by a StartToClose/Heartbeat
// timeout on the final attempt, or a schedule-to-close deadline reached — the terminal TimedOut
// failure must chain the application failure that drove the retries as its Cause, so the error
// survives to the client.
func (s *standaloneActivityTestSuite) TestParityTimeoutPreservesUnderlyingFailureCause() {
	env := s.newTestEnv()

	both := func(t *testing.T, timeoutType enumspb.TimeoutType, drive func(driver, *testing.T) terminalTimeout) {
		assert := func(t *testing.T, d driver) {
			got := drive(d, t)
			require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, got.Status)
			require.Equal(t, timeoutType, got.TimeoutType)
			require.Equal(t, reproCauseType, got.CauseType, "the terminal timeout must chain the application failure's Type as its Cause")
			require.Equal(t, reproCauseType, got.CauseMessage, "the terminal timeout must chain the application failure's Message as its Cause")
		}
		t.Run("WorkflowActivity", func(t *testing.T) { assert(t, &wfaDriver{s: s, env: env}) })
		t.Run("StandaloneActivity", func(t *testing.T) { assert(t, &saaDriver{s: s, env: env}) })
	}

	// Retries exhausted by a StartToClose timeout on the final attempt (attempt 1 failed retryably).
	s.T().Run("StartToClose", func(t *testing.T) {
		both(t, enumspb.TIMEOUT_TYPE_START_TO_CLOSE, func(d driver, t *testing.T) terminalTimeout {
			return d.start_Poll_Fail_Poll_StartToCloseTimeoutElapses(t)
		})
	})
	// Retries exhausted by a Heartbeat timeout on the final attempt: the attempt is started but never
	// heartbeats, so it times out.
	s.T().Run("Heartbeat", func(t *testing.T) {
		both(t, enumspb.TIMEOUT_TYPE_HEARTBEAT, func(d driver, t *testing.T) terminalTimeout {
			return d.start_Poll_Fail_Poll_HeartbeatTimeoutElapses(t)
		})
	})
	// Schedule-to-close deadline closes the activity while it backs off to retry.
	s.T().Run("ScheduleToClose", func(t *testing.T) {
		both(t, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE, func(d driver, t *testing.T) terminalTimeout {
			return d.start_Poll_Fail_ScheduleToCloseTimeoutElapses(t)
		})
	})
}

// driver is the interface implemented by the WFA and SAA drivers.
type driver interface {
	start_Poll_StartToCloseTimeoutElapses(t *testing.T) enumspb.ActivityExecutionStatus
	start_Poll_HeartbeatTimeoutElapses(t *testing.T) enumspb.ActivityExecutionStatus
	start_Poll_Fail_Poll_StartToCloseTimeoutElapses(t *testing.T) terminalTimeout
	start_Poll_Fail_Poll_HeartbeatTimeoutElapses(t *testing.T) terminalTimeout
	start_Poll_Fail_ScheduleToCloseTimeoutElapses(t *testing.T) terminalTimeout
}

// terminalTimeout is a terminal-timeout outcome: the terminal status, the timeout type, and the
// application failure the timeout chains as its Cause (its Type and Message).
type terminalTimeout struct {
	Status       enumspb.ActivityExecutionStatus
	TimeoutType  enumspb.TimeoutType
	CauseType    string
	CauseMessage string
}

// reproTimeout is the timeout under test, kept short so it fires within the test.
const reproTimeout = 2 * time.Second

// reproCauseType is the Type and Message of the retryable application failure driven on the first
// attempt; a terminal timeout must chain it verbatim as its Cause.
const reproCauseType = "drive"

// reproCause is the retryable application failure driven on the first attempt.
var reproCause = &failurepb.Failure{
	Message: reproCauseType,
	FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
		ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: reproCauseType},
	},
}

// --- standalone-activity driver ------------------------------------------------------------

// saaDriver drives one standalone activity through the frontend RPCs.
type saaDriver struct {
	s   *standaloneActivityTestSuite
	env *standaloneActivityEnv

	activityID string
	runID      string
	taskQueue  string
	taskToken  []byte
}

func (d *saaDriver) start_Poll_StartToCloseTimeoutElapses(t *testing.T) enumspb.ActivityExecutionStatus {
	d.startWithNonRetryableTimeout(t, enumspb.TIMEOUT_TYPE_START_TO_CLOSE)
	d.taskToken = pollTask(t, d.s, d.env, d.taskQueue)
	d.pollActivityExecution(t)
	return d.describeActivity(t).GetInfo().GetStatus()
}

func (d *saaDriver) start_Poll_HeartbeatTimeoutElapses(t *testing.T) enumspb.ActivityExecutionStatus {
	d.startWithNonRetryableTimeout(t, enumspb.TIMEOUT_TYPE_HEARTBEAT)
	d.taskToken = pollTask(t, d.s, d.env, d.taskQueue)
	d.pollActivityExecution(t)
	return d.describeActivity(t).GetInfo().GetStatus()
}

// pollActivityExecution long-polls until the activity reaches a terminal outcome.
func (d *saaDriver) pollActivityExecution(t *testing.T) *workflowservice.PollActivityExecutionResponse {
	resp, err := d.env.FrontendClient().PollActivityExecution(d.s.Context(), &workflowservice.PollActivityExecutionRequest{
		Namespace:  d.env.Namespace().String(),
		ActivityId: d.activityID,
		RunId:      d.runID,
	})
	require.NoError(t, err)
	return resp
}

// startWithNonRetryableTimeout starts an activity with the given timeout short and listed in
// NonRetryableErrorTypes; every other timeout is long.
func (d *saaDriver) startWithNonRetryableTimeout(t *testing.T, timeoutType enumspb.TimeoutType) {
	id := testcore.RandomizeStr(t.Name())
	req := &workflowservice.StartActivityExecutionRequest{
		Namespace:           d.env.Namespace().String(),
		ActivityId:          id,
		ActivityType:        d.env.Tv().ActivityType(),
		Identity:            "worker",
		Input:               defaultInput,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: id},
		StartToCloseTimeout: durationpb.New(time.Hour),
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:        durationpb.New(200 * time.Millisecond),
			BackoffCoefficient:     1.0,
			MaximumInterval:        durationpb.New(200 * time.Millisecond),
			MaximumAttempts:        3,
			NonRetryableErrorTypes: []string{retrypolicy.TimeoutFailureTypePrefix + timeoutType.String()},
		},
		RequestId: uuid.NewString(),
	}
	switch timeoutType {
	case enumspb.TIMEOUT_TYPE_START_TO_CLOSE:
		req.StartToCloseTimeout = durationpb.New(reproTimeout)
	case enumspb.TIMEOUT_TYPE_HEARTBEAT:
		req.HeartbeatTimeout = durationpb.New(reproTimeout)
	}
	resp, err := d.env.FrontendClient().StartActivityExecution(d.s.Context(), req)
	require.NoError(t, err)
	d.activityID, d.taskQueue, d.runID = id, id, resp.RunId
}

// describeActivity returns the DescribeActivityExecution response.
func (d *saaDriver) describeActivity(t *testing.T) *workflowservice.DescribeActivityExecutionResponse {
	resp, err := d.env.FrontendClient().DescribeActivityExecution(d.s.Context(), &workflowservice.DescribeActivityExecutionRequest{
		Namespace:  d.env.Namespace().String(),
		ActivityId: d.activityID,
		RunId:      d.runID,
	})
	require.NoError(t, err)
	return resp
}

func (d *saaDriver) start_Poll_Fail_Poll_StartToCloseTimeoutElapses(t *testing.T) terminalTimeout {
	d.startForCauseChaining(t, enumspb.TIMEOUT_TYPE_START_TO_CLOSE)
	failTask(t, d.s, d.env, d.taskQueue)
	pollTask(t, d.s, d.env, d.taskQueue) // start the final attempt, then never respond so it times out
	return d.awaitTerminalTimeout(t)
}

func (d *saaDriver) start_Poll_Fail_Poll_HeartbeatTimeoutElapses(t *testing.T) terminalTimeout {
	d.startForCauseChaining(t, enumspb.TIMEOUT_TYPE_HEARTBEAT)
	failTask(t, d.s, d.env, d.taskQueue)
	pollTask(t, d.s, d.env, d.taskQueue) // start the final attempt, then never heartbeat so it times out
	return d.awaitTerminalTimeout(t)
}

func (d *saaDriver) start_Poll_Fail_ScheduleToCloseTimeoutElapses(t *testing.T) terminalTimeout {
	d.startForCauseChaining(t, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE)
	failTask(t, d.s, d.env, d.taskQueue)
	// Don't poll the retry; the schedule-to-close deadline closes the activity while it backs off.
	return d.awaitTerminalTimeout(t)
}

// startForCauseChaining starts an activity that fails once retryably and then times out, so a terminal
// timeout has a prior application failure to chain. Retries are exhausted by MaximumAttempts
// (StartToClose/Heartbeat) or by the schedule-to-close deadline.
func (d *saaDriver) startForCauseChaining(t *testing.T, timeoutType enumspb.TimeoutType) {
	id := testcore.RandomizeStr(t.Name())
	req := &workflowservice.StartActivityExecutionRequest{
		Namespace:           d.env.Namespace().String(),
		ActivityId:          id,
		ActivityType:        d.env.Tv().ActivityType(),
		Identity:            "worker",
		Input:               defaultInput,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: id},
		StartToCloseTimeout: durationpb.New(time.Hour),
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:    durationpb.New(200 * time.Millisecond),
			BackoffCoefficient: 1.0,
			MaximumInterval:    durationpb.New(200 * time.Millisecond),
			MaximumAttempts:    2,
		},
		RequestId: uuid.NewString(),
	}
	switch timeoutType {
	case enumspb.TIMEOUT_TYPE_START_TO_CLOSE:
		req.StartToCloseTimeout = durationpb.New(reproTimeout)
	case enumspb.TIMEOUT_TYPE_HEARTBEAT:
		req.HeartbeatTimeout = durationpb.New(reproTimeout)
	case enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE:
		// Measured from schedule time (unlike the others, from Started), so it must outlast the first
		// attempt's failure before it closes the backing-off activity.
		req.ScheduleToCloseTimeout = durationpb.New(3 * reproTimeout)
		req.RetryPolicy.MaximumAttempts = 0 // the deadline, not the attempt count, ends the activity
	}
	resp, err := d.env.FrontendClient().StartActivityExecution(d.s.Context(), req)
	require.NoError(t, err)
	d.activityID, d.taskQueue, d.runID = id, id, resp.RunId
}

// awaitTerminalTimeout long-polls until the activity times out, then reports the terminal timeout and
// the application failure it chains as its Cause.
func (d *saaDriver) awaitTerminalTimeout(t *testing.T) terminalTimeout {
	failure := d.pollActivityExecution(t).GetOutcome().GetFailure()
	cause := failure.GetCause()
	return terminalTimeout{
		Status:       d.describeActivity(t).GetInfo().GetStatus(),
		TimeoutType:  failure.GetTimeoutFailureInfo().GetTimeoutType(),
		CauseType:    cause.GetApplicationFailureInfo().GetType(),
		CauseMessage: cause.GetMessage(),
	}
}

// --- workflow-activity driver --------------------------------------------------------------

// wfaDriver drives one activity scheduled by a helper workflow.
type wfaDriver struct {
	s   *standaloneActivityTestSuite
	env *standaloneActivityEnv

	run       sdkclient.WorkflowRun
	taskQueue string
	taskToken []byte
}

func (d *wfaDriver) start_Poll_StartToCloseTimeoutElapses(t *testing.T) enumspb.ActivityExecutionStatus {
	d.startWithNonRetryableTimeout(t, enumspb.TIMEOUT_TYPE_START_TO_CLOSE)
	d.taskToken = pollTask(t, d.s, d.env, d.taskQueue)
	return d.awaitTerminalStatus(t)
}

func (d *wfaDriver) start_Poll_HeartbeatTimeoutElapses(t *testing.T) enumspb.ActivityExecutionStatus {
	d.startWithNonRetryableTimeout(t, enumspb.TIMEOUT_TYPE_HEARTBEAT)
	d.taskToken = pollTask(t, d.s, d.env, d.taskQueue)
	return d.awaitTerminalStatus(t)
}

// startWithNonRetryableTimeout starts a workflow that schedules one activity with the given timeout short
// and listed in NonRetryableErrorTypes.
func (d *wfaDriver) startWithNonRetryableTimeout(t *testing.T, timeoutType enumspb.TimeoutType) {
	wfTQ := testcore.RandomizeStr("parity-wf")
	d.taskQueue = testcore.RandomizeStr("parity-act")

	w := sdkworker.New(d.env.SdkClient(), wfTQ, sdkworker.Options{})
	w.RegisterWorkflow(singleActivityWorkflow)
	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)

	p := workflowActivityParams{
		TaskQueue:              d.taskQueue,
		StartToClose:           time.Hour,
		MaxAttempts:            3,
		NonRetryableErrorTypes: []string{retrypolicy.TimeoutFailureTypePrefix + timeoutType.String()},
	}
	switch timeoutType {
	case enumspb.TIMEOUT_TYPE_START_TO_CLOSE:
		p.StartToClose = reproTimeout
	case enumspb.TIMEOUT_TYPE_HEARTBEAT:
		p.Heartbeat = reproTimeout
	}
	run, err := d.env.SdkClient().ExecuteWorkflow(d.s.Context(),
		sdkclient.StartWorkflowOptions{ID: testcore.RandomizeStr("parity-run"), TaskQueue: wfTQ},
		singleActivityWorkflow, p)
	require.NoError(t, err)
	d.run = run
}

// awaitTerminalStatus waits for the workflow to close and returns the activity's terminal status.
func (d *wfaDriver) awaitTerminalStatus(t *testing.T) enumspb.ActivityExecutionStatus {
	err := d.run.Get(d.s.Context(), nil)
	if err == nil {
		return enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED
	}
	if _, ok := errors.AsType[*temporal.CanceledError](err); ok {
		return enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED
	}
	var actErr *temporal.ActivityError
	require.ErrorAs(t, err, &actErr)
	switch actErr.Unwrap().(type) {
	case *temporal.TimeoutError:
		return enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT
	case *temporal.ApplicationError:
		return enumspb.ACTIVITY_EXECUTION_STATUS_FAILED
	default:
		return enumspb.ACTIVITY_EXECUTION_STATUS_FAILED
	}
}

func (d *wfaDriver) start_Poll_Fail_Poll_StartToCloseTimeoutElapses(t *testing.T) terminalTimeout {
	d.startForCauseChaining(t, enumspb.TIMEOUT_TYPE_START_TO_CLOSE)
	failTask(t, d.s, d.env, d.taskQueue)
	pollTask(t, d.s, d.env, d.taskQueue) // start the final attempt, then never respond so it times out
	return d.awaitTerminalTimeout(t)
}

func (d *wfaDriver) start_Poll_Fail_Poll_HeartbeatTimeoutElapses(t *testing.T) terminalTimeout {
	d.startForCauseChaining(t, enumspb.TIMEOUT_TYPE_HEARTBEAT)
	failTask(t, d.s, d.env, d.taskQueue)
	pollTask(t, d.s, d.env, d.taskQueue) // start the final attempt, then never heartbeat so it times out
	return d.awaitTerminalTimeout(t)
}

func (d *wfaDriver) start_Poll_Fail_ScheduleToCloseTimeoutElapses(t *testing.T) terminalTimeout {
	d.startForCauseChaining(t, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE)
	failTask(t, d.s, d.env, d.taskQueue)
	// Don't poll the retry; the schedule-to-close deadline closes the activity while it backs off.
	return d.awaitTerminalTimeout(t)
}

// startForCauseChaining starts a workflow that schedules one activity which fails once retryably and
// then times out, exhausting retries by MaximumAttempts (StartToClose/Heartbeat) or the schedule-to-
// close deadline.
func (d *wfaDriver) startForCauseChaining(t *testing.T, timeoutType enumspb.TimeoutType) {
	wfTQ := testcore.RandomizeStr("parity-wf")
	d.taskQueue = testcore.RandomizeStr("parity-act")

	w := sdkworker.New(d.env.SdkClient(), wfTQ, sdkworker.Options{})
	w.RegisterWorkflow(singleActivityWorkflow)
	require.NoError(t, w.Start())
	t.Cleanup(w.Stop)

	p := workflowActivityParams{TaskQueue: d.taskQueue, StartToClose: time.Hour, MaxAttempts: 2}
	switch timeoutType {
	case enumspb.TIMEOUT_TYPE_START_TO_CLOSE:
		p.StartToClose = reproTimeout
	case enumspb.TIMEOUT_TYPE_HEARTBEAT:
		p.Heartbeat = reproTimeout
	case enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE:
		// Measured from schedule time (unlike the others, from Started), so it must outlast worker
		// startup and the first attempt's failure before it closes the backing-off activity.
		p.ScheduleToClose = 3 * reproTimeout
		p.MaxAttempts = 0 // the deadline, not the attempt count, ends the activity
	}
	run, err := d.env.SdkClient().ExecuteWorkflow(d.s.Context(),
		sdkclient.StartWorkflowOptions{ID: testcore.RandomizeStr("parity-run"), TaskQueue: wfTQ},
		singleActivityWorkflow, p)
	require.NoError(t, err)
	d.run = run
}

// awaitTerminalTimeout waits for the workflow to close and reports the activity's terminal timeout and
// the application failure it chains as its Cause (surfaced by the SDK via TimeoutError.Unwrap).
func (d *wfaDriver) awaitTerminalTimeout(t *testing.T) terminalTimeout {
	err := d.run.Get(d.s.Context(), nil)
	var toErr *temporal.TimeoutError
	require.ErrorAs(t, err, &toErr)
	got := terminalTimeout{Status: enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, TimeoutType: toErr.TimeoutType()}
	if appErr, ok := errors.AsType[*temporal.ApplicationError](toErr.Unwrap()); ok {
		got.CauseType, got.CauseMessage = appErr.Type(), appErr.Message()
	}
	return got
}

// workflowActivityParams configures the single activity the helper workflow schedules.
type workflowActivityParams struct {
	TaskQueue              string
	StartToClose           time.Duration
	Heartbeat              time.Duration // 0 = unset
	ScheduleToClose        time.Duration // 0 = unset
	MaxAttempts            int32
	NonRetryableErrorTypes []string
}

// singleActivityWorkflow is a workflow that schedules one activity with the given options and returns its result.
func singleActivityWorkflow(ctx workflow.Context, p workflowActivityParams) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:              p.TaskQueue,
		ActivityID:             "act",
		StartToCloseTimeout:    p.StartToClose,
		HeartbeatTimeout:       p.Heartbeat,
		ScheduleToCloseTimeout: p.ScheduleToClose,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        200 * time.Millisecond,
			BackoffCoefficient:     1.0,
			MaximumInterval:        200 * time.Millisecond,
			MaximumAttempts:        p.MaxAttempts,
			NonRetryableErrorTypes: p.NonRetryableErrorTypes,
		},
	})
	return workflow.ExecuteActivity(ctx, "noopActivity").Get(ctx, nil)
}

// pollTask fetches a pending activity task, returning its token.
func pollTask(t *testing.T, s *standaloneActivityTestSuite, env *standaloneActivityEnv, taskQueue string) []byte {
	ctx, cancel := context.WithTimeout(s.Context(), 10*time.Second)
	defer cancel()
	resp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "worker",
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp.GetActivityId(), "no task was dispatched")
	return resp.GetTaskToken()
}

// failTask polls the next activity task and fails it with reproCause (a retryable application error),
// so a subsequent terminal timeout has an underlying failure to chain as its Cause.
func failTask(t *testing.T, s *standaloneActivityTestSuite, env *standaloneActivityEnv, taskQueue string) {
	_, err := env.FrontendClient().RespondActivityTaskFailed(s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
		Namespace: env.Namespace().String(),
		Identity:  "worker",
		TaskToken: pollTask(t, s, env, taskQueue),
		Failure:   reproCause,
	})
	require.NoError(t, err)
}
