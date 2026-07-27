package tests

// Shared by the two activity drivers, activity_standalone_driver.go and activity_workflow_driver.go:
// the activity a trace configures, the activity info both surfaces expose, and the event vocabulary
// and waiting machinery that belongs to neither.

import (
	"cmp"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/common/payloads"
	"google.golang.org/protobuf/types/known/durationpb"
)

// activityConfig is the activity a driver starts. One value configures either surface, so a parity
// test describes a single activity rather than two that might differ.
//
// Every field is the value it names. A zero duration leaves that option unset, which for a timeout
// means it never fires; the exceptions are noted.
//
// Timeouts are usually left unset: forTrace gives a short window to each one the trace fires, so
// writing model.HeartbeatElapses is itself the statement that this activity has a heartbeat timeout.
// Set one explicitly only to say something the trace cannot — that it exists without firing, or that
// its exact duration is what the test is about.
type activityConfig struct {
	MaxAttempts            int32         // RetryPolicy MaximumAttempts; 0 = unlimited
	RetryInterval          time.Duration // RetryPolicy InitialInterval; 0 => activityDefaultRetryInterval
	BackoffCoefficient     float64       // RetryPolicy BackoffCoefficient; 0 => 1.0 (constant interval)
	MaxRetryInterval       time.Duration // RetryPolicy MaximumInterval; 0 => RetryInterval
	NextRetryDelay         time.Duration // ApplicationFailureInfo.NextRetryDelay sent with RespondFailed
	NonRetryableErrorTypes []string      // RetryPolicy NonRetryableErrorTypes

	StartToClose    time.Duration // 0 => activityLongTimeout, so it does not fire
	ScheduleToClose time.Duration
	ScheduleToStart time.Duration
	Heartbeat       time.Duration
	StartDelay      time.Duration // SAA only: WFA has no per-activity start delay
}

// activityParityDefaultInput is the payload the drivers start activities with. Its content is never asserted on.
var activityParityDefaultInput = payloads.EncodeString("Input")

// activityLongTimeout is a timeout long enough not to fire during a test.
const activityLongTimeout = time.Hour

// activityShortTimeout is a timeout short enough for a trace to wait out.
const activityShortTimeout = 2 * time.Second

// activityLongRetryInterval is a retry interval long enough to observe an activity while it is still
// backing off.
const activityLongRetryInterval = 30 * time.Second

// activityShortRetryInterval is a retry interval short enough for a trace to wait the backoff out. Not
// much shorter is useful: a timer task's fire time is floored at now + TimerProcessorMaxTimeShift (~1s).
const activityShortRetryInterval = 1 * time.Second

// activityLongStartDelay is a start delay long enough to keep the first attempt pending for a whole test.
const activityLongStartDelay = time.Hour

func (c activityConfig) retryInterval() time.Duration {
	return cmp.Or(c.RetryInterval, activityDefaultRetryInterval)
}

func (c activityConfig) startToClose() time.Duration {
	return cmp.Or(c.StartToClose, activityLongTimeout)
}

// forTrace is the config with a short window for each timeout the trace fires, so that it can. A
// timeout the author set is left alone: only they can say how long a timeout that the trace does not
// fire should be, or that a fired one has a duration the test depends on.
func (c activityConfig) forTrace(trace []model.Event) activityConfig {
	for _, e := range trace {
		switch e.Type {
		case model.ScheduleToStartElapsesType:
			c.ScheduleToStart = cmp.Or(c.ScheduleToStart, activityShortTimeout)
		case model.ScheduleToCloseElapsesType:
			c.ScheduleToClose = cmp.Or(c.ScheduleToClose, activityShortTimeout)
		case model.StartToCloseElapsesType:
			c.StartToClose = cmp.Or(c.StartToClose, activityShortTimeout)
		case model.HeartbeatElapsesType:
			c.Heartbeat = cmp.Or(c.Heartbeat, activityShortTimeout)
		}
	}
	return c
}

// window is how long the clock behind a wall-clock event takes to elapse, from the option that event
// fires on. Zero for an event whose option is not configured, which no trace should drive.
func (c activityConfig) window(e model.Event) time.Duration {
	switch e.Type {
	case model.StartDelayElapsesType:
		return c.StartDelay
	case model.BackoffElapsesType:
		// The first backoff only: a later one is longer under a non-constant policy. Waiting for a
		// dispatch uses the server's schedule time instead; see awaitDispatchTimePassed.
		return cmp.Or(c.NextRetryDelay, c.retryInterval())
	case model.StartToCloseElapsesType:
		return c.startToClose()
	case model.ScheduleToCloseElapsesType:
		return c.ScheduleToClose
	case model.ScheduleToStartElapsesType:
		return c.ScheduleToStart
	case model.HeartbeatElapsesType:
		return c.Heartbeat
	default:
		return 0
	}
}

// modelConfig is the model's view of the activity: which options are configured at all. Deriving it
// means the two cannot disagree.
func (c activityConfig) modelConfig() model.Config {
	return model.Config{
		MaxAttempts:        c.MaxAttempts,
		HasStartDelay:      c.StartDelay > 0,
		HasScheduleToClose: c.ScheduleToClose > 0,
		HasScheduleToStart: c.ScheduleToStart > 0,
		HasHeartbeat:       c.Heartbeat > 0,
	}
}

// activityDefaultRetryInterval is the RetryPolicy InitialInterval when a driver sets none.
const activityDefaultRetryInterval = 200 * time.Millisecond

// activityDriverPositivePollTimeout bounds a poll that must find a task.
const activityDriverPositivePollTimeout = 10 * time.Second

// activityDriverWallClockSettle is slack added to a wall-clock event's window when waiting for its effect.
const activityDriverWallClockSettle = 2 * time.Second

// activityDriverPollInterval is the gap between reads when polling for a wall-clock event's effect.
const activityDriverPollInterval = 100 * time.Millisecond

// activityDriverTerminalTimeout bounds the wait for an activity the trace has driven to a terminal status.
const activityDriverTerminalTimeout = 10 * time.Second

// timeoutType is the TimeoutType a timeout-elapse event reports when it fires,
// TIMEOUT_TYPE_UNSPECIFIED for any other event. The model names no API types, so the correspondence
// lives here.
func timeoutType(e model.Event) enumspb.TimeoutType {
	switch e.Type {
	case model.ScheduleToStartElapsesType:
		return enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START
	case model.ScheduleToCloseElapsesType:
		return enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE
	case model.StartToCloseElapsesType:
		return enumspb.TIMEOUT_TYPE_START_TO_CLOSE
	case model.HeartbeatElapsesType:
		return enumspb.TIMEOUT_TYPE_HEARTBEAT
	default:
		return enumspb.TIMEOUT_TYPE_UNSPECIFIED
	}
}

// validateTrace rejects a trace no activity could produce: one that drives an event when the clock
// behind it is not running. Config is passed after forTrace, so a timeout the trace fires is
// configured by then and only the state conditions remain to be checked.
//
// Only driveTrace validates. driveTraceWithModelConformanceChecking drives stopped clocks on purpose,
// to assert the server does nothing, and the model already predicts that.
func validateTrace(t require.TestingT, cfg activityConfig, trace []model.Event) {
	require.NoError(t, model.ValidateTrace(cfg.modelConfig(), trace))
}

// isWallClockEvent reports whether an event fires on wall-clock time rather than synchronously.
func isWallClockEvent(k model.EventType) bool {
	switch k {
	case model.ScheduleToStartElapsesType, model.ScheduleToCloseElapsesType, model.StartToCloseElapsesType,
		model.HeartbeatElapsesType, model.StartDelayElapsesType, model.BackoffElapsesType:
		return true
	default:
		return false
	}
}

// isDispatchDelayEvent reports whether an event is a dispatch-delay window elapsing rather than a timeout.
// A dispatch delay advances no transition-history version; its effect is the pending dispatch time
// passing.
func isDispatchDelayEvent(k model.EventType) bool {
	return k == model.StartDelayElapsesType || k == model.BackoffElapsesType
}

func activityFailure(retryable bool, nextRetryDelay time.Duration) *failurepb.Failure {
	info := &failurepb.ApplicationFailureInfo{Type: "drive", NonRetryable: !retryable}
	if nextRetryDelay > 0 {
		info.NextRetryDelay = durationpb.New(nextRetryDelay)
	}
	return &failurepb.Failure{
		Message:     "drive",
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: info},
	}
}

// activityTimeoutMark is the timeouts an activity reports, and enough of its history to tell a fresh
// one from a leftover. A timeout can appear in more than one place: an attempt ended by a heartbeat
// timeout that then has no room to retry closes the activity as schedule-to-close, and both are true
// of it.
type activityTimeoutMark struct {
	attemptFailure enumspb.TimeoutType // ended the last attempt
	outcome        enumspb.TimeoutType // closed the activity
	cause          enumspb.TimeoutType // chained by outcome as what led to it
	attempt        int32
	closed         bool
}

// reports says whether the activity reports tt as having occurred.
func (m activityTimeoutMark) reports(tt enumspb.TimeoutType) bool {
	return tt != enumspb.TIMEOUT_TYPE_UNSPECIFIED &&
		(m.attemptFailure == tt || m.outcome == tt || m.cause == tt)
}

func timeoutTypeOf(f *failurepb.Failure) enumspb.TimeoutType {
	return f.GetTimeoutFailureInfo().GetTimeoutType()
}

// activityDriverPollUntil reports whether cond held before the deadline, reading every activityDriverPollInterval.
//
// common/testing/await is the usual way to write this, but await.Require and await.RequireTrue take a
// testing.TB, which has an unexported method and so admits only *testing.T. The drivers take a
// require.TestingT instead, which is what lets their self-tests hand them a recorder and assert on
// what they reported.
func activityDriverPollUntil(deadline time.Time, cond func() bool) bool {
	for {
		if cond() {
			return true
		}
		if !time.Now().Before(deadline) {
			return false
		}
		time.Sleep(activityDriverPollInterval)
	}
}

// activityHeartbeatDetails is the checkpoint payload both drivers send with a Heartbeat event.
var activityHeartbeatDetails = &commonpb.Payloads{Payloads: []*commonpb.Payload{{
	Metadata: map[string][]byte{"encoding": []byte("json/plain")},
	Data:     []byte(`"hb"`),
}}}

func firstPayloadData(p *commonpb.Payloads) []byte {
	if ps := p.GetPayloads(); len(ps) > 0 {
		return ps[0].GetData()
	}
	return nil
}

// activityInfo is user-visible activity state projected out of the two different messages that
// carry it: SAA's ActivityExecutionInfo and WFA's PendingActivityInfo.
//
// CurrentRetryInterval is rounded to the second, because WFA derives it by subtracting two stored
// timestamps while SAA stores it exactly. NextAttemptScheduleTime is reduced to whether it is set
// to facilitate test assertions.
type activityInfo struct {
	RunState                   enumspb.PendingActivityState
	Attempt                    int32
	CurrentRetryInterval       time.Duration
	NextAttemptScheduleTimeSet bool
}

// activityTerminalProjection is the terminal status plus the failure discriminant a user sees: the
// application failure Type for FAILED, the TimeoutType string for TIMED_OUT, empty otherwise.
type activityTerminalProjection struct {
	Status      enumspb.ActivityExecutionStatus
	FailureType string
}

// failureCause is the Type and Message of the failure a terminal outcome chains as its Cause.
type failureCause struct {
	Type    string
	Message string
}
