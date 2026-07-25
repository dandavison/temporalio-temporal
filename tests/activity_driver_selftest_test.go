package tests

// Self-tests for the activity drivers: when a scripted event's effect never arrives, the driver must
// say so. Without this, a trace can silently fail to reach the state it scripts and the test goes on to
// assert against whatever state it did reach.
//
// Each case injects a mismatch between what the harness believes it configured and what the server
// actually got, via customizeStart — the documented escape hatch for start-time config the harness does
// not model. So the injection does not depend on any current harness defect.
//
// The driver reports through the require.TestingT it is handed, so these tests hand it a recorder and
// assert on what it recorded, rather than failing themselves.

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/activity/model"
	"google.golang.org/protobuf/types/known/durationpb"
)

// recordingT collects what the driver reports instead of failing the enclosing test.
type recordingT struct{ failures []string }

var errRecordedFailNow = fmt.Errorf("recordingT.FailNow")

func (r *recordingT) Errorf(format string, args ...any) {
	r.failures = append(r.failures, fmt.Sprintf(format, args...))
}

func (r *recordingT) FailNow() { panic(errRecordedFailNow) }

// recordDriverReports runs drive against a recorder and returns everything the driver reported.
func recordDriverReports(drive func(require.TestingT)) (failures []string) {
	rt := &recordingT{}
	defer func() {
		if p := recover(); p != nil && p != errRecordedFailNow {
			panic(p)
		}
		failures = rt.failures
	}()
	drive(rt)
	return rt.failures
}

// TestSAADriverReportsUnrealizedWallClockEvents drives a trace whose wall-clock event provably cannot
// take effect inside the window the driver waits, and requires the driver to report it. Each case is
// paired with an uninjected control, so a check that always fails cannot pass these tests.
func (s *standaloneActivityTestSuite) TestSAADriverReportsUnrealizedWallClockEvents() {
	env := s.newTestEnv()

	// realRetryInterval and realStartToClose are far longer than the windows the harness derives below,
	// so the corresponding event cannot possibly have taken effect when the driver moves on.
	const realRetryInterval, realStartToClose = 30 * time.Second, 30 * time.Second

	s.T().Run("BackoffElapses", func(t *testing.T) {
		drive := func(customize func(*workflowservice.StartActivityExecutionRequest)) []string {
			return recordDriverReports(func(rt require.TestingT) {
				h := newSAAHarness(t, env, model.Config{MaxAttempts: 3})
				h.retryInterval = time.Second // the window the driver will wait out
				h.customizeStart = customize
				h.driveTrace(rt, []model.Event{saaPoll, saaFailRetryably, saaBackoffDelayElapse})
			})
		}

		require.Empty(t, drive(nil), "control: an uninjected trace must drive cleanly")

		injected := drive(func(req *workflowservice.StartActivityExecutionRequest) {
			req.RetryPolicy.InitialInterval = durationpb.New(realRetryInterval)
			req.RetryPolicy.MaximumInterval = durationpb.New(realRetryInterval)
		})
		require.NotEmpty(t, injected,
			"the retry was still backing off when the driver moved past BackoffElapses; the driver must report that")
	})

	s.T().Run("StartToCloseElapses", func(t *testing.T) {
		drive := func(customize func(*workflowservice.StartActivityExecutionRequest)) []string {
			return recordDriverReports(func(rt require.TestingT) {
				h := newSAAHarness(t, env, model.Config{MaxAttempts: 1})
				h.shortTimeout = model.StartToCloseElapses // the window the driver will wait out
				h.customizeStart = customize
				h.driveTrace(rt, []model.Event{saaPoll, saaStartToCloseElapse})
			})
		}

		require.Empty(t, drive(nil), "control: an uninjected trace must drive cleanly")

		injected := drive(func(req *workflowservice.StartActivityExecutionRequest) {
			req.StartToCloseTimeout = durationpb.New(realStartToClose)
		})
		require.NotEmpty(t, injected,
			"the attempt was still running when the driver moved past StartToCloseElapses; the driver must report that")
	})
}

// TestSAAHarnessRejectsInconsistentConfig requires the harness to refuse a configuration in which the
// activity it starts and the model.Config it reports do not describe the same activity. saaHarness takes
// its start-time config from two places — the model.Config flags and the timing knobs — and nothing
// relates them, so a knob can be silently dropped (a scheduleToClose with no HasScheduleToClose) or a
// timeout can be configured that the model does not know about (a startDelay with no HasStartDelay).
//
// Both directions are harness bugs that surface as product findings: a parity test whose SAA side
// silently lacks a timeout its WFA side has fails on SAA and reads as a divergence.
func (s *standaloneActivityTestSuite) TestSAAHarnessRejectsInconsistentConfig() {
	env := s.newTestEnv()

	for _, tc := range []struct {
		name  string
		cfg   model.Config
		knobs func(*saaHarness)
		why   string
	}{
		{
			name:  "scheduleToCloseWithoutConfigFlag",
			cfg:   model.Config{MaxAttempts: 1},
			knobs: func(h *saaHarness) { h.scheduleToClose = 10 * time.Second },
			why:   "startRequest drops scheduleToClose unless cfg.HasScheduleToClose is set",
		},
		{
			name:  "shortHeartbeatTimeoutWithoutConfigFlag",
			cfg:   model.Config{MaxAttempts: 1},
			knobs: func(h *saaHarness) { h.shortTimeout = model.HeartbeatElapses },
			why:   "no heartbeat timeout is configured at all, so a HeartbeatElapses event can never fire",
		},
		{
			name:  "startDelayWithoutConfigFlag",
			cfg:   model.Config{MaxAttempts: 1},
			knobs: func(h *saaHarness) { h.startDelay = time.Hour },
			why:   "the activity is start-delayed but the model believes it is immediately dispatchable",
		},
	} {
		s.T().Run(tc.name, func(t *testing.T) {
			reports := recordDriverReports(func(rt require.TestingT) {
				h := newSAAHarness(t, env, tc.cfg)
				tc.knobs(h)
				h.start(rt)
			})
			require.NotEmpty(t, reports, "the harness must reject this configuration: %s", tc.why)
		})
	}

	// Control: a consistent configuration must start cleanly, so a check that rejects everything cannot
	// pass this test.
	s.T().Run("consistentConfigIsAccepted", func(t *testing.T) {
		reports := recordDriverReports(func(rt require.TestingT) {
			h := newSAAHarness(t, env, model.Config{MaxAttempts: 1, HasScheduleToClose: true, HasHeartbeat: true})
			h.scheduleToClose = 10 * time.Second
			h.shortTimeout = model.HeartbeatElapses
			h.start(rt)
		})
		require.Empty(t, reports, "a consistent configuration must be accepted")
	})
}

// TestSAADriverAttributesAnUnexpectedDispatch requires the negative poll — the check that a delayed
// activity does not dispatch — to distinguish two situations it previously reported identically, as a
// product divergence:
//
//   - the activity dispatched before it was due, which is a product bug
//   - the delay window closed before or during the check, so the task it saw was legitimately
//     dispatched and the check simply outlived what it was checking
//
// The second is a harness failure and must say so. Reporting it as "a task WAS dispatched" sends the
// reader looking for a product bug that is not there.
//
// Injected via customizeStart, which strips the start delay the harness believes it configured, so the
// activity is immediately dispatchable while the model still expects a pending first dispatch.
func (s *standaloneActivityTestSuite) TestSAADriverAttributesAnUnexpectedDispatch() {
	env := s.newTestEnv()
	t := s.T()

	reports := recordDriverReports(func(rt require.TestingT) {
		h := newSAAHarness(t, env, model.Config{HasStartDelay: true})
		h.startDelay = time.Hour // the model believes the first dispatch is an hour away
		h.customizeStart = func(req *workflowservice.StartActivityExecutionRequest) {
			req.StartDelay = nil // ... but the server dispatches at once
		}
		a := h.start(rt)
		cur := model.Initial(h.cfg)
		a.apply(rt, model.Event{Kind: model.Poll}, cur, model.Transition(h.cfg, cur, model.Event{Kind: model.Poll}), true)
	})

	require.NotEmpty(t, reports, "the driver must report that its no-dispatch check did not hold")
	joined := strings.Join(reports, "\n")
	require.Contains(t, joined, "harness:",
		"an unexpected dispatch that was not early must be attributed to the harness, not the product")
	require.NotContains(t, joined, "a task WAS dispatched",
		"this must not be reported as a product divergence")
}
