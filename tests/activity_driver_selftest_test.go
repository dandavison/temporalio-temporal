package tests

// Self-tests for the activity drivers: when a scripted event's effect never arrives, the driver must
// say so. Without this, a trace can silently fail to reach the state it scripts and the test goes on to
// assert against whatever state it did reach.
//
// Each case injects a mismatch between what the driver believes it configured and what the server
// actually got, via customizeStart — the documented escape hatch for start-time config the driver does
// not model. So the injection does not depend on any current driver defect.
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

	// realRetryInterval and realStartToClose are far longer than the windows the driver derives below,
	// so the corresponding event cannot possibly have taken effect when the driver moves on.
	const realRetryInterval, realStartToClose = 30 * time.Second, 30 * time.Second

	s.T().Run("BackoffElapses", func(t *testing.T) {
		drive := func(customize func(*workflowservice.StartActivityExecutionRequest)) []string {
			return recordDriverReports(func(rt require.TestingT) {
				d := newSAADriver(t, env, model.Config{MaxAttempts: 3})
				d.retryInterval = time.Second // the window the driver will wait out
				d.customizeStart = customize
				d.driveTrace(rt, []model.Event{model.Poll, model.FailRetryably, model.BackoffElapses})
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
				d := newSAADriver(t, env, model.Config{MaxAttempts: 1})
				d.shortTimeout = model.StartToCloseElapsesKind // the window the driver will wait out
				d.customizeStart = customize
				d.driveTrace(rt, []model.Event{model.Poll, model.StartToCloseElapses})
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

// TestSAADriverRejectsInconsistentConfig requires the driver to refuse a configuration in which the
// activity it starts and the model.Config it reports do not describe the same activity. saaDriver takes
// its start-time config from two places — the model.Config flags and the timing knobs — and nothing
// relates them, so a knob can be silently dropped (a scheduleToClose with no HasScheduleToClose) or a
// timeout can be configured that the model does not know about (a startDelay with no HasStartDelay).
//
// Both directions are driver bugs that surface as product findings: a parity test whose SAA side
// silently lacks a timeout its WFA side has fails on SAA and reads as a divergence.
func (s *standaloneActivityTestSuite) TestSAADriverRejectsInconsistentConfig() {
	env := s.newTestEnv()

	for _, tc := range []struct {
		name  string
		cfg   model.Config
		knobs func(*saaDriver)
		why   string
	}{
		{
			name:  "scheduleToCloseWithoutConfigFlag",
			cfg:   model.Config{MaxAttempts: 1},
			knobs: func(d *saaDriver) { d.scheduleToClose = 10 * time.Second },
			why:   "startRequest drops scheduleToClose unless cfg.HasScheduleToClose is set",
		},
		{
			name:  "shortHeartbeatTimeoutWithoutConfigFlag",
			cfg:   model.Config{MaxAttempts: 1},
			knobs: func(d *saaDriver) { d.shortTimeout = model.HeartbeatElapsesKind },
			why:   "no heartbeat timeout is configured at all, so a HeartbeatElapses event can never fire",
		},
		{
			name:  "startDelayWithoutConfigFlag",
			cfg:   model.Config{MaxAttempts: 1},
			knobs: func(d *saaDriver) { d.startDelay = time.Hour },
			why:   "the activity is start-delayed but the model believes it is immediately dispatchable",
		},
	} {
		s.T().Run(tc.name, func(t *testing.T) {
			reports := recordDriverReports(func(rt require.TestingT) {
				d := newSAADriver(t, env, tc.cfg)
				tc.knobs(d)
				d.start(rt)
			})
			require.NotEmpty(t, reports, "the driver must reject this configuration: %s", tc.why)
		})
	}

	// Control: a consistent configuration must start cleanly, so a check that rejects everything cannot
	// pass this test.
	s.T().Run("consistentConfigIsAccepted", func(t *testing.T) {
		reports := recordDriverReports(func(rt require.TestingT) {
			d := newSAADriver(t, env, model.Config{MaxAttempts: 1, HasScheduleToClose: true, HasHeartbeat: true})
			d.scheduleToClose = 10 * time.Second
			d.shortTimeout = model.HeartbeatElapsesKind
			d.start(rt)
		})
		require.Empty(t, reports, "a consistent configuration must be accepted")
	})
}

// TestSAADriverAttributesAnOutrunDispatchWindowToTheDriver requires the negative poll to blame the
// driver, not the product, when it can no longer make its check.
//
// The negative poll asserts that a start-delayed or backing-off activity dispatches nothing. It decides
// whether to run from the delay the driver configured, not from how much of the window is actually
// left, so it assumes little time has passed since the delay began. Nothing enforces that: under load
// the window can close first, and the poll then finds a task that was dispatched entirely legitimately
// and reports it as a product divergence. Such a finding at least announces itself, unlike a missed one,
// but it costs an investigation, invites a fix to a product that is behaving correctly, and teaches
// everyone to read a red parity test as flake.
//
// Injected here by shortening the real start delay to nothing while the driver still believes it is an
// hour, which puts the poll in exactly the position a slow machine would.
func (s *standaloneActivityTestSuite) TestSAADriverAttributesAnOutrunDispatchWindowToTheDriver() {
	env := s.newTestEnv()

	// negativePoll drives the one Poll of a start-delayed activity through the model-checking path, which
	// is what runs the negative poll, and returns everything the driver reported.
	negativePoll := func(t *testing.T, customize func(*workflowservice.StartActivityExecutionRequest)) []string {
		return recordDriverReports(func(rt require.TestingT) {
			d := newSAADriver(t, env, model.Config{MaxAttempts: 1, HasStartDelay: true})
			d.startDelay = time.Hour
			d.customizeStart = customize
			a := d.start(rt)
			_, err := a.observed() // seed the stamp baseline, as the model-checking driver does after Start
			require.NoError(rt, err)
			cur, poll := model.Initial(d.cfg), model.Poll
			a.apply(rt, poll, cur, model.Transition(d.cfg, cur, poll), true)
		})
	}

	s.T().Run("windowStillOpen", func(t *testing.T) {
		require.Empty(t, negativePoll(t, nil),
			"control: an activity genuinely inside its start delay dispatches nothing, and the poll must say so")
	})

	s.T().Run("windowAlreadyClosed", func(t *testing.T) {
		reports := strings.Join(negativePoll(t, func(req *workflowservice.StartActivityExecutionRequest) {
			req.StartDelay = nil
		}), "\n")
		require.NotContains(t, reports, "a task WAS dispatched",
			"the dispatch was legitimate — the driver ran its check after the window closed — so this must "+
				"not be reported as the product dispatching early")
		require.Contains(t, reports, outranDispatchWindow,
			"the driver must report that it could no longer make this check")
	})
}

// TestAdjudicateDispatch pins how a negative poll tells a product defect from its own window closing.
// Removing the timing margin rests on this: a task found while the window was still open is still
// reported as the product dispatching early, and only one found after the window closed is excused.
func TestAdjudicateDispatch(t *testing.T) {
	dispatchTime := time.Date(2020, 1, 1, 0, 0, 10, 0, time.UTC)
	for _, tc := range []struct {
		name        string
		polledUntil time.Time
		want        negativePollResult
	}{
		{"well inside the window", dispatchTime.Add(-5 * time.Second), dispatchedEarly},
		{"just inside the window", dispatchTime.Add(-time.Nanosecond), dispatchedEarly},
		{"exactly at the dispatch time", dispatchTime, windowOutrun},
		{"after the window closed", dispatchTime.Add(5 * time.Second), windowOutrun},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, adjudicateDispatch(tc.polledUntil, dispatchTime))
		})
	}
}
