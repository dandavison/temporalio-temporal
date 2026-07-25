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
