package tests

// Does a start-delayed activity dispatch before its start delay has elapsed?
//
// `TestStartDelay_Declarative/start-delay/first-dispatch` intermittently fails with "model expected no
// dispatch (StartDelayPending pending) but a task WAS dispatched (attempt 1)": a poll issued right
// after Start, bounded well inside the delay window, came back with a task.
//
// Everything here is compared using server timestamps, so a slow or loaded client cannot confound it:
//
//	scheduleTime   = info.ScheduleTime            when the activity was created
//	dispatchTime   = info.ExecutionTime           when it is due to dispatch (schedule + start delay)
//	startedTime    = pollResponse.StartedTime     when the attempt actually started
//
// earliness = dispatchTime - startedTime. Positive means the attempt started BEFORE it was due.

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm/lib/activity/model"
)

const earlyDispatchDelay = 5 * time.Second // same window the failing trace uses (saaDelayWindow)

func (s *standaloneActivityTestSuite) TestSAAStartDelayDoesNotDispatchEarly() {
	env := s.newTestEnv()

	for i := range 2 {
		s.T().Run(fmt.Sprintf("run%d", i), func(t *testing.T) {
			h := newSAAHarness(t, env, model.Config{HasStartDelay: true})
			h.startDelay = earlyDispatchDelay
			a := h.start(t)

			info := a.describe(t).GetInfo()
			scheduleTime := info.GetScheduleTime().AsTime()
			dispatchTime := info.GetExecutionTime().AsTime()
			configuredDelay := dispatchTime.Sub(scheduleTime)

			// Generous bound: we want the attempt whenever it comes, not a pass/fail on timing.
			resp := a.pollForTask(t, 30*time.Second)
			require.NotNil(t, resp, "no task within 30s")
			startedTime := resp.GetStartedTime().AsTime()

			earliness := dispatchTime.Sub(startedTime)
			t.Logf("run%d: configuredDelay=%v earliness=%v (scheduled=%s due=%s started=%s)",
				i, configuredDelay.Round(time.Millisecond), earliness.Round(time.Millisecond),
				scheduleTime.Format("15:04:05.000"), dispatchTime.Format("15:04:05.000"),
				startedTime.Format("15:04:05.000"))

			require.InDelta(t, earlyDispatchDelay.Seconds(), configuredDelay.Seconds(), 0.5,
				"the start delay the server recorded must match what was requested")
			require.LessOrEqual(t, earliness, time.Duration(0),
				"the attempt must not start before the activity is due to dispatch")
		})
	}
}
