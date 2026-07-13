package tests

// Timeout traces for the standalone-activity spec. A timeout is modeled as an event
// (saaspec.ScheduleToCloseElapses, etc.), triggered by configuring the matching timeout short and
// waiting; driveTrace checks the resulting state against Model(). Model must handle every timeout
// event, else the trace panics.

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm/lib/activity/saaspec"
)

// An saaTimeoutTrace specifies a sequence of events (involving timeouts and/or delay elapses) to be
// tested.
type saaTimeoutTrace struct {
	name string
	// The timeout under test. It's set to a short value, while all the others are long, and placed
	// as the last event in the trace.
	timeout    saaspec.EventKind
	cfg        saaspec.Config
	path       []saaspec.Event
	startDelay time.Duration
}

// saaLongStartDelay keeps a first attempt in its start-delay window for the whole trace, so a short
// timeout under test fires while the activity is still SCHEDULED and pending dispatch.
const saaLongStartDelay = time.Hour

// To read these: `timeout` is the timeout that fires first; `path` is the events leading up to it.
var saaTimeoutTraces = []saaTimeoutTrace{
	{name: "schedule-to-close/elapses-while-paused", timeout: saaspec.ScheduleToCloseElapses, path: []saaspec.Event{{Kind: saaspec.Pause}}, cfg: saaspec.Config{HasScheduleToClose: true}},
	{name: "schedule-to-start/elapses-while-scheduled", timeout: saaspec.ScheduleToStartElapses, cfg: saaspec.Config{HasScheduleToStart: true}},
	{name: "schedule-to-start/elapses-while-paused", timeout: saaspec.ScheduleToStartElapses, path: []saaspec.Event{{Kind: saaspec.Pause}}, cfg: saaspec.Config{HasScheduleToStart: true}},
	{name: "start-to-close/elapses-while-started/retries-remain", timeout: saaspec.StartToCloseElapses, path: []saaspec.Event{{Kind: saaspec.Poll}}, cfg: saaspec.Config{}},
	{name: "start-to-close/elapses-while-started/last-attempt", timeout: saaspec.StartToCloseElapses, path: []saaspec.Event{{Kind: saaspec.Poll}}, cfg: saaspec.Config{MaxAttempts: 1}},
	{name: "heartbeat/elapses-while-started/retries-remain", timeout: saaspec.HeartbeatElapses, path: []saaspec.Event{{Kind: saaspec.Poll}}, cfg: saaspec.Config{HasHeartbeat: true}},
	{name: "heartbeat/elapses-while-started/last-attempt", timeout: saaspec.HeartbeatElapses, path: []saaspec.Event{{Kind: saaspec.Poll}}, cfg: saaspec.Config{HasHeartbeat: true, MaxAttempts: 1}},
	// Dispatch-delay interactions
	{name: "schedule-to-start/elapses-within-start-delay", timeout: saaspec.ScheduleToStartElapses, startDelay: saaLongStartDelay, cfg: saaspec.Config{HasStartDelay: true, HasScheduleToStart: true}},
	{name: "schedule-to-close/elapses-within-start-delay", timeout: saaspec.ScheduleToCloseElapses, startDelay: saaLongStartDelay, cfg: saaspec.Config{HasStartDelay: true, HasScheduleToClose: true}},
}

func (s *standaloneActivityTestSuite) TestSpecTimeoutPaths() {
	env := s.newTestEnv()
	t := s.T()
	ctx := s.Context()

	chasmCtx, err := env.GetTestCluster().Host().ChasmContext(ctx)
	require.NoError(t, err)

	// Each trace is an independent subtest, so `-run 'TestSpecTimeoutPaths/heartbeat'` selects by scenario.
	for i, p := range saaTimeoutTraces {
		t.Run(p.name, func(t *testing.T) {
			h := &saaHarness{
				env: env, ctx: ctx, chasmCtx: chasmCtx, nsID: env.NamespaceID().String(),
				cfg: p.cfg, cfgIdx: i, shortTimeout: p.timeout, startDelay: p.startDelay,
			}
			// The trace reaches the source state via the RPC path, then fires the timeout under test.
			h.driveTrace(t, append(append([]saaspec.Event{}, p.path...), saaspec.Event{Kind: p.timeout}))
		})
	}
}
