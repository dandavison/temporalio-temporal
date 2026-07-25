package activity

// Which physical queue an ActivityDispatchTask is routed to. The dispatch is a side-effect task, so CHASM
// derives its category from the ScheduledTime the transition gave it: a future time makes it a timer task,
// held until it fires; no time at all makes it an immediate task on the transfer queue. A dispatch that is
// already due must take the latter — a timer task's fire time is floored at
// now + TimerProcessorMaxTimeShift (~1s) when its task key is assigned, so routing a due dispatch through
// the timer queue delays it by that much.
//
// These assertions are on the physical category rather than on the TaskAttributes the transition passed, so
// they hold whether the routing decision is made here or by the framework.

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/service/history/tasks"
)

func TestDispatchRouting(t *testing.T) {
	// polled leaves a fresh activity STARTED: attempt 1 was dispatched and picked up by a worker.
	polled := func(t *testing.T) *inProcActivity {
		a := newInProcExplorer(t, model.Config{MaxAttempts: 3}).start()
		require.NoError(t, a.realize(model.Event{Kind: model.Poll}))
		return a
	}
	// backedOff leaves it SCHEDULED with a retry backoff still pending.
	backedOff := func(t *testing.T) *inProcActivity {
		a := polled(t)
		require.NoError(t, a.realize(model.Event{Kind: model.RespondFailed, Retryable: true}))
		return a
	}
	// dispatchable leaves it SCHEDULED with the backoff elapsed, so nothing is left to wait for.
	dispatchable := func(t *testing.T) *inProcActivity {
		a := backedOff(t)
		require.NoError(t, a.realize(model.Event{Kind: model.BackoffElapses}))
		return a
	}

	t.Run("initial schedule", func(t *testing.T) {
		require.Equal(t, routing{transfer: 1}, newInProcExplorer(t, model.Config{MaxAttempts: 3}).start().routed())
	})

	// The negative control: a dispatch the server is meant to defer must stay a timer task, or a retry
	// backoff would not be honored at all.
	t.Run("retry with a backoff still to wait out", func(t *testing.T) {
		a := polled(t)
		require.Equal(t, routing{timer: 1}, a.dispatchRouting(func() {
			require.NoError(t, a.realize(model.Event{Kind: model.RespondFailed, Retryable: true}))
		}), "a retry scheduled in the future must remain a timer task")
	})

	t.Run("unpause once the backoff has elapsed", func(t *testing.T) {
		a := dispatchable(t)
		a.pause(t)
		require.Equal(t, routing{transfer: 1}, a.dispatchRouting(func() { a.unpause(t) }))
	})

	t.Run("reset once the backoff has elapsed", func(t *testing.T) {
		a := dispatchable(t)
		require.Equal(t, routing{transfer: 1}, a.dispatchRouting(func() { a.reset(t) }))
	})

	// Reset discards a pending backoff, so it dispatches at once even mid-backoff.
	t.Run("reset mid-backoff", func(t *testing.T) {
		a := backedOff(t)
		require.Equal(t, routing{transfer: 1}, a.dispatchRouting(func() { a.reset(t) }))
	})
}

// routing counts side-effect tasks per category, over the categories a dispatch can be routed to. A
// dispatch never carries a Destination, so outbound is not among them.
type routing map[string]int

var (
	transfer = tasks.CategoryTransfer.Name()
	timer    = tasks.CategoryTimer.Name()
)

// dispatchRouting runs fn and reports where the side-effect tasks it added were routed.
func (a *inProcActivity) dispatchRouting(fn func()) routing {
	before := a.routed()
	fn()
	added := routing{}
	for category, n := range a.routed() {
		if n -= before[category]; n != 0 {
			added[category] = n
		}
	}
	return added
}

// routed counts the side-effect tasks the execution has accumulated in each category a dispatch can be
// routed to. The framework's own visibility tasks are excluded by that restriction; pure tasks are excluded
// because they coalesce into a single payload-free ChasmTaskPure that is always a timer task, and so say
// nothing about dispatch routing.
func (a *inProcActivity) routed() routing {
	byCategory, err := a.x.engine.Tasks(a.ref)
	require.NoError(a.x.t, err)
	counts := routing{}
	for _, category := range []tasks.Category{tasks.CategoryTransfer, tasks.CategoryTimer} {
		for _, task := range byCategory[category] {
			if _, ok := task.(*tasks.ChasmTask); ok {
				counts[category.Name()]++
			}
		}
	}
	return counts
}

func (a *inProcActivity) pause(t *testing.T) {
	require.NoError(t, a.update(func(act *Activity, mc chasm.MutableContext) error {
		_, err := act.handlePauseRequested(mc, &activitypb.PauseActivityExecutionRequest{
			NamespaceId:     inProcNS,
			FrontendRequest: &workflowservice.PauseActivityExecutionRequest{Identity: "operator"},
		})
		return err
	}))
}

func (a *inProcActivity) unpause(t *testing.T) {
	require.NoError(t, a.update(func(act *Activity, mc chasm.MutableContext) error {
		_, err := act.handleUnpauseRequested(mc, &activitypb.UnpauseActivityExecutionRequest{
			NamespaceId:     inProcNS,
			FrontendRequest: &workflowservice.UnpauseActivityExecutionRequest{Identity: "operator"},
		})
		return err
	}))
}

func (a *inProcActivity) reset(t *testing.T) {
	require.NoError(t, a.update(func(act *Activity, mc chasm.MutableContext) error {
		_, err := act.handleReset(mc, &activitypb.ResetActivityExecutionRequest{
			NamespaceId:     inProcNS,
			FrontendRequest: &workflowservice.ResetActivityExecutionRequest{Identity: "operator"},
		})
		return err
	}))
}
