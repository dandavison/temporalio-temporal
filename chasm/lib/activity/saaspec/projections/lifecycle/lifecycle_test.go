package lifecycle

import (
	"testing"

	"go.temporal.io/server/chasm/lib/activity/saaspec"
)

// fullCfg reaches every operator-relevant state, including Paused and PauseRequested.
var fullCfg = saaspec.Config{
	HasScheduleToClose: true, HasScheduleToStart: true, HasHeartbeat: true, HasStartDelay: true,
	MaxAttempts: 3,
}

// explorerEvents mirrors the events the interactive explorer drives per operation.
var explorerEvents = map[string]saaspec.Event{
	"pause":               {Kind: saaspec.Pause},
	"unpause (if paused)": {Kind: saaspec.Unpause},
	"update start_delay":  {Kind: saaspec.UpdateOptions, SetsStartDelay: true},
	"cancel":              {Kind: saaspec.RequestCancel},
	"reset":               {Kind: saaspec.Reset},
	"terminate":           {Kind: saaspec.Terminate},
}

// The interactive explorer styles each operation up-front by whether the spec permits it on the
// current state. That styling must agree with what the spec actually does to that state: a cell shown
// "not permitted" must be one the spec rejects, and an accepted operation must not be shown
// not-permitted. (Reproduces the bug where unpause was styled not-permitted while the activity was
// Paused, even though the spec accepts it.)
func TestExplorerOpStylingMatchesSpec(t *testing.T) {
	var sawPausedUnpause bool
	for _, s := range Reachable(fullCfg) {
		for _, op := range Ops {
			ev := explorerEvents[op.Name]
			styledNotPermitted := op.Classify(fullCfg, s).Category == NotPermitted
			specRejects := saaspec.Model(fullCfg, s, ev).Reject != saaspec.NoError
			if styledNotPermitted != specRejects {
				t.Errorf("op %q at %s/%s: styled not-permitted=%v, but spec rejects=%v",
					op.Name, s.Status, s.Dispatchability, styledNotPermitted, specRejects)
			}
			if ev.Kind == saaspec.Unpause && (s.Status == saaspec.Paused || s.Status == saaspec.PauseRequested) {
				sawPausedUnpause = true
			}
		}
	}
	if !sawPausedUnpause {
		t.Fatal("no Paused/PauseRequested state reached; test would be vacuous")
	}
}
