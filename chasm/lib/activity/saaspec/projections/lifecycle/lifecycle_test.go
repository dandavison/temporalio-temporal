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

// The interactive explorer styles each operation up-front by whether the spec permits it on the
// current state (via LiveOps/ClassifyLive). That styling must agree with what the spec actually does
// to that state: a cell shown "not permitted" must be one the spec rejects, and an accepted operation
// must not be shown not-permitted.
func TestExplorerOpStylingMatchesSpec(t *testing.T) {
	for _, s := range Reachable(fullCfg) {
		for _, op := range LiveOps {
			styledNotPermitted := ClassifyLive(fullCfg, s, op.Event).Category == NotPermitted
			specRejects := saaspec.Model(fullCfg, s, op.Event).Reject != saaspec.NoError
			if styledNotPermitted != specRejects {
				t.Errorf("op %q at %s/%s: styled not-permitted=%v, but spec rejects=%v",
					op.Name, s.Status, s.Dispatchability, styledNotPermitted, specRejects)
			}
		}
	}
}

// unpause must be offered as available whenever the activity is actually paused — that is the whole
// point of a concrete, current-state explorer (the diagram/prose Ops classifier, by contrast, treats
// unpause hypothetically and reports it not-permitted on an already-paused state).
func TestExplorerUnpauseAvailableWhilePaused(t *testing.T) {
	unpause := saaspec.Event{Kind: saaspec.Unpause}
	var saw bool
	for _, s := range Reachable(fullCfg) {
		if s.Status != saaspec.Paused && s.Status != saaspec.PauseRequested {
			continue
		}
		saw = true
		if ClassifyLive(fullCfg, s, unpause).Category == NotPermitted {
			t.Errorf("unpause styled not-permitted at %s/%s", s.Status, s.Dispatchability)
		}
		if out := saaspec.Model(fullCfg, s, unpause); out.Reject != saaspec.NoError || out.Next == s {
			t.Errorf("expected unpause to be accepted with a state change at %s/%s, got reject=%v next=%+v",
				s.Status, s.Dispatchability, out.Reject, out.Next)
		}
	}
	if !saw {
		t.Fatal("no Paused/PauseRequested state reached; test would be vacuous")
	}
}
