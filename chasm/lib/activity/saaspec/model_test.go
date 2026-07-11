package saaspec

import "testing"

// Smoke tests over the two worked examples, so the package starts green. As you fill in
// Model(), add cases here (or rely on the explorer, which checks the whole graph).

func TestInitial(t *testing.T) {
	got := Initial(Config{HasScheduleToClose: true})
	want := AbstractState{Status: Scheduled, Count: 1, Stamp: 1, STCStamp: 1, DispatchTimeSet: true}
	if got != want {
		t.Fatalf("Initial: got %+v want %+v", got, want)
	}
}

func TestPollFromScheduledStarts(t *testing.T) {
	s := Initial(Config{})
	out := Model(Config{}, s, Event{Kind: Poll})
	if out.Reject != NoError {
		t.Fatalf("unexpected reject %v", out.Reject)
	}
	if out.Next.Status != Started || !out.Next.FirstAttemptStarted {
		t.Fatalf("Poll: got %+v", out.Next)
	}
	if out.Next.Stamp != s.Stamp {
		t.Fatalf("Poll must not bump stamp: %d -> %d", s.Stamp, out.Next.Stamp)
	}
}

func TestPauseFromScheduledBumpsStamp(t *testing.T) {
	cfg := Config{HasScheduleToClose: true}
	s := Initial(cfg)
	out := Model(cfg, s, Event{Kind: Pause})
	if out.Reject != NoError {
		t.Fatalf("unexpected reject %v", out.Reject)
	}
	if out.Next.Status != Paused {
		t.Fatalf("want Paused got %v", out.Next.Status)
	}
	if out.Next.Stamp != s.Stamp+1 {
		t.Fatalf("pause from scheduled must bump stamp: %d -> %d", s.Stamp, out.Next.Stamp)
	}
	if out.Next.STCStamp != s.STCStamp {
		t.Fatalf("pause must not touch STCStamp: %d -> %d", s.STCStamp, out.Next.STCStamp)
	}
}

func TestPauseWhileStartedIsPauseRequested(t *testing.T) {
	cfg := Config{}
	s := Model(cfg, Initial(cfg), Event{Kind: Poll}).Next // Scheduled -> Started
	out := Model(cfg, s, Event{Kind: Pause})
	if out.Reject != NoError {
		t.Fatalf("unexpected reject %v", out.Reject)
	}
	if out.Next.Status != PauseRequested {
		t.Fatalf("want PauseRequested got %v", out.Next.Status)
	}
	if out.Next.Stamp != s.Stamp {
		t.Fatalf("pause while started must NOT bump stamp: %d -> %d", s.Stamp, out.Next.Stamp)
	}
}
