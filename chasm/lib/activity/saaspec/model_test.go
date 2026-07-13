package saaspec

import "testing"

// Smoke tests over the two worked examples, so the package starts green. As you fill in
// Model(), add cases here (or rely on the explorer, which checks the whole graph).

func TestInitial(t *testing.T) {
	got := Initial(Config{HasScheduleToClose: true})
	want := AbstractState{Status: Scheduled, Count: 1, Stamp: 1, ScheduleToCloseStamp: 1, DispatchTimeSet: true}
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
	if out.Next.ScheduleToCloseStamp != s.ScheduleToCloseStamp {
		t.Fatalf("pause must not touch ScheduleToCloseStamp: %d -> %d", s.ScheduleToCloseStamp, out.Next.ScheduleToCloseStamp)
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

// The tests below pin the six dispatch-delay requirements (start_delay and retry backoff
// interacting with the timeouts and operator commands) at the spec level: they assert Model()
// encodes the intended behavior. The server-side explorer checks the implementation against Model().

// backedOffRetry returns a Scheduled state with a pending retry backoff (attempt 2), reached the way
// a worker would: poll the first attempt, then fail it retryably.
func backedOffRetry(t *testing.T, cfg Config) AbstractState {
	t.Helper()
	started := Model(cfg, Initial(cfg), Event{Kind: Poll}).Next
	s := Model(cfg, started, Event{Kind: RespondFailed, Retryable: true}).Next
	if s.Status != Scheduled || s.Dispatchability != BackoffPending {
		t.Fatalf("expected a Scheduled/BackoffPending retry, got %v/%v", s.Status, s.Dispatchability)
	}
	return s
}

func pollable(cfg Config, s AbstractState) bool {
	return Model(cfg, s, Event{Kind: Poll}).Next.Status == Started
}

// (1) Schedule-to-close keeps running during a start delay: it fires even while the first dispatch
// is still delayed.
func TestScheduleToCloseFiresDuringStartDelay(t *testing.T) {
	cfg := Config{HasStartDelay: true, HasScheduleToClose: true}
	s := Initial(cfg)
	if s.Dispatchability != StartDelayPending {
		t.Fatalf("Initial with start delay should be StartDelayPending, got %v", s.Dispatchability)
	}
	if out := Model(cfg, s, Event{Kind: ScheduleToCloseFires}); out.Next.Status != TimedOut {
		t.Fatalf("schedule-to-close must fire during the start delay, got %v", out.Next.Status)
	}
}

// (2) Pause during a start delay is possible; unpause does not dispatch immediately — it keeps
// waiting for the delay, and only a StartDelayElapses makes it dispatchable.
func TestPauseUnpauseDuringStartDelay(t *testing.T) {
	cfg := Config{HasStartDelay: true}
	paused := Model(cfg, Initial(cfg), Event{Kind: Pause})
	if paused.Reject != NoError || paused.Next.Status != Paused {
		t.Fatalf("pause during start delay must succeed -> Paused, got %v/%v", paused.Reject, paused.Next.Status)
	}
	unpaused := Model(cfg, paused.Next, Event{Kind: Unpause}).Next
	if unpaused.Status != Scheduled || unpaused.Dispatchability != StartDelayPending {
		t.Fatalf("unpause during start delay must resume waiting (Scheduled/StartDelayPending), got %v/%v", unpaused.Status, unpaused.Dispatchability)
	}
	if pollable(cfg, unpaused) {
		t.Fatalf("a poll must find no task while the start delay is still pending")
	}
	elapsed := Model(cfg, unpaused, Event{Kind: StartDelayElapses}).Next
	if !pollable(cfg, elapsed) {
		t.Fatalf("once the start delay elapses the activity must dispatch")
	}
}

// (3) Same as (2) for a retry backoff.
func TestPauseUnpauseDuringBackoff(t *testing.T) {
	cfg := Config{}
	retry := backedOffRetry(t, cfg)
	paused := Model(cfg, retry, Event{Kind: Pause})
	if paused.Reject != NoError || paused.Next.Status != Paused {
		t.Fatalf("pause during backoff must succeed -> Paused, got %v/%v", paused.Reject, paused.Next.Status)
	}
	unpaused := Model(cfg, paused.Next, Event{Kind: Unpause}).Next
	if unpaused.Dispatchability != BackoffPending {
		t.Fatalf("unpause during backoff must resume waiting (BackoffPending), got %v", unpaused.Dispatchability)
	}
	if pollable(cfg, unpaused) {
		t.Fatalf("a poll must find no task while the backoff is still pending")
	}
	if !pollable(cfg, Model(cfg, unpaused, Event{Kind: BackoffElapses}).Next) {
		t.Fatalf("once the backoff elapses the activity must dispatch")
	}
}

// (4) Schedule-to-start is pushed back by a start delay (and a retry backoff): it must not fire
// while the dispatch is still delayed, only after it becomes available.
func TestScheduleToStartPushedBackByDispatchDelay(t *testing.T) {
	startDelayCfg := Config{HasStartDelay: true, HasScheduleToStart: true}
	s := Initial(startDelayCfg)
	if out := Model(startDelayCfg, s, Event{Kind: ScheduleToStartFires}); out.Next.Status != Scheduled {
		t.Fatalf("schedule-to-start must not fire during the start delay (pushed back), got %v", out.Next.Status)
	}
	dispatched := Model(startDelayCfg, s, Event{Kind: StartDelayElapses}).Next
	if out := Model(startDelayCfg, dispatched, Event{Kind: ScheduleToStartFires}); out.Next.Status != TimedOut {
		t.Fatalf("schedule-to-start should fire once the delay elapses, got %v", out.Next.Status)
	}

	backoffCfg := Config{HasScheduleToStart: true}
	retry := backedOffRetry(t, backoffCfg)
	if out := Model(backoffCfg, retry, Event{Kind: ScheduleToStartFires}); out.Next.Status != Scheduled {
		t.Fatalf("schedule-to-start must not fire during the retry backoff (pushed back), got %v", out.Next.Status)
	}
}

// (5) Reset during a start delay is possible and behaves like unpause: it keeps waiting for the
// delay rather than dispatching now.
func TestResetDuringStartDelayPreservesDelay(t *testing.T) {
	cfg := Config{HasStartDelay: true}
	out := Model(cfg, Initial(cfg), Event{Kind: Reset})
	if out.Reject != NoError {
		t.Fatalf("reset during start delay must be accepted, got reject %v", out.Reject)
	}
	if out.Next.Dispatchability != StartDelayPending {
		t.Fatalf("reset during start delay must keep waiting (StartDelayPending), got %v", out.Next.Dispatchability)
	}
	if pollable(cfg, out.Next) {
		t.Fatalf("a poll must find no task after a reset during the start delay")
	}
}

// (6) Reset during a retry backoff discards the backoff: the reset attempt dispatches immediately.
func TestResetDuringBackoffDispatchesImmediately(t *testing.T) {
	cfg := Config{}
	out := Model(cfg, backedOffRetry(t, cfg), Event{Kind: Reset})
	if out.Reject != NoError {
		t.Fatalf("reset during backoff must be accepted, got reject %v", out.Reject)
	}
	if out.Next.Dispatchability != Dispatchable {
		t.Fatalf("reset during backoff must discard the backoff (Dispatchable), got %v", out.Next.Dispatchability)
	}
	if !pollable(cfg, out.Next) {
		t.Fatalf("a poll after reset-during-backoff must dispatch immediately")
	}
}
