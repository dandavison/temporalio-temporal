package saaspec

// Initial is the state immediately after a successful StartActivityExecution.
func Initial(cfg Config) AbstractState {
	s := AbstractState{Status: Scheduled, Count: 1, Stamp: 1, DispatchTimeSet: true}
	if cfg.HasScheduleToClose {
		s.STCStamp = 1 // TransitionScheduled bumps schedule_to_close_stamp when STC is set
	}
	return s
}

func noop(s AbstractState) Outcome                { return Outcome{Next: s, Reject: NoError} }
func reject(s AbstractState, k ErrorKind) Outcome { return Outcome{Next: s, Reject: k} }

// Model is the spec. It is TOTAL: every (status, event) must be handled. Making it total
// is the point — it forces every open corner of the intended behavior to be resolved.
// Unfilled cases panic with "TODO(spec): ..."; fill them in, following the two worked
// examples below (modelPoll and modelPause).
func Model(cfg Config, s AbstractState, e Event) Outcome {
	switch e.Kind {
	case Poll:
		return modelPoll(cfg, s, e)
	case Heartbeat:
		return modelHeartbeat(cfg, s, e)
	case RespondCompleted:
		return modelRespondCompleted(cfg, s, e)
	case RespondFailed:
		return modelRespondFailed(cfg, s, e)
	case RespondCanceled:
		return modelRespondCanceled(cfg, s, e)
	case RequestCancel:
		return modelRequestCancel(cfg, s, e)
	case Terminate:
		return modelTerminate(cfg, s, e)
	case Pause:
		return modelPause(cfg, s, e)
	case Unpause:
		return modelUnpause(cfg, s, e)
	case Reset:
		return modelReset(cfg, s, e)
	case UpdateOptions:
		return modelUpdateOptions(cfg, s, e)
	default:
		panic("saaspec: unhandled event kind")
	}
}

// ---------------------------------------------------------------------------
// Worked example #1: Poll (worker pickup). Fully implemented — use as a pattern.
// ---------------------------------------------------------------------------

func modelPoll(_ Config, s AbstractState, _ Event) Outcome {
	// A poll only advances a SCHEDULED activity to STARTED. In every other status there
	// is no dispatchable task, so a poll leaves state unchanged. (Whether a task is
	// *available yet* under backoff/start-delay is a timing concern handled by the
	// explorer, which only issues Poll where it expects a task to be dispatchable.)
	if s.Status != Scheduled {
		return noop(s)
	}
	n := s
	n.Status = Started
	n.FirstAttemptStarted = true // set once, on the first pickup
	// NOTE: no stamp bump — Started keeps the attempt's dispatch stamp.
	return Outcome{Next: n}
}

// ---------------------------------------------------------------------------
// Worked example #2: Pause. Implemented, but REVIEW against your intended spec —
// especially the ResetRequested case, which is left as a decision.
// ---------------------------------------------------------------------------

func modelPause(_ Config, s AbstractState, e Event) Outcome {
	switch s.Status {
	case Scheduled:
		n := s
		n.Status = Paused
		n.Stamp++ // must invalidate the pending dispatch task so we don't dispatch while paused
		return Outcome{Next: n}
	case Started:
		n := s
		n.Status = PauseRequested // worker still in charge; NO stamp bump (its timers stay valid)
		return Outcome{Next: n}
	case Paused, PauseRequested:
		if e.SameRequestID {
			return noop(s) // idempotent repeat
		}
		return reject(s, FailedPrecondition) // "already paused"
	case CancelRequested:
		return reject(s, FailedPrecondition) // cancel takes precedence
	case ResetRequested:
		// DECISION POINT: reject? no-op? depends on ResetKeepPaused? Resolve and replace.
		panic("TODO(spec): Pause during ResetRequested — decide")
	default:
		if s.Status.Terminal() {
			return reject(s, FailedPrecondition)
		}
		panic("TODO(spec): Pause from " + s.Status.String())
	}
}

// ---------------------------------------------------------------------------
// TODO(spec): fill these in, following the modelPause pattern. Each must handle EVERY
// status (use a `switch s.Status`), returning either a mutated copy (`n := s; n.X = ...;
// return Outcome{Next: n}`), a `noop(s)`, or a `reject(s, kind)`. Where a (status,event)
// truly cannot be reached, panic("unreachable: ...") so the static diff can confirm the
// code agrees.
// ---------------------------------------------------------------------------

func modelHeartbeat(cfg Config, s AbstractState, e Event) Outcome {
	// Heartbeat does not change status/count/stamp/flags, so on the token-valid statuses
	// {Started, CancelRequested, PauseRequested, ResetRequested} it is a state no-op;
	// elsewhere the token is invalid -> NotFound. (The heartbeat RESPONSE flags —
	// CancelRequested / ActivityPaused / ActivityReset — are a separate oracle in the
	// explorer, not part of AbstractState.)
	_ = cfg
	_ = e
	panic("TODO(spec): Heartbeat from " + s.Status.String())
}

func modelRespondCompleted(cfg Config, s AbstractState, e Event) Outcome {
	// Worker success. Token-valid statuses complete the activity; note completion is
	// accepted even in PauseRequested / ResetRequested (worker finished — honor it). Does
	// any deferred flag need clearing on completion?
	_ = cfg
	_ = e
	panic("TODO(spec): RespondCompleted from " + s.Status.String())
}

func modelRespondFailed(cfg Config, s AbstractState, e Event) Outcome {
	// The subtle one. Outcome depends on e.Retryable (failure retryable AND retries remain
	// per cfg.MaxAttempts/Count) AND the current status:
	//   - Started         -> Scheduled (retry) or Failed (exhausted/non-retryable)
	//   - PauseRequested  -> Paused (retry) or Failed
	//   - ResetRequested  -> Scheduled or Paused (per ResetKeepPaused); reset is honored even
	//                        when not retryable/exhausted. Count resets to 1.
	// On a retry, Count++ and Stamp++ (a fresh attempt); on the reset paths Count resets to 1.
	_ = cfg
	_ = e
	panic("TODO(spec): RespondFailed from " + s.Status.String())
}

func modelRespondCanceled(cfg Config, s AbstractState, e Event) Outcome {
	_ = cfg
	_ = e
	panic("TODO(spec): RespondCanceled from " + s.Status.String())
}

func modelRequestCancel(cfg Config, s AbstractState, e Event) Outcome {
	// SCHEDULED / PAUSED (no worker) cancel immediately -> Canceled; STARTED /
	// PAUSE_REQUESTED / RESET_REQUESTED -> CancelRequested (wait for worker). Idempotent on
	// repeat request id.
	_ = cfg
	_ = e
	panic("TODO(spec): RequestCancel from " + s.Status.String())
}

func modelTerminate(cfg Config, s AbstractState, e Event) Outcome {
	// Terminates from any non-terminal status -> Terminated; idempotent on repeat request id.
	_ = cfg
	_ = e
	panic("TODO(spec): Terminate from " + s.Status.String())
}

func modelUnpause(cfg Config, s AbstractState, e Event) Outcome {
	// PAUSED -> Scheduled (re-dispatch; Stamp++). PAUSE_REQUESTED -> Started (worker still
	// running; no stamp bump). Consider e.ResetAttempts / e.ResetHeartbeat. Non-paused ->
	// no-op or reject?
	_ = cfg
	_ = e
	panic("TODO(spec): Unpause from " + s.Status.String())
}

func modelReset(cfg Config, s AbstractState, e Event) Outcome {
	// SCHEDULED / PAUSED (no worker): immediate reset to attempt 1 (Count=1, Stamp++,
	//   discard backoff). Does STCStamp change? Is start_delay re-honored?
	// STARTED / PAUSE_REQUESTED: deferred -> ResetRequested; which flags get set from
	//   e.RestoreOriginal / e.ResetHeartbeat / e.KeepPaused?
	// CANCEL_REQUESTED / RESET_REQUESTED: reject? terminal: reject.
	_ = cfg
	_ = e
	panic("TODO(spec): Reset from " + s.Status.String())
}

func modelUpdateOptions(cfg Config, s AbstractState, e Event) Outcome {
	// Rejected only in terminal/unspecified statuses. Otherwise bumps Stamp (and reissues
	// STC -> STCStamp++ when STC is set). RestoreOriginal vs field-mask merge. Does it
	// re-dispatch when SCHEDULED?
	_ = cfg
	_ = e
	panic("TODO(spec): UpdateOptions from " + s.Status.String())
}
