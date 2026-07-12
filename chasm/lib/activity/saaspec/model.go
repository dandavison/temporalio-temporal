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
// Below, each must handle EVERY status (use a `switch s.Status`), returning either a mutated copy
// (`n := s; n.X = ...; return Outcome{Next: n}`), a `noop(s)`, or a `reject(s, kind)`. Where a
// (status,event) truly cannot be reached, panic("unreachable: ...") so the static diff can confirm
// the code agrees.
// ---------------------------------------------------------------------------

func modelPoll(_ Config, s AbstractState, _ Event) Outcome {
	// A poll advances a SCHEDULED activity to STARTED. In any other status there is no
	// activity task for a worker to pick up, so a poll leaves the state unchanged.
	//
	// A worker can only pick up the task after history's outbound queue has added it to
	// Matching. That happens asynchronously after the activity is scheduled, and only after
	// any start delay or retry backoff has elapsed. The explorer accounts for this: when it
	// wants to advance the activity it polls repeatedly, up to a timeout, until it receives
	// the task, and only then reads the resulting state to compare against this prediction.
	// If this function predicts STARTED but no task is received within the timeout, the
	// explorer reports it as a bug: a SCHEDULED activity that was never dispatched. The
	// exploration uses zero start delay and short retry backoff so the wait is short; the
	// timing of start delay and of longer backoffs is checked by separate tests.
	if s.Status != Scheduled {
		return noop(s)
	}
	n := s
	n.Status = Started
	n.FirstAttemptStarted = true // set once, when the first attempt is picked up
	// Do not invalidate attempt tasks
	return Outcome{Next: n}
}

func modelPause(_ Config, s AbstractState, e Event) Outcome {
	switch s.Status {
	case Scheduled:
		n := s
		n.Status = Paused
		n.Stamp++ // invalidate any pending dispatch task
		return Outcome{Next: n}
	case Started:
		n := s
		n.Status = PauseRequested
		// do not invalidate attempt timer tasks
		return Outcome{Next: n}
	case Paused, PauseRequested:
		if e.SameRequestID {
			return noop(s) // requestID-based idempotency
		}
		return reject(s, FailedPrecondition) // "already paused"
	case CancelRequested:
		return reject(s, FailedPrecondition) // earlier cancel takes precedence
	case ResetRequested:
		return reject(s, FailedPrecondition) // earlier reset takes precedence
	default:
		panic("TODO(spec): Pause while in status " + s.Status.String())
	}
}

func modelHeartbeat(_ Config, s AbstractState, _ Event) Outcome {
	// See ExpectedHeartbeatFlags in responses.go for the spec related to heartbeat response flags
	// (CancelRequested / ActivityPaused / ActivityReset).
	switch s.Status {
	case Started, PauseRequested, CancelRequested, ResetRequested:
		return noop(s)
	case Scheduled, Paused:
		return reject(s, NotFound)
	default:
		panic("TODO(spec): Heartbeat while in status " + s.Status.String())
	}
}

func modelRespondCompleted(cfg Config, s AbstractState, e Event) Outcome {
	// TODO(dan) Does any deferred flag need clearing on completion?
	_ = cfg
	_ = e
	switch s.Status {
	case Started, PauseRequested, CancelRequested, ResetRequested:
		n := s
		n.Status = Completed
		return Outcome{Next: n}
	case Scheduled, Paused:
		return reject(s, NotFound)
	default:
		panic("TODO(spec): RespondCompleted while in status " + s.Status.String())
	}
}

func modelRespondFailed(cfg Config, s AbstractState, e Event) Outcome {
	// The outcome depends on e.Retryable (the failure is retryable and retries remain per
	// cfg.MaxAttempts/Count) and on the current status:
	//   - Started         -> Scheduled (retry) or Failed (exhausted/non-retryable)
	//   - PauseRequested  -> Paused (retry) or Failed
	//   - ResetRequested  -> Scheduled or Paused (per ResetKeepPaused); reset is honored even
	//                        when not retryable/exhausted. Count resets to 1.
	// On a retry, Count++ and Stamp++ (a fresh attempt); on the reset paths Count resets to 1.
	_ = cfg
	_ = e
	retriesRemaining := cfg.MaxAttempts == 0 || s.Count < cfg.MaxAttempts
	switch s.Status {
	case Started, PauseRequested, ResetRequested:
		// TODO(dan): It's more complicated than this. A reset schedule attempt 1 even if the error
		// is non-retryable/retries exhausted. And PauseRequested -> Paused. But let's leave it for
		// now and check that the harness catches it.
		switch {
		case e.Retryable && retriesRemaining:
			// retry
			n := s
			n.Status = Scheduled
			n.Count++
			n.Stamp++
			return Outcome{Next: n}
		default:
			// no retry
			n := s
			n.Status = Failed
			return Outcome{Next: n}
		}
	case CancelRequested:
		// TODO(dan): is this right? Worker must respondCanceled to transition to Canceled
		n := s
		n.Status = Failed
		return Outcome{Next: n}
	case Scheduled, Paused:
		return reject(s, NotFound)
	default:
		panic("TODO(spec): RespondFailed while in status " + s.Status.String())
	}
}

func modelRespondCanceled(cfg Config, s AbstractState, e Event) Outcome {
	// This is the worker API. It is only accepted when an attempt is in progress.
	_ = cfg
	_ = e
	switch s.Status {
	case Started, PauseRequested, CancelRequested, ResetRequested:
		n := s
		n.Status = Canceled
		return Outcome{Next: n}
	case Scheduled, Paused:
		return reject(s, NotFound)
	default:
		panic("TODO(spec): RespondCanceled while in status " + s.Status.String())
	}
}

func modelRequestCancel(cfg Config, s AbstractState, e Event) Outcome {
	// SCHEDULED / PAUSED (no worker) cancel immediately -> Canceled; STARTED /
	// PAUSE_REQUESTED / RESET_REQUESTED -> CancelRequested (wait for worker). Idempotent on
	// repeat request id.
	_ = cfg
	_ = e
	switch s.Status {
	case Scheduled, Paused:
		n := s
		s.Status = Canceled
		// TODO(dan) stamp bump?
		return Outcome{Next: n}
	case Started, PauseRequested:
		n := s
		s.Status = CancelRequested
		return Outcome{Next: n}
	case CancelRequested:
		return noop(s)
	case ResetRequested:
		panic("TODO(spec) how do we handle Reset while in CancelRequested?")
	default:
		panic("TODO(spec): RequestCancel while in status " + s.Status.String())
	}
}

func modelTerminate(cfg Config, s AbstractState, e Event) Outcome {
	// Terminates from any non-terminal status -> Terminated; idempotent on repeat request id.
	_ = cfg
	_ = e
	switch s.Status {
	case Scheduled, Paused:
		n := s
		s.Status = Terminated
		return Outcome{Next: n}
	case Started, PauseRequested, CancelRequested, ResetRequested:
		n := s
		s.Status = Terminated
		// TODO(dan) stamp bump?
		return Outcome{Next: n}
	default:
		panic("TODO(spec): Terminate while in status " + s.Status.String())
	}
}

func modelUnpause(cfg Config, s AbstractState, e Event) Outcome {
	// PAUSED -> Scheduled (re-dispatch; Stamp++). PAUSE_REQUESTED -> Started (worker still
	// running; no stamp bump). Consider e.ResetAttempts / e.ResetHeartbeat. Non-paused ->
	// no-op or reject?
	_ = cfg
	_ = e
	switch s.Status {
	case Paused:
		n := s
		n.Status = Scheduled
		return Outcome{Next: n}
	case Scheduled, Started, PauseRequested, CancelRequested, ResetRequested:
		return reject(s, FailedPrecondition)
	default:
		panic("TODO(spec): Unpause while in status " + s.Status.String())
	}
}

func modelReset(cfg Config, s AbstractState, e Event) Outcome {
	// SCHEDULED / PAUSED (no worker): immediate reset to attempt 1 (Count=1, Stamp++,
	//   discard backoff). Does STCStamp change? Is start_delay re-honored?
	// STARTED / PAUSE_REQUESTED: deferred -> ResetRequested; which flags get set from
	//   e.RestoreOriginal / e.ResetHeartbeat / e.KeepPaused?
	// CANCEL_REQUESTED / RESET_REQUESTED: reject? terminal: reject.
	_ = cfg
	_ = e
	switch s.Status {
	case Scheduled:
	case Paused:
	case Started:
	case PauseRequested:
	case CancelRequested:
	case ResetRequested:
	default:
		panic("TODO(spec): Reset while in status " + s.Status.String())
	}
	panic("TODO(spec): Reset while in status " + s.Status.String())
}

func modelUpdateOptions(cfg Config, s AbstractState, e Event) Outcome {
	// Rejected only in terminal/unspecified statuses. Otherwise bumps Stamp (and reissues
	// STC -> STCStamp++ when STC is set). RestoreOriginal vs field-mask merge. Does it
	// re-dispatch when SCHEDULED?
	_ = cfg
	_ = e
	switch s.Status {
	case Scheduled:
	case Paused:
	case Started:
	case PauseRequested:
	case CancelRequested:
	case ResetRequested:
	default:
		panic("TODO(spec): UpdateOptions while in status " + s.Status.String())
	}
	panic("TODO(spec): UpdateOptions while in status " + s.Status.String())
}
