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
// Unfilled cases panic with "SAA model does not handle ..."; fill them in, following the two worked
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

// Worker PollActivityTaskQueue advances a Scheduled attempt to Started.
func modelPoll(_ Config, s AbstractState, _ Event) Outcome {
	if s.Status != Scheduled {
		// No activity task; no state change.
		return noop(s)
	}
	n := s
	n.Status = Started
	n.FirstAttemptStarted = true // set once, when the first attempt is picked up
	return Outcome{Next: n}
}

// Worker RespondActivityTaskCompleted with task token completes an in-progress attempt.
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
		panic("SAA model does not handle RespondCompleted while in status " + s.Status.String())
	}
}

// Worker RespondActivityTaskFailed with task token fails an in-progress attempt
func modelRespondFailed(cfg Config, s AbstractState, e Event) Outcome {
	retriesRemaining := cfg.MaxAttempts == 0 || s.Count < cfg.MaxAttempts
	switch s.Status {
	case Started, ResetRequested:
		// TODO(dan): It's more complicated than this. A reset schedule attempt 1 even if the error
		// is non-retryable/retries exhausted.
		switch {
		case e.Retryable && retriesRemaining:
			// retry
			n := s
			n.Status = Scheduled
			n.Count++
			n.Stamp++ // invalidate last attempt's tasks
			return Outcome{Next: n}
		default:
			// no retry: terminal failure
			n := s
			n.Status = Failed
			return Outcome{Next: n}
		}
	case PauseRequested:
		switch {
		case e.Retryable && retriesRemaining:
			// pause before retry
			n := s
			n.Status = Paused
			n.Count++
			n.Stamp++ // invalidate last attempt's tasks
			return Outcome{Next: n}
		default:
			// no retry: terminal failure
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
		panic("SAA model does not handle RespondFailed while in status " + s.Status.String())
	}
}

// RequestCancelActivityExecution requests cancellation an activity.
func modelRequestCancel(_ Config, s AbstractState, e Event) Outcome {
	switch s.Status {
	case Scheduled, Paused:
		n := s
		n.Status = Canceled
		// TODO(dan) stamp bump?
		return Outcome{Next: n}
	case Started, PauseRequested:
		n := s
		n.Status = CancelRequested
		return Outcome{Next: n}
	case CancelRequested:
		if e.SameRequestID {
			return noop(s) // requestID-based idempotency
		}
		return reject(s, FailedPrecondition) // TODO(dan): should we consider making is idempotent success even when requestID differs?
	case ResetRequested:
		panic("TODO(spec) how do we handle Reset while in CancelRequested?")
	default:
		panic("SAA model does not handle RequestCancel while in status " + s.Status.String())
	}
}

// Worker RespondActivityTaskCanceled with task token cancels an in-progress attempt for which
// cancellation has been requested.
func modelRespondCanceled(_ Config, s AbstractState, _ Event) Outcome {
	switch s.Status {
	case CancelRequested:
		n := s
		n.Status = Canceled
		return Outcome{Next: n}
	case Scheduled, Paused:
		return reject(s, NotFound) // task token invalid
	case Started, PauseRequested, ResetRequested:
		return reject(s, FailedPrecondition) // task token valid
	default:
		panic("SAA model does not handle RespondCanceled while in status " + s.Status.String())
	}
}

// TerminateActivityExecution terminates a non-closed
func modelTerminate(cfg Config, s AbstractState, e Event) Outcome {
	// Terminates from any non-terminal status -> Terminated; idempotent on repeat request id.
	_ = cfg
	_ = e
	switch s.Status {
	case Scheduled, Paused, Started, PauseRequested, CancelRequested, ResetRequested:
		n := s
		n.Status = Terminated
		return Outcome{Next: n}
	default:
		panic("SAA model does not handle Terminate while in status " + s.Status.String())
	}
}

// RecordActivityTaskHeartbeat
func modelHeartbeat(_ Config, s AbstractState, _ Event) Outcome {
	// See ExpectedHeartbeatFlags in responses.go for the spec related to heartbeat response flags
	// (CancelRequested / ActivityPaused / ActivityReset).
	switch s.Status {
	case Started, PauseRequested, CancelRequested, ResetRequested:
		return noop(s)
	case Scheduled, Paused:
		return reject(s, NotFound)
	default:
		panic("SAA model does not handle Heartbeat while in status " + s.Status.String())
	}
}

// PauseActivityExecution
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
		panic("SAA model does not handle Pause while in status " + s.Status.String())
	}
}

// UnpauseActivityExecution
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
		// TODO(dan): is it desirable that we repeat Unpause are accepted idempotently but other
		// things such as repeat cancel requests are FailedPrecondition?
		return noop(s)
	default:
		panic("SAA model does not handle Unpause while in status " + s.Status.String())
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
		panic("SAA model does not handle Reset while in status " + s.Status.String())
	}
	panic("SAA model does not handle Reset while in status " + s.Status.String())
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
		panic("SAA model does not handle UpdateOptions while in status " + s.Status.String())
	}
	panic("SAA model does not handle UpdateOptions while in status " + s.Status.String())
}
