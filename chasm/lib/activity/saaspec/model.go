// Order of precedence is Cancel > Reset > Pause
// I.e. you can Cancel in {Reset,Pause}Requested, and you can Reset in PauseRequested.
package saaspec

// Initial is the state immediately after a successful StartActivityExecution.
func Initial(cfg Config) AbstractState {
	s := AbstractState{Status: Scheduled, Count: 1, Stamp: 1, DispatchTimeSet: true}
	if cfg.HasScheduleToClose {
		s.STCStamp = 1 // bumped by TransitionScheduled when STC is set
	}
	return s
}

func noop(s AbstractState) Outcome                { return Outcome{Next: s, Reject: NoError} }
func reject(s AbstractState, k ErrorKind) Outcome { return Outcome{Next: s, Reject: k} }

func Model(cfg Config, s AbstractState, e Event) Outcome {
	switch e.Kind {
	case Poll:
		return poll(cfg, s, e)
	case Heartbeat:
		return heartbeat(cfg, s, e)
	case RespondCompleted:
		return respondCompleted(cfg, s, e)
	case RespondFailed:
		return respondFailed(cfg, s, e)
	case RespondCanceled:
		return respondCanceled(cfg, s, e)
	case RequestCancel:
		return requestCancel(cfg, s, e)
	case Terminate:
		return terminate(cfg, s, e)
	case Pause:
		return pause(cfg, s, e)
	case Unpause:
		return unpause(cfg, s, e)
	case Reset:
		return reset(cfg, s, e)
	case UpdateOptions:
		return updateOptions(cfg, s, e)
	default:
		panic("saaspec: unhandled event kind")
	}
}

// Below, each modelFoo must return Outcome{Next: n}`), a `noop(s)`, or a `reject(s, kind)`.

// Worker PollActivityTaskQueue advances a Scheduled attempt to Started.
func poll(_ Config, s AbstractState, _ Event) Outcome {
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
func respondCompleted(_ Config, s AbstractState, _ Event) Outcome {
	if s.Status.Terminal() {
		return reject(s, NotFound) // task token invalid
	}
	switch s.Status {
	case Started, PauseRequested, CancelRequested, ResetRequested:
		n := s
		n.Status = Completed
		n.ResetHeartbeats = false // terminal transition clears the deferred reset-heartbeat flag
		return Outcome{Next: n}
	case Scheduled, Paused:
		return reject(s, NotFound)
	default:
		panic("SAA model does not handle RespondCompleted while in status " + s.Status.String())
	}
}

// Worker RespondActivityTaskFailed with task token fails an in-progress attempt
func respondFailed(cfg Config, s AbstractState, e Event) Outcome {
	retriesRemaining := cfg.MaxAttempts == 0 || s.Count < cfg.MaxAttempts
	if s.Status.Terminal() {
		return reject(s, NotFound) // task token invalid
	}
	switch s.Status {
	case ResetRequested:
		// Deferred reset: consume the flags set at reset time and apply their effects.
		n := s
		n.Count = 1
		n.Stamp++ // invalidate last attempt's tasks
		if s.ResetRestoreOptions && cfg.HasScheduleToClose {
			n.STCStamp++ // restoring options reissues the schedule-to-close task
		}
		if s.ResetKeepPaused {
			n.Status = Paused
			n.DispatchTimeSet = false // no dispatch task while paused
		} else {
			n.Status = Scheduled
			n.DispatchTimeSet = true
		}
		n.ResetKeepPaused = false
		n.ResetRestoreOptions = false
		n.ResetHeartbeats = false
		return Outcome{Next: n}
	case Started, PauseRequested:
		n := s
		if e.Retryable && retriesRemaining {
			n.Status = Scheduled
			if s.Status == PauseRequested {
				n.Status = Paused // pause takes effect on the retry
			}
			n.Count++
			n.Stamp++ // invalidate last attempt's tasks
		} else {
			// no retry: terminal failure
			n.Status = Failed
			n.ResetHeartbeats = false // terminal transition clears the deferred reset-heartbeat flag
		}
		return Outcome{Next: n}
	case CancelRequested:
		n := s
		n.Status = Failed
		n.ResetHeartbeats = false // terminal transition clears the deferred reset-heartbeat flag
		return Outcome{Next: n}
	case Scheduled, Paused:
		return reject(s, NotFound) // task token invalid
	default:
		panic("SAA model does not handle RespondFailed while in status " + s.Status.String())
	}
}

// RequestCancelActivityExecution requests cancellation an activity.
func requestCancel(_ Config, s AbstractState, e Event) Outcome {
	if s.Status.Terminal() {
		return reject(s, FailedPrecondition)
	}
	switch s.Status {
	case Scheduled, Paused:
		n := s
		n.Status = Canceled
		// TODO(dan) stamp bump?
		return Outcome{Next: n}
	case Started, PauseRequested, ResetRequested:
		n := s
		n.Status = CancelRequested
		return Outcome{Next: n}
	case CancelRequested:
		if e.SameRequestID {
			return noop(s) // requestID-based idempotency
		}
		return reject(s, FailedPrecondition) // TODO(dan): should we consider making is idempotent success even when requestID differs?
	default:
		panic("SAA model does not handle RequestCancel while in status " + s.Status.String())
	}
}

// Worker RespondActivityTaskCanceled with task token cancels an in-progress attempt for which
// cancellation has been requested.
func respondCanceled(_ Config, s AbstractState, _ Event) Outcome {
	if s.Status.Terminal() {
		return reject(s, NotFound) // task token invalid
	}
	switch s.Status {
	case CancelRequested:
		n := s
		n.Status = Canceled
		n.ResetHeartbeats = false // terminal transition clears the deferred reset-heartbeat flag
		return Outcome{Next: n}
	case Scheduled, Paused:
		return reject(s, NotFound) // task token invalid
	case Started, PauseRequested, ResetRequested:
		return reject(s, FailedPrecondition) // task token valid
	default:
		panic("SAA model does not handle RespondCanceled while in status " + s.Status.String())
	}
}

// TerminateActivityExecution from any non-terminal status -> Terminated; idempotent on repeat
// request id.
func terminate(_ Config, s AbstractState, e Event) Outcome {
	if s.Status.Terminal() {
		if s.Status == Terminated && e.SameRequestID {
			// Idempotent only from Terminated
			return noop(s)
		}
		// Other terminals (Completed/Failed/Canceled/TimedOut), or Terminated with a different id
		return reject(s, FailedPrecondition)
	}
	switch s.Status {
	case Scheduled, Paused, Started, PauseRequested, CancelRequested, ResetRequested:
		n := s
		n.Status = Terminated
		n.ResetHeartbeats = false // terminal transition clears the deferred reset-heartbeat flag
		return Outcome{Next: n}
	default:
		panic("SAA model does not handle Terminate while in status " + s.Status.String())
	}
}

// RecordActivityTaskHeartbeat
func heartbeat(_ Config, s AbstractState, _ Event) Outcome {
	// See ExpectedHeartbeatFlags in responses.go for the spec related to heartbeat response flags
	// (CancelRequested / ActivityPaused / ActivityReset).
	if s.Status.Terminal() {
		return reject(s, NotFound) // task token invalid
	}
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
func pause(_ Config, s AbstractState, e Event) Outcome {
	if s.Status.Terminal() {
		return reject(s, FailedPrecondition)
	}
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
		return reject(s, FailedPrecondition)
	case ResetRequested:
		if s.ResetKeepPaused && e.SameRequestID {
			// Pause -> Reset(keepPaused) -> Pause(same requestID)
			return noop(s)
		}
		return reject(s, FailedPrecondition)
	default:
		panic("SAA model does not handle Pause while in status " + s.Status.String())
	}
}

// UnpauseActivityExecution
func unpause(_ Config, s AbstractState, e Event) Outcome {
	if s.Status.Terminal() {
		return reject(s, FailedPrecondition)
	}
	switch s.Status {
	case Paused:
		n := s
		n.Status = Scheduled
		n.Stamp++ // TODO(dan) double-check this is as it should be: we bump the stamp on Unpause, not on entry to Paused?
		n.DispatchTimeSet = true
		if e.ResetAttempts {
			n.Count = 1
		}
		return Outcome{Next: n}
	case PauseRequested:
		// TODO(dan): Unlike CancelRequested and ResetRequested, PauseRequested can be "undone" (by Unpause).
		n := s
		n.Status = Started
		return Outcome{Next: n}
	case ResetRequested:
		n := s
		// TODO(dan): our implementation has Unpause strip the ResetKeepPaused flag from a pending
		// Reset. But is that really what we want? Does it not seem like rather complex/ambitious
		// behavior?
		n.ResetKeepPaused = false // TODO(dan) see comment above; not sure this line should be in the spec
		return Outcome{Next: n}
	case Scheduled, Started, CancelRequested:
		// TODO(dan): is it desirable that repeat Unpause are accepted idempotently but other things
		// such as repeat cancel requests are FailedPrecondition? If the repeat Unpauses carry
		// e.ResetAttempts / e.ResetHeartbeat, should they be no-op or reject?
		return noop(s)
	default:
		panic("SAA model does not handle Unpause while in status " + s.Status.String())
	}
}

// ResetActivityExecution makes the activity behave as if it were starting its first attempt, except
// for the ScheduleToCLose timer which keeps running. The reset is not applied until any current
// attempt has ended.
func reset(cfg Config, s AbstractState, e Event) Outcome {
	if s.Status.Terminal() {
		return reject(s, FailedPrecondition)
	}
	switch s.Status {
	case Scheduled, Paused:
		n := s
		n.Count = 1
		n.Stamp++
		if s.Status == Paused && e.KeepPaused {
			n.DispatchTimeSet = false
		} else {
			n.Status = Scheduled
			n.DispatchTimeSet = true
		}
		if e.RestoreOriginal && cfg.HasScheduleToClose {
			n.STCStamp++
		}
		return Outcome{Next: n}
	case Started, PauseRequested:
		n := s
		n.Status = ResetRequested
		if s.Status == PauseRequested {
			// KeepPaused is stored if a Reset arrives during PauseRequested
			if e.KeepPaused {
				n.ResetKeepPaused = true
				s.Status = PauseRequested
			} else {
				n.ResetKeepPaused = false
			}
		}
		n.ResetHeartbeats = e.ResetHeartbeat
		n.ResetRestoreOptions = e.RestoreOriginal
		// Current attempt remains live and reset will be ignored if it completes; do not invalidate
		// attempt tasks.
		return Outcome{Next: n}
	case CancelRequested, ResetRequested:
		// TODO(dan): should we support repeat reset requests?
		return reject(s, FailedPrecondition)
	default:
		panic("SAA model does not handle Reset while in status " + s.Status.String())
	}
}

// UpdateActivityExecutionOptions
func updateOptions(cfg Config, s AbstractState, _ Event) Outcome {
	// TODO(dan): RestoreOriginal, field-mask merge. Does it re-dispatch when SCHEDULED?
	if s.Status.Terminal() {
		return reject(s, FailedPrecondition)
	}
	switch s.Status {
	case Scheduled, Paused, Started, PauseRequested, CancelRequested, ResetRequested:
		n := s
		n.Stamp++
		if cfg.HasScheduleToClose {
			n.STCStamp++
		}
		return Outcome{Next: n}
	default:
		panic("SAA model does not handle UpdateOptions while in status " + s.Status.String())
	}
}
