// Package model is a test-only behavioral model of the CHASM activity archetype: an
// implementation-independent description of how an activity should behave. It should never be
// imported by the server binary.
//
// Drivers can be written that realize these events for Standalone Activity or for Workflow Activity.
package model

// Config contains start-time activity options.
type Config struct {
	MaxAttempts int32 // 0 = unlimited
}

// EventKind enumerates the events the model covers.
type EventKind int

const (
	Poll EventKind = iota // worker poll that transitions Scheduled -> Started
	RespondCompleted
	RespondFailed

	// BackoffElapses is the retry-backoff clock elapsing: the delayed retry dispatch becomes available.
	// Status is unchanged, so the only observable is a subsequent Poll returning a task. The harness
	// triggers it by configuring the backoff short and waiting.
	BackoffElapses
)

// Event carries the variant flags that affect the outcome. Leave irrelevant flags zero.
type Event struct {
	Kind EventKind

	Retryable bool // RespondFailed: the failure is retryable. Whether it actually retries also depends on cfg.MaxAttempts.
}
