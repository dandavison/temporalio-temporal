// Package failurereason holds failure message constants. In the server they live in the common root
// package, which imports most of the server.
package failurereason

const (
	ActivityTimeout                     = "activity %v timeout"
	ActivityRetryScheduleToCloseTimeout = "Not enough time to schedule next retry before activity ScheduleToClose timeout, giving up retrying"
	FailureExceedsLimit                 = "Failure exceeds size limit."
)
