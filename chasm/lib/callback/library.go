package callback

import (
	"go.temporal.io/server/chasm"
)

// InvocationTaskGroup is the outbound queue task group that callback invocation tasks are
// scheduled under. The queue's per-destination rate limiters and circuit breakers are keyed by it.
const InvocationTaskGroup = chasm.CallbackLibraryName + ".invoke"

// DestinationBlockedFn reports whether the outbound queue is currently holding back callback
// deliveries to the given destination, i.e. whether its circuit breaker is open.
type DestinationBlockedFn func(namespaceID string, destination string) bool

type ctxKeyCallbackContextType struct{}

var ctxKeyCallbackContext = ctxKeyCallbackContextType{}

// callbackContext holds the dependencies injected into the chasm.Context for use by Callback methods.
type callbackContext struct {
	destinationBlocked DestinationBlockedFn
}

// callbackContextFromChasm extracts the callbackContext from a chasm.Context.
// Panics if the context value is missing, which indicates a library registration bug.
func callbackContextFromChasm(ctx chasm.Context) *callbackContext {
	//nolint:revive // unchecked-type-assertion: intentional panic on missing context value
	return ctx.Value(ctxKeyCallbackContext).(*callbackContext)
}

// Library is the CHASM library for callbacks.
type Library struct {
	chasm.UnimplementedLibrary

	invocationTaskHandler *invocationTaskHandler
	backoffTaskHandler    *backoffTaskHandler

	destinationBlocked DestinationBlockedFn
}

// NewNilLibrary creates a Library that cannot execute invocation tasks. Useful for
// registration-only contexts like tdbg where no task execution is needed.
func NewNilLibrary() *Library {
	return &Library{}
}

// NewLibrary returns the callback library. invocationTaskExecutor delivers callbacks.
// destinationBlocked reports whether deliveries to a destination are being held back; nil means
// they never are.
func NewLibrary(
	invocationTaskExecutor InvocationTaskExecutor,
	destinationBlocked DestinationBlockedFn,
) *Library {
	return &Library{
		invocationTaskHandler: &invocationTaskHandler{executor: invocationTaskExecutor},
		backoffTaskHandler:    &backoffTaskHandler{},
		destinationBlocked:    destinationBlocked,
	}
}

func (l *Library) Name() string {
	return chasm.CallbackLibraryName
}

func (l *Library) Components() []*chasm.RegistrableComponent {
	destinationBlocked := l.destinationBlocked
	if destinationBlocked == nil {
		// Processes that don't run the outbound queue never report a destination as blocked.
		destinationBlocked = func(string, string) bool { return false }
	}
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Callback](
			chasm.CallbackComponentName,
			chasm.WithDetached(),
			chasm.WithContextValues(map[any]any{
				ctxKeyCallbackContext: &callbackContext{
					destinationBlocked: destinationBlocked,
				},
			}),
		),
	}
}

func (l *Library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrableSideEffectTask(
			"invoke",
			l.invocationTaskHandler,
			chasm.WithTaskGroup(InvocationTaskGroup),
		),
		chasm.NewRegistrablePureTask(
			"backoff",
			l.backoffTaskHandler,
		),
	}
}
