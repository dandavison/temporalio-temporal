package activity

import (
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
)

type ctxKeyActivityContextType struct{}

var ctxKeyActivityContext = ctxKeyActivityContextType{}

// activityContext holds dependencies injected into the chasm.Context for use by Activity methods.
type activityContext struct {
	config *Config
}

// activityContextFromChasm extracts the activityContext from a chasm.Context.
// Panics if the context value is missing, which indicates a library registration bug.
func activityContextFromChasm(ctx chasm.Context) *activityContext {
	//nolint:revive // unchecked-type-assertion: intentional panic on missing context value
	return ctx.Value(ctxKeyActivityContext).(*activityContext)
}

const (
	libraryName   = "activity"
	componentName = "activity"
)

var (
	Archetype   = chasm.FullyQualifiedName(libraryName, componentName)
	ArchetypeID = chasm.GenerateTypeID(Archetype)
)

type componentOnlyLibrary struct {
	chasm.UnimplementedLibrary
	config *Config
}

// NewComponentOnlyLibrary returns the activity library without task handlers, for processes
// that only need the component registered, e.g. to serialize component references.
func NewComponentOnlyLibrary(
	config *Config,
) chasm.Library {
	return newComponentOnlyLibrary(config)
}

func newComponentOnlyLibrary(
	config *Config,
) *componentOnlyLibrary {
	return &componentOnlyLibrary{
		config: config,
	}
}

func (l *componentOnlyLibrary) Name() string {
	return libraryName
}

func (l *componentOnlyLibrary) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Activity](
			componentName,
			chasm.WithExecutionType(enumspb.EXECUTION_TYPE_ACTIVITY),
			chasm.WithSearchAttributes(
				TypeSearchAttribute,
				StatusSearchAttribute,
				chasm.SearchAttributeTaskQueue,
				chasm.SearchAttributeExecutionTime,
			),
			chasm.WithBusinessIDAlias("ActivityId"),
			chasm.WithContextValues(map[any]any{
				ctxKeyActivityContext: &activityContext{
					config: l.config,
				},
			}),
		),
	}
}

// NewNilLibrary creates a Library with all nil handlers. Useful for
// registration-only contexts like tdbg where no task execution is needed.
func NewNilLibrary() chasm.Library {
	return &library{
		componentOnlyLibrary: *newComponentOnlyLibrary(nil),
	}
}

type library struct {
	componentOnlyLibrary

	activityDispatchTaskHandler       *activityDispatchTaskHandler
	scheduleToStartTimeoutTaskHandler *scheduleToStartTimeoutTaskHandler
	scheduleToCloseTimeoutTaskHandler *scheduleToCloseTimeoutTaskHandler
	startToCloseTimeoutTaskHandler    *startToCloseTimeoutTaskHandler
	heartbeatTimeoutTaskHandler       *heartbeatTimeoutTaskHandler
}

// NewLibrary returns the activity library. Activity tasks are added to task queues with
// matchingClient; dispatchTaskHook, if not nil, wraps each dispatch (see DispatchTaskHook).
func NewLibrary(
	matchingClient MatchingClient,
	dispatchTaskHook DispatchTaskHook,
	config *Config,
) chasm.Library {
	return &library{
		componentOnlyLibrary: *newComponentOnlyLibrary(config),
		activityDispatchTaskHandler: newActivityDispatchTaskHandler(activityDispatchTaskHandlerOptions{
			MatchingClient:   matchingClient,
			DispatchTaskHook: dispatchTaskHook,
		}),
		scheduleToStartTimeoutTaskHandler: newScheduleToStartTimeoutTaskHandler(),
		scheduleToCloseTimeoutTaskHandler: newScheduleToCloseTimeoutTaskHandler(),
		startToCloseTimeoutTaskHandler:    newStartToCloseTimeoutTaskHandler(),
		heartbeatTimeoutTaskHandler:       newHeartbeatTimeoutTaskHandler(),
	}
}

func (l *library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrableSideEffectTask(
			"dispatch",
			l.activityDispatchTaskHandler,
		),
		chasm.NewRegistrablePureTask(
			"scheduleToStartTimer",
			l.scheduleToStartTimeoutTaskHandler,
		),
		chasm.NewRegistrablePureTask(
			"scheduleToCloseTimer",
			l.scheduleToCloseTimeoutTaskHandler,
		),
		chasm.NewRegistrablePureTask(
			"startToCloseTimer",
			l.startToCloseTimeoutTaskHandler,
		),
		chasm.NewRegistrablePureTask(
			"heartbeatTimer",
			l.heartbeatTimeoutTaskHandler,
		),
	}
}
