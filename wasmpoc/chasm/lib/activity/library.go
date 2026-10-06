package activity

import (
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/wasmpoc/chasm"
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

// NewLibrary returns the activity library with its task handlers, dispatching activity tasks via
// matchingClient.
func NewLibrary(matchingClient MatchingClient, config *Config) chasm.Library {
	return newLibrary(
		newActivityDispatchTaskHandler(activityDispatchTaskHandlerOptions{MatchingClient: matchingClient}),
		newScheduleToStartTimeoutTaskHandler(),
		newScheduleToCloseTimeoutTaskHandler(),
		newStartToCloseTimeoutTaskHandler(),
		newHeartbeatTimeoutTaskHandler(),
		config,
	)
}

func newLibrary(
	activityDispatchTaskHandler *activityDispatchTaskHandler,
	scheduleToStartTimeoutTaskHandler *scheduleToStartTimeoutTaskHandler,
	scheduleToCloseTimeoutTaskHandler *scheduleToCloseTimeoutTaskHandler,
	startToCloseTimeoutTaskHandler *startToCloseTimeoutTaskHandler,
	heartbeatTimeoutTaskHandler *heartbeatTimeoutTaskHandler,
	config *Config,
) *library {
	return &library{
		componentOnlyLibrary:              *newComponentOnlyLibrary(config),
		activityDispatchTaskHandler:       activityDispatchTaskHandler,
		scheduleToStartTimeoutTaskHandler: scheduleToStartTimeoutTaskHandler,
		scheduleToCloseTimeoutTaskHandler: scheduleToCloseTimeoutTaskHandler,
		startToCloseTimeoutTaskHandler:    startToCloseTimeoutTaskHandler,
		heartbeatTimeoutTaskHandler:       heartbeatTimeoutTaskHandler,
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
