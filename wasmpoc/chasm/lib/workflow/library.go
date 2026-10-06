package workflow

import (
	"context"

	taskqueuepb "go.temporal.io/api/taskqueue/v1"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/wasmpoc/chasm"
)

//go:generate protoc -I proto/v1 -I /opt/homebrew/include --go_out=paths=source_relative:gen/workflowpb/v1 proto/v1/workflow.proto

const libraryName = "localworkflow"

var (
	Archetype   = chasm.FullyQualifiedName(libraryName, "workflow")
	ArchetypeID = chasm.GenerateTypeID(Archetype)
)

// MatchingClient receives scheduled workflow tasks.
type MatchingClient interface {
	AddWorkflowTask(ctx context.Context, taskQueue *taskqueuepb.TaskQueue, ref chasm.ComponentRef, stamp int32) error
}

type library struct {
	chasm.UnimplementedLibrary
	matchingClient MatchingClient
}

func NewLibrary(matchingClient MatchingClient) chasm.Library {
	return &library{matchingClient: matchingClient}
}

func (l *library) Name() string {
	return libraryName
}

func (l *library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Workflow]("workflow",
			chasm.WithExecutionType(enumspb.EXECUTION_TYPE_WORKFLOW),
			chasm.WithBusinessIDAlias("WorkflowId"),
		),
		chasm.NewRegistrableComponent[*Timer]("timer"),
	}
}

func (l *library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrableSideEffectTask("dispatchWorkflowTask", &workflowTaskDispatchTaskHandler{matchingClient: l.matchingClient}),
		chasm.NewRegistrablePureTask("fireTimer", &timerFireTaskHandler{}),
	}
}
