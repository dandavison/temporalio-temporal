package activity

import (
	"context"

	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/resource"
	"go.uber.org/fx"
)

// matchingServiceAdapter adds activity tasks via the matching service.
type matchingServiceAdapter struct {
	client resource.MatchingClient
}

func (c matchingServiceAdapter) AddActivityTask(ctx context.Context, request *DispatchRequest) error {
	_, err := c.client.AddActivityTask(ctx, &matchingservice.AddActivityTaskRequest{
		NamespaceId:            request.NamespaceID,
		ScheduleToStartTimeout: request.ScheduleToStartTimeout,
		TaskQueue:              request.TaskQueue,
		Priority:               request.Priority,
		ComponentRef:           request.ComponentRef,
		Stamp:                  request.Stamp,
	})
	return err
}

type activityDispatchTaskHandlerParams struct {
	fx.In

	MatchingClient   resource.MatchingClient
	DispatchTaskHook DispatchTaskHook `optional:"true"`
}

func activityDispatchTaskHandlerProvider(params activityDispatchTaskHandlerParams) *activityDispatchTaskHandler {
	return newActivityDispatchTaskHandler(activityDispatchTaskHandlerOptions{
		MatchingClient:   matchingServiceAdapter{client: params.MatchingClient},
		DispatchTaskHook: params.DispatchTaskHook,
	})
}
