package activityserver

import (
	"context"

	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/common/resource"
)

// matchingServiceAdapter adds activity tasks via the matching service.
type matchingServiceAdapter struct {
	client resource.MatchingClient
}

func (c matchingServiceAdapter) AddActivityTask(ctx context.Context, request *activity.DispatchRequest) error {
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
