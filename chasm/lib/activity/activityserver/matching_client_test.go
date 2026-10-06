package activityserver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/common/testing/protorequire"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestMatchingServiceAdapter(t *testing.T) {
	controller := gomock.NewController(t)
	client := matchingservicemock.NewMockMatchingServiceClient(controller)
	client.EXPECT().AddActivityTask(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *matchingservice.AddActivityTaskRequest, _ ...any) (*matchingservice.AddActivityTaskResponse, error) {
			protorequire.ProtoEqual(t, &matchingservice.AddActivityTaskRequest{
				NamespaceId:            "namespace-id",
				ScheduleToStartTimeout: durationpb.New(5),
				TaskQueue:              &taskqueuepb.TaskQueue{Name: "task-queue"},
				Priority:               &commonpb.Priority{FairnessKey: "key"},
				ComponentRef:           []byte("component-ref"),
				Stamp:                  7,
			}, request)
			return &matchingservice.AddActivityTaskResponse{}, nil
		},
	)

	err := matchingServiceAdapter{client: client}.AddActivityTask(context.Background(), &activity.DispatchRequest{
		NamespaceID:            "namespace-id",
		ScheduleToStartTimeout: durationpb.New(5),
		TaskQueue:              &taskqueuepb.TaskQueue{Name: "task-queue"},
		Priority:               &commonpb.Priority{FairnessKey: "key"},
		ComponentRef:           []byte("component-ref"),
		Stamp:                  7,
	})
	require.NoError(t, err)
}
