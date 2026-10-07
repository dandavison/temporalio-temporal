package temporaltest_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/server/api/adminservice/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/localexecution"
	"go.temporal.io/server/temporaltest"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// A local server syncs a completed workflow task that schedules an activity, then stops syncing, as
// it does when its process dies. Once the lease expires, the server takes the run back and an
// ordinary worker receives the activity.
func TestLocalExecutionLeaseExpiresAfterSync(t *testing.T) {
	server := temporaltest.NewServer(
		temporaltest.WithT(t),
		temporaltest.WithDynamicConfig(dynamicconfig.EnableLocalExecution, true),
	)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	const taskQueue = "local-execution-lease-expiry"

	run, err := server.GetDefaultClient().ExecuteWorkflow(
		ctx,
		sdkclient.StartWorkflowOptions{ID: "local-execution-lease-expiry", TaskQueue: taskQueue},
		"unregistered-workflow",
	)
	require.NoError(t, err)
	execution := &commonpb.WorkflowExecution{WorkflowId: run.GetID(), RunId: run.GetRunID()}

	acquired, err := server.GetDefaultClient().WorkflowService().PollWorkflowTaskQueue(
		ctx,
		&workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: server.GetDefaultNamespace(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  "local-server",
			LocalExecutionOptions: &workflowservice.LocalExecutionPollOptions{
				LocalServerId:          "local-server",
				ProtocolVersion:        localexecution.ProtocolVersion,
				SyncInterval:           durationpb.New(time.Second),
				RequestedLeaseDuration: durationpb.New(3 * time.Second),
			},
		},
	)
	require.NoError(t, err)
	ownership := acquired.GetLocalExecutionInfo()
	require.Equal(t, int64(2), ownership.GetLastSynchronizedEventId())

	now := time.Now()
	batches := [][]*historypb.HistoryEvent{
		{{
			EventId:   3,
			EventTime: timestamppb.New(now),
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
			Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
				ScheduledEventId: 2,
				Identity:         "local-worker",
			}},
		}},
		{
			{
				EventId:   4,
				EventTime: timestamppb.New(now),
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
				Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
					ScheduledEventId: 2,
					StartedEventId:   3,
					Identity:         "local-worker",
				}},
			},
			{
				EventId:   5,
				EventTime: timestamppb.New(now),
				EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
				Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
					ActivityId:                   "step",
					ActivityType:                 &commonpb.ActivityType{Name: "step"},
					TaskQueue:                    &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					ScheduleToCloseTimeout:       durationpb.New(time.Minute),
					ScheduleToStartTimeout:       durationpb.New(time.Minute),
					StartToCloseTimeout:          durationpb.New(time.Minute),
					HeartbeatTimeout:             durationpb.New(0),
					WorkflowTaskCompletedEventId: 4,
				}},
			},
		},
	}
	serializer := serialization.NewSerializer()
	var blobs []*commonpb.DataBlob
	for _, batch := range batches {
		blob, err := serializer.SerializeEvents(batch)
		require.NoError(t, err)
		blobs = append(blobs, blob)
	}
	adminClient, _ := localFirstAdminClient(ctx, t, server)
	_, err = adminClient.SyncLocalExecution(ctx, &adminservice.SyncLocalExecutionRequest{
		Namespace:            server.GetDefaultNamespace(),
		Execution:            execution,
		ProtocolVersion:      localexecution.ProtocolVersion,
		LocalServerId:        "local-server",
		SyncId:               "first-sync",
		PreviousEventId:      2,
		PreviousEventVersion: ownership.GetLastSynchronizedEventVersion(),
		NewEventId:           5,
		NewEventVersion:      ownership.GetLastSynchronizedEventVersion(),
		HistoryBatches:       blobs,
		VersionHistory: &historyspb.VersionHistory{Items: []*historyspb.VersionHistoryItem{{
			EventId: 5,
			Version: ownership.GetLastSynchronizedEventVersion(),
		}}},
		OwnershipToken: ownership.GetOwnershipToken(),
		FencingEpoch:   ownership.GetFencingEpoch(),
	})
	require.NoError(t, err)

	activityTask, err := server.GetDefaultClient().WorkflowService().PollActivityTaskQueue(
		ctx,
		&workflowservice.PollActivityTaskQueueRequest{
			Namespace: server.GetDefaultNamespace(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  "ordinary-worker",
		},
	)
	require.NoError(t, err)
	require.Equal(t, "step", activityTask.GetActivityId())

	described, err := adminClient.DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: server.GetDefaultNamespace(),
		Execution: execution,
	})
	require.NoError(t, err)
	localInfo := described.GetDatabaseMutableState().GetExecutionInfo().GetLocalExecutionInfo()
	require.Equal(t, persistencespb.LocalExecutionInfo_STATE_UNOWNED, localInfo.GetState())
	require.Greater(t, localInfo.GetFencingEpoch(), ownership.GetFencingEpoch())
}
