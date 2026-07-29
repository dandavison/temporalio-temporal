package unpauseactivity

import (
	"context"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
)

func Invoke(
	ctx context.Context,
	request *historyservice.UnpauseActivityRequest,
	shardContext historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (resp *historyservice.UnpauseActivityResponse, retError error) {
	var response *historyservice.UnpauseActivityResponse
	var metricsHandlers []metrics.Handler

	err := api.GetAndUpdateWorkflowWithNew(
		ctx,
		nil,
		definition.NewWorkflowKey(
			request.NamespaceId,
			request.GetFrontendRequest().GetExecution().GetWorkflowId(),
			request.GetFrontendRequest().GetExecution().GetRunId(),
		),
		func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			metricsHandlers = nil
			mutableState := workflowLease.GetMutableState()
			var err error
			var activityInfos []*persistencespb.ActivityInfo
			response, activityInfos, err = processUnpauseActivityRequest(shardContext, mutableState, request)
			if err != nil {
				return nil, err
			}
			for _, activityInfo := range activityInfos {
				metricsHandlers = append(metricsHandlers, workflow.GetPerActivityScope(
					shardContext,
					mutableState,
					activityInfo,
					metrics.ActivityUnpausedScope,
				))
			}
			return &api.UpdateWorkflowAction{
				Noop:               false,
				CreateWorkflowTask: false,
			}, nil
		},
		nil,
		shardContext,
		workflowConsistencyChecker,
	)

	if err != nil {
		return nil, err
	}

	frontendReq := request.GetFrontendRequest()
	for _, handler := range metricsHandlers {
		metrics.ActivityUnpause.With(handler).Record(1)
	}

	shardContext.GetLogger().Info("unpauseactivity: activity unpaused",
		tag.WorkflowNamespaceID(request.GetNamespaceId()),
		tag.WorkflowID(frontendReq.GetExecution().GetWorkflowId()),
		tag.WorkflowRunID(frontendReq.GetExecution().GetRunId()),
		tag.NewBoolTag("reset_attempts", frontendReq.GetResetAttempts()),
		tag.NewBoolTag("reset_heartbeat", frontendReq.GetResetHeartbeat()),
		tag.NewDurationTag("jitter", frontendReq.GetJitter().AsDuration()),
	)

	return response, err
}

func processUnpauseActivityRequest(
	shardContext historyi.ShardContext,
	mutableState historyi.MutableState,
	request *historyservice.UnpauseActivityRequest,
) (*historyservice.UnpauseActivityResponse, []*persistencespb.ActivityInfo, error) {

	if !mutableState.IsWorkflowExecutionRunning() {
		return nil, nil, consts.ErrWorkflowCompleted
	}
	frontendRequest := request.GetFrontendRequest()
	var activityIDs []string
	switch a := frontendRequest.GetActivity().(type) {
	case *workflowservice.UnpauseActivityRequest_Id:
		activityIDs = append(activityIDs, a.Id)
	case *workflowservice.UnpauseActivityRequest_Type:
		activityType := a.Type
		for _, ai := range mutableState.GetPendingActivityInfos() {
			if ai.ActivityType.Name == activityType {
				activityIDs = append(activityIDs, ai.ActivityId)
			}
		}
	case *workflowservice.UnpauseActivityRequest_UnpauseAll:
		for _, ai := range mutableState.GetPendingActivityInfos() {
			activityIDs = append(activityIDs, ai.ActivityId)
		}
	}

	if len(activityIDs) == 0 {
		return nil, nil, consts.ErrActivityNotFound
	}

	activityInfos := make([]*persistencespb.ActivityInfo, 0, len(activityIDs))
	for _, activityId := range activityIDs {

		ai, activityFound := mutableState.GetActivityByActivityID(activityId)

		if !activityFound {
			return nil, nil, consts.ErrActivityNotFound
		}

		if !ai.Paused {
			// do nothing
			continue
		}

		if err := workflow.UnpauseActivity(
			shardContext, mutableState, ai,
			frontendRequest.GetResetAttempts(),
			frontendRequest.GetResetHeartbeat(),
			frontendRequest.GetJitter().AsDuration()); err != nil {
			return nil, nil, err
		}
		activityInfos = append(activityInfos, ai)
	}

	return &historyservice.UnpauseActivityResponse{}, activityInfos, nil
}
