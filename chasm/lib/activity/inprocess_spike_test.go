package activity

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
)

// TestInProcessSpike proves the tier-2 foundation: a real in-memory CHASM engine + component-only
// activity library + virtual clock, driving start / poll / fail through the production component
// methods and observing internal state via ReadComponent. No onebox, no wall clock.
func TestInProcessSpike(t *testing.T) {
	const ns = "spike-ns"
	nsReg := namespace.NewMockRegistry(gomock.NewController(t))
	nsReg.EXPECT().GetNamespaceName(gomock.Any()).Return(namespace.Name(ns), nil).AnyTimes()
	registry := chasm.NewRegistry(log.NewNoopLogger())
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(newComponentOnlyLibrary(ConfigProvider(dynamicconfig.NewNoopCollection()), nsReg)))

	ts := clock.NewEventTimeSource()
	ts.Update(time.Now())
	engine := chasmtest.NewEngine(t, registry, chasmtest.WithTimeSource(ts))
	ctx := chasm.NewEngineContext(context.Background(), engine)

	const activityID = "spike-act"
	startReq := &workflowservice.StartActivityExecutionRequest{
		Namespace:           ns,
		ActivityId:          activityID,
		ActivityType:        &commonpb.ActivityType{Name: "spike-type"},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: activityID},
		StartToCloseTimeout: durationpb.New(2 * time.Second),
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval: durationpb.New(time.Second), BackoffCoefficient: 1.0,
			MaximumInterval: durationpb.New(time.Second), MaximumAttempts: 3,
		},
		RequestId: uuid.NewString(),
	}

	result, err := chasm.StartExecution(ctx, chasm.ExecutionKey{NamespaceID: ns, BusinessID: activityID},
		func(mc chasm.MutableContext, req *workflowservice.StartActivityExecutionRequest) (*Activity, error) {
			a, err := NewStandaloneActivity(mc, req)
			if err != nil {
				return nil, err
			}
			return a, TransitionScheduled.Apply(a, mc, nil)
		},
		startReq,
		chasm.WithRequestID(startReq.RequestId),
		chasm.WithBusinessIDPolicy(chasm.BusinessIDReusePolicyAllowDuplicate, chasm.BusinessIDConflictPolicyFail),
	)
	require.NoError(t, err)
	ref := chasm.NewComponentRef[*Activity](chasm.ExecutionKey{NamespaceID: ns, BusinessID: activityID, RunID: result.ExecutionKey.RunID})

	observe := func() model.AbstractState {
		o, err := chasm.ReadComponent(ctx, ref, func(a *Activity, cctx chasm.Context, _ struct{}) (model.Observed, error) {
			attempt := a.LastAttempt.Get(cctx)
			return model.Observed{
				Status: a.GetStatus(), Count: attempt.GetCount(), Stamp: attempt.GetStamp(),
				DispatchTimeSet: attempt.GetDispatchTime() != nil,
			}, nil
		}, struct{}{})
		require.NoError(t, err)
		return model.Abstract(o)
	}

	// token mints a task token for the current attempt (attempt/stamp 0 => by-id, skipping those checks).
	token := func() *tokenspb.Task {
		refBytes, err := chasm.ReadComponent(ctx, ref, func(a *Activity, cctx chasm.Context, _ struct{}) ([]byte, error) {
			return cctx.Ref(a)
		}, struct{}{})
		require.NoError(t, err)
		return &tokenspb.Task{ComponentRef: refBytes}
	}

	require.Equal(t, model.Scheduled, observe().Status)

	// Poll -> Started (HandleStarted, the RecordActivityTaskStarted path).
	stamp, err := chasm.ReadComponent(ctx, ref, func(a *Activity, cctx chasm.Context, _ struct{}) (int32, error) {
		return a.LastAttempt.Get(cctx).GetStamp(), nil
	}, struct{}{})
	require.NoError(t, err)
	_, _, err = chasm.UpdateComponent(ctx, ref, func(a *Activity, mc chasm.MutableContext, _ any) (any, error) {
		return a.HandleStarted(mc, &historyservice.RecordActivityTaskStartedRequest{
			Stamp:       stamp,
			PollRequest: &workflowservice.PollActivityTaskQueueRequest{Namespace: ns, Identity: "worker"},
		})
	}, nil)
	require.NoError(t, err)
	require.Equal(t, model.Started, observe().Status)

	// RespondFailed (retryable) -> SCHEDULED for retry, attempt 2.
	_, _, err = chasm.UpdateComponent(ctx, ref, func(a *Activity, mc chasm.MutableContext, _ any) (any, error) {
		return a.HandleFailed(mc, RespondFailedEvent{
			Token: token(),
			Request: &historyservice.RespondActivityTaskFailedRequest{
				NamespaceId: ns,
				FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
					Identity: "worker",
					Failure: &failurepb.Failure{Message: "drive", FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
						ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "drive"}}},
				},
			},
		})
	}, nil)
	require.NoError(t, err)
	st := observe()
	require.Equal(t, model.Scheduled, st.Status)
	require.EqualValues(t, 2, st.Count)

	t.Logf("spike OK: reached %s attempt %d in-process with virtual clock", st.Status, st.Count)
}
