package tests

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

func (s *standaloneActivityTestSuite) TestEagerStartRetryRedeliversTask() {
	env := s.newTestEnv()
	t := s.T()
	ctx := s.Context()
	request := newEagerStartRequest(env, t, testcore.RandomizeStr(t.Name()))

	first, err := env.FrontendClient().StartActivityExecution(ctx, request)
	require.NoError(t, err)
	require.NotNil(t, first.GetEagerActivityTask())

	// A client whose first response was lost retries with the same request ID. Eager workflow start
	// returns the task again while the first attempt is still started.
	retry, err := env.FrontendClient().StartActivityExecution(ctx, request)
	require.NoError(t, err)
	require.NotNil(t, retry.GetEagerActivityTask(), "a retried start must redeliver the eager task")
	require.EqualValues(t, 1, retry.GetEagerActivityTask().GetAttempt())

	_, err = env.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
		Namespace: env.Namespace().String(),
		TaskToken: retry.GetEagerActivityTask().GetTaskToken(),
		Result:    defaultResult,
		Identity:  defaultIdentity,
	})
	require.NoError(t, err)
	env.validateCompletion(ctx, t, request.GetActivityId(), first.GetRunId(), defaultIdentity)
}

func (s *standaloneActivityTestSuite) TestEagerStartRespectsWorkerVersioning() {
	env := s.newTestEnv(
		testcore.WithWorkerService("worker deployment workflows"),
		testcore.WithDynamicConfig(dynamicconfig.MatchingDeploymentWorkflowVersion, int(versioning3DeploymentWorkflowVersion)),
		testcore.WithTestVars(func(tv *testvars.TestVars) *testvars.TestVars {
			return tv.WithDeploymentSeries("v3").WithBuildID("b")
		}),
	)
	versioningEnv := &VersioningTestEnv{TestEnv: env.TestEnv}
	t := s.T()
	ctx := s.Context()
	tv := env.Tv()

	versioningEnv.pollUntilRegistered(s, tv, tqTypeAct)
	versioningEnv.setCurrentDeployment(s, tv)

	// The request cannot express the caller's worker version, so the server cannot know that the
	// caller's worker is the version that versioning would select.
	request := newEagerStartRequest(env, t, tv.TaskQueue().GetName())
	response, err := env.FrontendClient().StartActivityExecution(ctx, request)
	require.NoError(t, err)
	require.Nil(t, response.GetEagerActivityTask(), "eager start must not bypass worker versioning")

	versioningEnv.pollActivityAndHandle(s, tv, nil,
		func(task *workflowservice.PollActivityTaskQueueResponse) (*workflowservice.RespondActivityTaskCompletedRequest, error) {
			require.Equal(t, request.GetActivityId(), task.GetActivityId())
			return &workflowservice.RespondActivityTaskCompletedRequest{Result: defaultResult}, nil
		})
}

func (s *standaloneActivityTestSuite) TestEagerStartDisabledByActivityEagerExecutionConfig() {
	env := s.newTestEnv(testcore.WithDynamicConfig(dynamicconfig.EnableActivityEagerExecution, false))
	t := s.T()

	response, err := env.FrontendClient().StartActivityExecution(s.Context(),
		newEagerStartRequest(env, t, testcore.RandomizeStr(t.Name())))
	require.NoError(t, err)
	require.Nil(t, response.GetEagerActivityTask(),
		"system.enableActivityEagerExecution=false must disable eager standalone activity start")
}

func (s *standaloneActivityTestSuite) TestEagerStartDenialIsCounted() {
	env := s.newTestEnv()
	t := s.T()
	metricCapture := env.StartNamespaceMetricCapture()

	request := newEagerStartRequest(env, t, testcore.RandomizeStr(t.Name()))
	request.StartDelay = durationpb.New(time.Minute)
	response, err := env.FrontendClient().StartActivityExecution(s.Context(), request)
	require.NoError(t, err)
	require.Nil(t, response.GetEagerActivityTask())

	require.Len(t, metricCapture.Metric("activity_eager_execution_denied"), 1,
		"a denied eager start must be counted, as for eager workflow start")
}

func newEagerStartRequest(env *standaloneActivityEnv, t *testing.T, taskQueue string) *workflowservice.StartActivityExecutionRequest {
	return &workflowservice.StartActivityExecutionRequest{
		Namespace:             env.Namespace().String(),
		RequestEagerExecution: true,
		ActivityId:            testcore.RandomizeStr(t.Name()),
		ActivityType:          env.Tv().ActivityType(),
		Identity:              defaultIdentity,
		Input:                 defaultInput,
		TaskQueue:             &taskqueuepb.TaskQueue{Name: taskQueue},
		StartToCloseTimeout:   durationpb.New(defaultStartToCloseTimeout),
		RequestId:             testcore.RandomizeStr(t.Name()),
	}
}
