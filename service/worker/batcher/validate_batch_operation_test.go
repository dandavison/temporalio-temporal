package batcher

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchpb "go.temporal.io/api/batch/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
)

// activityBatchOps enumerates the three standalone-activity batch operations so
// that target-selector validation can be asserted identically across
// terminate/cancel/delete. They share a single validation arm, and a regression
// in it (rejecting the correct ArchetypeExecutions field instead of the
// deprecated Executions field) is invisible to unit tests unless that arm is
// exercised directly, which is what these cases do.
var activityBatchOps = []struct {
	name    string
	setOp   func(*workflowservice.StartBatchOperationRequest)
	expType enumspb.ExecutionType
}{
	{
		name: "terminate",
		setOp: func(r *workflowservice.StartBatchOperationRequest) {
			r.Operation = &workflowservice.StartBatchOperationRequest_TerminateActivitiesOperation{
				TerminateActivitiesOperation: &batchpb.BatchOperationTerminateActivities{},
			}
		},
		expType: enumspb.EXECUTION_TYPE_ACTIVITY,
	},
	{
		name: "cancel",
		setOp: func(r *workflowservice.StartBatchOperationRequest) {
			r.Operation = &workflowservice.StartBatchOperationRequest_CancelActivitiesOperation{
				CancelActivitiesOperation: &batchpb.BatchOperationCancelActivities{},
			}
		},
		expType: enumspb.EXECUTION_TYPE_ACTIVITY,
	},
	{
		name: "delete",
		setOp: func(r *workflowservice.StartBatchOperationRequest) {
			r.Operation = &workflowservice.StartBatchOperationRequest_DeleteActivitiesOperation{
				DeleteActivitiesOperation: &batchpb.BatchOperationDeleteActivities{},
			}
		},
		expType: enumspb.EXECUTION_TYPE_ACTIVITY,
	},
}

func activityExecution() *commonpb.Execution {
	return &commonpb.Execution{
		Type:       enumspb.EXECUTION_TYPE_ACTIVITY,
		BusinessId: "activity-id",
		RunId:      uuid.NewString(),
	}
}

func workflowExecution() *commonpb.Execution {
	return &commonpb.Execution{
		Type:       enumspb.EXECUTION_TYPE_WORKFLOW,
		BusinessId: "workflow-id",
		RunId:      uuid.NewString(),
	}
}

// baseRequest is a request that is valid except for its target selector and
// operation, which each test sets.
func baseRequest() *workflowservice.StartBatchOperationRequest {
	return &workflowservice.StartBatchOperationRequest{
		JobId:     uuid.NewString(),
		Namespace: "ns",
		Reason:    "test",
	}
}

func TestValidateBatchOperation_ActivityOperations(t *testing.T) {
	for _, op := range activityBatchOps {
		t.Run(op.name, func(t *testing.T) {
			t.Run("ArchetypeExecutions is accepted", func(t *testing.T) {
				req := baseRequest()
				op.setOp(req)
				req.ArchetypeExecutions = []*commonpb.Execution{activityExecution()}
				require.NoError(t, ValidateBatchOperation(req))
			})

			t.Run("VisibilityQuery is accepted", func(t *testing.T) {
				req := baseRequest()
				op.setOp(req)
				req.VisibilityQuery = "ActivityType='foo'"
				require.NoError(t, ValidateBatchOperation(req))
			})

			t.Run("deprecated Executions is rejected", func(t *testing.T) {
				req := baseRequest()
				op.setOp(req)
				//nolint:staticcheck // SA1019: exercising rejection of the deprecated Executions field
				req.Executions = []*commonpb.WorkflowExecution{{WorkflowId: "wf", RunId: uuid.NewString()}}
				err := ValidateBatchOperation(req)
				assert.ErrorContains(t, err, "use archetype executions")
			})

			t.Run("workflow-typed ArchetypeExecutions is rejected", func(t *testing.T) {
				req := baseRequest()
				op.setOp(req)
				req.ArchetypeExecutions = []*commonpb.Execution{workflowExecution()}
				err := ValidateBatchOperation(req)
				assert.ErrorContains(t, err, "requires")
			})
		})
	}
}

func TestValidateBatchOperation_WorkflowOperationsRejectActivityExecutions(t *testing.T) {
	req := baseRequest()
	req.Operation = &workflowservice.StartBatchOperationRequest_TerminationOperation{
		TerminationOperation: &batchpb.BatchOperationTermination{},
	}
	req.ArchetypeExecutions = []*commonpb.Execution{activityExecution()}
	err := ValidateBatchOperation(req)
	assert.ErrorContains(t, err, "requires")
}

func TestValidateBatchOperation_WorkflowOperationsAcceptDeprecatedExecutions(t *testing.T) {
	req := baseRequest()
	req.Operation = &workflowservice.StartBatchOperationRequest_TerminationOperation{
		TerminationOperation: &batchpb.BatchOperationTermination{},
	}
	//nolint:staticcheck // SA1019: the deprecated field remains valid for workflow operations
	req.Executions = []*commonpb.WorkflowExecution{{WorkflowId: "wf", RunId: uuid.NewString()}}
	require.NoError(t, ValidateBatchOperation(req))
}

func TestValidateBatchOperation_TargetSelectors(t *testing.T) {
	newActivityReq := func() *workflowservice.StartBatchOperationRequest {
		req := baseRequest()
		activityBatchOps[0].setOp(req)
		return req
	}

	t.Run("no selector is rejected", func(t *testing.T) {
		err := ValidateBatchOperation(newActivityReq())
		assert.Error(t, err)
	})

	t.Run("query and archetype executions are mutually exclusive", func(t *testing.T) {
		req := newActivityReq()
		req.VisibilityQuery = "ActivityType='foo'"
		req.ArchetypeExecutions = []*commonpb.Execution{activityExecution()}
		err := ValidateBatchOperation(req)
		assert.ErrorContains(t, err, "mutually exclusive")
	})

	t.Run("missing reason is rejected", func(t *testing.T) {
		req := newActivityReq()
		req.ArchetypeExecutions = []*commonpb.Execution{activityExecution()}
		req.Reason = ""
		assert.Error(t, ValidateBatchOperation(req))
	})
}
