// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package pollupdate

import (
	"context"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"

	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/history/workflow/update"
)

const (
	unspecifiedStage = enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED
	acceptedStage    = enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED
	completedStage   = enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED
)

func Invoke(
	ctx context.Context,
	req *historyservice.PollWorkflowExecutionUpdateRequest,
	shardContext shard.Context,
	ctxLookup api.WorkflowConsistencyChecker,
) (*historyservice.PollWorkflowExecutionUpdateResponse, error) {
	waitStage := req.GetRequest().GetWaitPolicy().GetLifecycleStage()
	updateRef := req.GetRequest().GetUpdateRef()
	wfexec := updateRef.GetWorkflowExecution()
	wfKey, upd, ok, err := func() (*definition.WorkflowKey, *update.Update, bool, error) {
		wfctx, err := ctxLookup.GetWorkflowContext(
			ctx,
			nil,
			api.BypassMutableStateConsistencyPredicate,
			definition.NewWorkflowKey(
				req.GetNamespaceId(),
				wfexec.GetWorkflowId(),
				wfexec.GetRunId(),
			),
			workflow.LockPriorityHigh,
		)
		if err != nil {
			return nil, nil, false, err
		}
		release := wfctx.GetReleaseFn()
		defer release(nil)
		upd, found := wfctx.GetUpdateRegistry(ctx).Find(ctx, updateRef.UpdateId)
		wfKey := wfctx.GetWorkflowKey()
		return &wfKey, upd, found, nil
	}()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, serviceerror.NewNotFound(fmt.Sprintf("update %q not found", updateRef.GetUpdateId()))
	}

	var stage enumspb.UpdateWorkflowExecutionLifecycleStage
	var outcome *updatepb.Outcome

	switch waitStage {
	case unspecifiedStage:
		stage, outcome, err = upd.LifecycleStage()
		if err != nil {
			return nil, err
		}
	case acceptedStage, completedStage:
		stage, outcome, err = waitLifecycleStage(ctx, upd, waitStage, shardContext, req.GetNamespaceId())
		if err != nil {
			return nil, err
		}
	default:
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("support for LifecycleStage=%v is not implemented", waitStage))
	}
	return &historyservice.PollWorkflowExecutionUpdateResponse{
		Response: &workflowservice.PollWorkflowExecutionUpdateResponse{
			Outcome: outcome,
			Stage:   stage,
			UpdateRef: &updatepb.UpdateRef{
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: wfKey.WorkflowID,
					RunId:      wfKey.RunID,
				},
				UpdateId: updateRef.UpdateId,
			},
		},
	}, nil
}

func waitLifecycleStage(ctx context.Context, upd *update.Update,
	waitStage enumspb.UpdateWorkflowExecutionLifecycleStage,
	shardContext shard.Context, namespaceId string) (stage enumspb.UpdateWorkflowExecutionLifecycleStage, outcome *updatepb.Outcome, err error) {

	namespaceID := namespace.ID(namespaceId)
	namespaceRegistry, err := shardContext.GetNamespaceRegistry().GetNamespaceByID(namespaceID)
	if err != nil {
		return unspecifiedStage, nil, err
	}
	serverTimeout := shardContext.GetConfig().LongPollExpirationInterval(namespaceRegistry.Name().String())
	// If the long-poll times out due to serverTimeout then return a non-error empty response.
	return upd.WaitLifecycleStage(ctx, waitStage, serverTimeout)
}
