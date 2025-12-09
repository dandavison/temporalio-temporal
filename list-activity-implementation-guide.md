# ListActivityExecutions Implementation Guide

A step-by-step guide to implementing `ListActivityExecutions` and `CountActivityExecutions` APIs for standalone activities.

---

## Step 1: Define Activity Search Attributes

Create search attribute definitions for the Activity component.

[chasm/lib/activity/activity.go](https://github.com/temporalio/temporal/blob/main/chasm/lib/activity/activity.go#L26) - Add after imports, before the `ActivityStore` interface:

```go
// Activity search attribute aliases
const (
	ActivityStatusSAAlias = "ActivityStatus"
	ActivityTypeSAAlias   = "ActivityType"
)

// Activity search attributes mapped to CHASM fields
var (
	ActivityStatusSearchAttribute = chasm.NewSearchAttributeKeyword(ActivityStatusSAAlias, chasm.SearchAttributeFieldKeyword01)
	ActivityTypeSearchAttribute   = chasm.NewSearchAttributeKeyword(ActivityTypeSAAlias, chasm.SearchAttributeFieldKeyword02)

	// Compile-time interface checks
	_ chasm.VisibilitySearchAttributesProvider = (*Activity)(nil)
	_ chasm.VisibilityMemoProvider             = (*Activity)(nil)
)
```

---

## Step 2: Define Activity Memo Proto

Create a proto message for the activity list memo. This contains **only** fields needed to populate `ActivityExecutionListInfo` that are NOT available from visibility system fields.

**Fields from visibility system (NOT needed in memo):**
- `activity_id` ← `ExecutionInfo.BusinessID`
- `run_id` ← `ExecutionInfo.RunID`
- `schedule_time` ← `ExecutionInfo.StartTime`
- `close_time` ← `ExecutionInfo.CloseTime`
- `state_transition_count` ← `ExecutionInfo.StateTransitionCount`
- `state_size_bytes` ← `ExecutionInfo.HistorySizeBytes`
- `search_attributes` ← `ExecutionInfo.CustomSearchAttributes`
- `execution_duration` ← computed from `close_time - schedule_time`

**Fields that MUST be in memo:**
- `activity_type` - not in visibility system
- `task_queue` - not in visibility system
- `status` - not in visibility system

[chasm/lib/activity/proto/v1/activity_state.proto](https://github.com/temporalio/temporal/blob/main/chasm/lib/activity/proto/v1/activity_state.proto) - Add at the end of the file:

```protobuf
// ActivityListMemo is stored in visibility to populate ListActivityExecutions responses.
// Contains only fields NOT available from visibility system fields.
message ActivityListMemo {
    string activity_type = 1;
    string task_queue = 2;
    ActivityExecutionStatus status = 3;
}
```

After editing, regenerate protos:
```bash
make proto
```

---

## Step 3: Implement VisibilitySearchAttributesProvider

Add the `SearchAttributes` method to the Activity component.

[chasm/lib/activity/activity.go](https://github.com/temporalio/temporal/blob/main/chasm/lib/activity/activity.go#L580) - Add at the end of the file:

```go
// SearchAttributes implements chasm.VisibilitySearchAttributesProvider interface.
// Returns the current search attribute values for this activity execution.
func (a *Activity) SearchAttributes(_ chasm.Context) []chasm.SearchAttributeKeyValue {
	return []chasm.SearchAttributeKeyValue{
		ActivityStatusSearchAttribute.Value(a.Status.String()),
		ActivityTypeSearchAttribute.Value(a.ActivityType.GetName()),
	}
}
```

---

## Step 4: Implement VisibilityMemoProvider

Add the `Memo` method to the Activity component.

[chasm/lib/activity/activity.go](https://github.com/temporalio/temporal/blob/main/chasm/lib/activity/activity.go#L580) - Add after `SearchAttributes`:

```go
// Memo implements chasm.VisibilityMemoProvider interface.
// Returns the memo data to be stored in visibility for list responses.
// Only includes fields NOT available from visibility system fields.
func (a *Activity) Memo(_ chasm.Context) proto.Message {
	return &activitypb.ActivityListMemo{
		ActivityType: a.ActivityType.GetName(),
		TaskQueue:    a.TaskQueue.GetName(),
		Status:       a.Status,
	}
}
```

---

## Step 5: Register Search Attributes with the Library

Update the activity library to register the search attributes with the CHASM registry.

[chasm/lib/activity/library.go](https://github.com/temporalio/temporal/blob/main/chasm/lib/activity/library.go#L21) - Modify the `Components()` method:

```go
func (l *componentOnlyLibrary) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Activity]("activity",
			chasm.WithSearchAttributes(
				ActivityStatusSearchAttribute,
				ActivityTypeSearchAttribute,
			),
		),
	}
}
```

---

## Step 6: Implement ListActivityExecutions in Frontend Handler

Add the `ListActivityExecutions` method to the frontend handler. This populates `ActivityExecutionListInfo` using:
- Visibility system fields (`ExecutionInfo.*`)
- Memo fields (`ChasmMemo.*`)

[chasm/lib/activity/frontend.go](https://github.com/temporalio/temporal/blob/main/chasm/lib/activity/frontend.go#L278) - Add at the end of the file:

```go
// ListActivityExecutions lists activity executions matching the given query.
func (h *frontendHandler) ListActivityExecutions(
	ctx context.Context,
	req *workflowservice.ListActivityExecutionsRequest,
) (*workflowservice.ListActivityExecutionsResponse, error) {
	if req == nil {
		return nil, serviceerror.NewInvalidArgument("request is nil")
	}

	namespaceID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(req.GetNamespace()))
	if err != nil {
		return nil, err
	}

	// Create context with visibility manager
	ctx = chasm.NewVisibilityManagerContext(ctx, h.visibilityManager)

	// TODO: validate page size against config

	chasmReq := &chasm.ListExecutionsRequest{
		NamespaceID:   namespaceID.String(),
		NamespaceName: req.GetNamespace(),
		PageSize:      int(req.GetPageSize()),
		NextPageToken: req.GetNextPageToken(),
		Query:         req.GetQuery(),
	}

	resp, err := chasm.ListExecutions[*Activity, *activitypb.ActivityListMemo](ctx, chasmReq)
	if err != nil {
		return nil, err
	}

	executions := make([]*apiactivitypb.ActivityExecutionListInfo, 0, len(resp.Executions))
	for _, exec := range resp.Executions {
		info := &apiactivitypb.ActivityExecutionListInfo{
			// From visibility system fields:
			ActivityId:           exec.BusinessID,
			RunId:                exec.RunID,
			ScheduleTime:         timestamppb.New(exec.StartTime),
			CloseTime:            timestamppb.New(exec.CloseTime),
			StateTransitionCount: exec.StateTransitionCount,
			StateSizeBytes:       exec.HistorySizeBytes,
			// TODO: SearchAttributes from exec.CustomSearchAttributes

			// From memo (fields not in visibility system):
			ActivityType: &commonpb.ActivityType{Name: exec.ChasmMemo.GetActivityType()},
			TaskQueue:    exec.ChasmMemo.GetTaskQueue(),
			Status:       activityStatusFromInternal(exec.ChasmMemo.GetStatus()),
		}

		// Compute execution duration if closed
		if !exec.CloseTime.IsZero() && !exec.StartTime.IsZero() {
			info.ExecutionDuration = durationpb.New(exec.CloseTime.Sub(exec.StartTime))
		}

		executions = append(executions, info)
	}

	return &workflowservice.ListActivityExecutionsResponse{
		Executions:    executions,
		NextPageToken: resp.NextPageToken,
	}, nil
}

// activityStatusFromInternal converts internal activity status to API status.
func activityStatusFromInternal(status activitypb.ActivityExecutionStatus) enumspb.ActivityExecutionStatus {
	switch status {
	case activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
		activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED:
		return enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING
	case activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED:
		return enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED
	case activitypb.ACTIVITY_EXECUTION_STATUS_FAILED:
		return enumspb.ACTIVITY_EXECUTION_STATUS_FAILED
	case activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED:
		return enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED
	case activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED:
		return enumspb.ACTIVITY_EXECUTION_STATUS_TERMINATED
	case activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT:
		return enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT
	default:
		return enumspb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED
	}
}
```

---

## Step 7: Implement CountActivityExecutions in Frontend Handler

Add the `CountActivityExecutions` method to the frontend handler.

[chasm/lib/activity/frontend.go](https://github.com/temporalio/temporal/blob/main/chasm/lib/activity/frontend.go#L278) - Add after `ListActivityExecutions`:

```go
// CountActivityExecutions counts activity executions matching the given query.
func (h *frontendHandler) CountActivityExecutions(
	ctx context.Context,
	req *workflowservice.CountActivityExecutionsRequest,
) (*workflowservice.CountActivityExecutionsResponse, error) {
	if req == nil {
		return nil, serviceerror.NewInvalidArgument("request is nil")
	}

	namespaceID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(req.GetNamespace()))
	if err != nil {
		return nil, err
	}

	// Create context with visibility manager
	ctx = chasm.NewVisibilityManagerContext(ctx, h.visibilityManager)

	chasmReq := &chasm.CountExecutionsRequest{
		NamespaceID:   namespaceID.String(),
		NamespaceName: req.GetNamespace(),
		Query:         req.GetQuery(),
	}

	resp, err := chasm.CountExecutions[*Activity](ctx, chasmReq)
	if err != nil {
		return nil, err
	}

	return &workflowservice.CountActivityExecutionsResponse{
		Count: resp.Count,
	}, nil
}
```

---

## Step 8: Add Required Imports to frontend.go

Update imports in the frontend handler file.

[chasm/lib/activity/frontend.go](https://github.com/temporalio/temporal/blob/main/chasm/lib/activity/frontend.go#L3) - Update the imports:

```go
import (
	"context"

	"github.com/google/uuid"
	apiactivitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/searchattribute"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)
```

---

## Step 9: Inject CHASM Visibility Manager Context

The frontend handler needs access to the CHASM visibility manager. The `chasm.ListExecutions` and `chasm.CountExecutions` functions retrieve the visibility manager from the context, so we must inject it.

[chasm/lib/activity/frontend.go](https://github.com/temporalio/temporal/blob/main/chasm/lib/activity/frontend.go#L32) - Add visibility manager to the struct:

```go
type frontendHandler struct {
	FrontendHandler
	client             activitypb.ActivityServiceClient
	dc                 *dynamicconfig.Collection
	logger             log.Logger
	metricsHandler     metrics.Handler
	namespaceRegistry  namespace.Registry
	saMapperProvider   searchattribute.MapperProvider
	saValidator        *searchattribute.Validator
	visibilityManager  chasm.VisibilityManager  // Add this field
}
```

Update `NewFrontendHandler` to accept and store the visibility manager:

```go
func NewFrontendHandler(
	client activitypb.ActivityServiceClient,
	dc *dynamicconfig.Collection,
	logger log.Logger,
	metricsHandler metrics.Handler,
	namespaceRegistry namespace.Registry,
	saMapperProvider searchattribute.MapperProvider,
	saValidator *searchattribute.Validator,
	visibilityManager chasm.VisibilityManager,  // Add this parameter
) FrontendHandler {
	return &frontendHandler{
		client:            client,
		dc:                dc,
		logger:            logger,
		metricsHandler:    metricsHandler,
		namespaceRegistry: namespaceRegistry,
		saMapperProvider:  saMapperProvider,
		saValidator:       saValidator,
		visibilityManager: visibilityManager,
	}
}
```

Then in each List/Count method, create the context with the visibility manager before calling CHASM functions:

```go
// Create context with visibility manager
ctx = chasm.NewVisibilityManagerContext(ctx, h.visibilityManager)
```

This line is already included in the Step 6 and Step 7 implementations above.

---

## Step 10: Implement DeleteActivityExecution (Stub)

Add a stub for `DeleteActivityExecution` if not already implemented.

[chasm/lib/activity/frontend.go](https://github.com/temporalio/temporal/blob/main/chasm/lib/activity/frontend.go#L278) - Add if missing:

```go
// DeleteActivityExecution deletes an activity execution record.
func (h *frontendHandler) DeleteActivityExecution(
	ctx context.Context,
	req *workflowservice.DeleteActivityExecutionRequest,
) (*workflowservice.DeleteActivityExecutionResponse, error) {
	return nil, serviceerror.NewUnimplemented("DeleteActivityExecution not implemented")
}
```

---

## Step 11: Update FX Provider (if needed)

Ensure the FX module provides the visibility manager to the frontend handler.

[chasm/lib/activity/fx.go](https://github.com/temporalio/temporal/blob/main/chasm/lib/activity/fx.go) - Update the provider to include visibility manager injection.

---

## Step 12: Run the Functional Test

The test in `tests/standalone_activity_test.go` (`TestListActivityExecutions`) should now pass:

```bash
go test -tags test_dep -v -run TestListActivityExecutions ./tests/...
```

---

## Summary of Files Changed

| File | Change |
|------|--------|
| `chasm/lib/activity/activity.go` | Add search attribute definitions, implement `SearchAttributes()` and `Memo()` |
| `chasm/lib/activity/proto/v1/activity_state.proto` | Add `ActivityListMemo` message (3 fields only) |
| `chasm/lib/activity/library.go` | Register search attributes with `WithSearchAttributes()` |
| `chasm/lib/activity/frontend.go` | Implement `ListActivityExecutions()`, `CountActivityExecutions()`, add visibility manager |
| `chasm/lib/activity/fx.go` | Wire visibility manager injection |

---

## Data Flow Summary

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        ActivityExecutionListInfo                             │
├─────────────────────────────────────────────────────────────────────────────┤
│  From Visibility System Fields:          │  From Memo:                      │
│  ─────────────────────────────────────   │  ────────────────────────────    │
│  activity_id      ← BusinessID           │  activity_type  ← memo.activity  │
│  run_id           ← RunID                │  task_queue     ← memo.task_q    │
│  schedule_time    ← StartTime            │  status         ← memo.status    │
│  close_time       ← CloseTime            │                                  │
│  state_transition ← StateTransitionCount │                                  │
│  state_size_bytes ← HistorySizeBytes     │                                  │
│  search_attrs     ← CustomSearchAttrs    │                                  │
│  exec_duration    ← computed             │                                  │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Query Examples

After implementation, these queries should work:

```sql
-- List all running activities
ActivityStatus = 'ACTIVITY_EXECUTION_STATUS_STARTED'

-- List activities of a specific type
ActivityType = 'my-activity-type'

-- List activities by ID (system field)
ActivityId = 'my-activity-id'

-- Combined query
ActivityStatus = 'ACTIVITY_EXECUTION_STATUS_STARTED' AND ActivityType = 'process-order'
```
