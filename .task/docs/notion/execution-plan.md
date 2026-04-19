---
title: "Standalone Activities: execution plan"
notion_url: "https://www.notion.so/2858fc56773880c3a412e7414e59b954"
last_edited: "2025-10-09T19:17:42.370Z"
page_type: design doc
---

# Overview
Ultimately, Standalone Activity (SA) will be two things:
1. A request that an end-user can submit for an activity to be executed by a worker in their own namespace
2. An important way for Nexus handlers to implement operations

Our initial focus is (1). The implementation of (2) will involve adding support to SDK Nexus handlers for a new `ActivityRunOperation` that will use (1) to start an SA with a cross-namespace Nexus callback and links.

Initially, we will implement (1) as a new workflowservice gRPC `StartActivityExecution` request that creates an Activity CHASM component in the caller namespace. The only way for callers to obtain the result will be to poll the CHASM component (unlike Nexus non-workflow callers, there will be no push-based result delivery via Matching).

(Later, we will re-implement (1) as a within-namespace Nexus operation using a special "System" endpoint and a generic nexus operation with a name like "StartActivity". The Nexus request will continue to be handled by creating the Activity CHASM component. This phase of work is not ready to start and can be ignored for now. It will leave `StartActivityExecution` as a deprecated API.)

# Implementation walkthrough
Python: `client.start_activity(activity, input, ...)`

`StartActivityExecution` gRPC => FE
	Future: w/ Nexus callback

[`func NewWorkflowHandler`](https://github.com/temporalio/temporal/blob/fredtzeng/standalone-activity/service/frontend/workflow_handler.go#L152) creates `workflowservice` gRPC service and is passed a CHASM `activity.FrontendHandler`.

In `/chasm/lib/activity/` the activity component has created a gRPC service
```go
type ActivityServiceClient interface {
	StartActivityExecution(ctx context.Context, in *StartActivityExecutionRequest, opts ...grpc.CallOption) (*StartActivityExecutionResponse, error)
	DescribeActivityExecution(ctx context.Context, in *DescribeActivityExecutionRequest, opts ...grpc.CallOption) (*DescribeActivityExecutionResponse, error)
}
```

Create CHASM component with initial tasks (Activity transfer task, visibility)
Activity is executed by worker subject to retry policy

Worker `RespondActivityTaskCompleted` => FE => History => CHASM component => transition to terminal state

# Execution plan
https://temporalio.atlassian.net/browse/ACT-1

### New workflowservice gRPC APIs
`StartActivityExecution`
`GetActivityExecutionResult`
`RequestCancelActivityExecution`
### Python client API prototype
### Python worker protototype

## Support Standalone Activity in Nexus handlers
- Add Nexus callback to SA CHASM component
- Python `ActivityRunOperation` prototype
