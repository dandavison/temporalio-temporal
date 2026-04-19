---
title: "Standalone Activity Design Options"
notion_url: "https://www.notion.so/2298fc56773880e192e9c83d1b4efca6"
last_edited: "2025-07-17T23:17:28.185Z"
page_type: design doc
---

## Option 1 - Completely new set of server APIs
- `ActivityService`
	- `StartActivityExecution`
	- `DescribeActivity`
	- `ListActivities`
	- …
### Pros
- New powerful, tailored, explicit experience
- Easier to optimize for
- Full flexibility in the design and implementation
### Cons
- "New" primitive with a new way of listing and addressing activities
- Limited to single activity, cannot be extended to support arbitrary number of steps
## Option 2 - Only new SDK client APIs
- `client.startActivity`
- Server implementation is `StartWorkflowExecution` + new options (e.g. `ScheduleActivity` command)
	- This is similar to "optimistic workflow start" - a concept that has not been fully designed
### Pros
- Standalone activities are found and addresses the same way as workflow activities
- Reduces the set of server APIs we maintain
### Cons
- Not very straightforward experience, the implicit workflow may be confusing for users
## Option 3 - Workflow task gets upgraded to activity task
- Client starts a workflow, does not know about single-activity workflows
- First workflow task gets "upgraded" to an activity task. Draft design idea - needs to be fleshed out:
	- Essentially the SDK would start the activity locally if it can reserve an activity task slot and allow it to continue as a non-local activity - we want this for improving the local activity experience anyways and removing the concept of a local activity from the user-facing SDK API (discussion needed)
	- SDK would complete the workflow task with a `ScheduleActivity` + request eager execution command when the workflow task timeout approaches
	- Once the activity completes, the SDK wakes up the workflow again and generates `ActivityTaskStarted`, `ActivityTaskCompleted` and `WorkflowExecutionCompleted` events
- Note that has the same problem as local activity blocking new workflow tasks from being delivered to the SDK, it would make this problem worse
	- We can prioritize fixing that, exact solution TBD, there's a variety of options ranging from small hacks to a complete redesign of the server ↔ workflow-worker protocol
### Pros
- Does not introduce new top level concept
- Client does not get the false impression it is starting the activity directly like it does with option 2
- Optimizations in workflow task processing helps existing workflow implementations
- Workflow implementor gets to control how activity is executed and is free to add more "steps" before and after activity execution
### Cons
- Extra explicit workflow in the way
- Depending on the SDK sugar we provide, requires registration of both activity and workflow (complex)
## Option 4 - `client.startSingleActivityWorkflow`
A more explicit version of option 2

# Optimization ideas for single activity workflows
```typescript
import * as workflow from '@temporalio/workflow';

export async function myWorkflow(input) {
  await workflow.withSpeculation(async () => {
		await runSomeActivity(input);
  });
}
```
`withSpeculation` + activity should be as cheap as a local activity as it would start locally but can be upgraded to a normal activity after some time has passed and the SDK decides to commit progress to the server.
With speculation, the activity completion can be reported directly to the workflow, which can continue speculative execution.
If the activity completes before the speculation time, the SDK would generate an `ActivityTaskScheduled,Started,Completed` events (and in this case a `WorkflowExecutionCompleted` event) and commit those on WFT completion.
Otherwise, the SDK commits `ActivityTaskScheduled,Started` and the server allows the activity to complete on the worker up to the configured timeout.
## Client initiated optimizations
The client may choose to start the workflow eagerly, submitting the first workflow task directly to a local worker, saving a roundtrip.
The client may also choose to start the workflow optimistically, letting the worker run the task without acquiring a lock on the server. When the SDK decides to commit the first workflow task, any generated history would be flushed to the server in the `StartWorkflowExecutionRequest`.
## Issues
### Workflow task timeout limitations
Speculation is limited to the workflow task timeout, which defaults to 10 seconds (for good reasons).
While a workflow task is being processed, no new events or messages can be delivered to the workflow. If we wish to run uncommitted workflow tasks for a long time, we would want to remove this restriction.
There may be some hacks that we can use to remove this restriction but a proper solution is a huge change on both the SDK and the server. It may include removing the concept of workflow task events completely, establishing a bidirectional stream between the worker and the server, adding a concept of an inbox, and writing buffered events directly to history.
### gRPC 4MB limit
The more time speculative code is executed the higher the risk that the total accumulated payload size of the SDK generated history would exceed the 4MB gRPC request limit.
We would need to address this limitation for a proper solution.
