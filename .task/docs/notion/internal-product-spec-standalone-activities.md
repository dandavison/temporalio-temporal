---
title: "[INTERNAL] Product Spec: Standalone Activities for Lower Cost single-Activity Workflows"
notion_url: "https://www.notion.so/2f88fc567738817ab94cd59058dd7800"
last_edited: "2026-01-30T22:30:15.174Z"
page_type: spec
---

# Product Spec: Standalone Activities for durable job processing
# Summary
Standalone Activities adds the ability to execute any [Temporal Activity](https://docs.temporal.io/activities) as a top-level Activity Execution for durable job processing with built-in retries, timeouts, and fairness.
The underlying [Activity programming model](https://docs.temporal.io/activities) and Activity Worker deployment is not changing, but now Activities can be invoked standalone (as a top-level Activity Execution) or within a Workflow.
Activity Executions have full visibility and lifecycle control, similar to how a Workflow Execution can be started or viewed from the Temporal UI, CLI, SDK Client, or API.
Standalone Activities are more cost-effective than single-Activity Workflows (1 Action vs. 2 Actions in Temporal Cloud) - see [pricing](about:blank#pricing---cloud-actions) for details.
## Key features
- **Execute any Temporal Activity as a top-level primitive** without the overhead of a Workflow.
- **Native async task processing model**: schedule -> dispatch -> process -> result
- **No head-of-line blocking** - a slow task doesn't block the dispatch of other tasks
- **Arbitrary length tasks** with heartbeats to checkpoint progress and handle worker failures
- **At-least-once execution** by default with native retry policy & timeouts
- **At-most-once execution** if retry max attempts is 1
- **Addressable** - get an activity ID / run id and get the result, (un)pause, reset, cancel, terminate
- **Dedupe** - conflict policy: (USE_EXISTING, …), reuse policy: (REJECT_DUPLICATES, …) with a separate ID space from Workflows
- **Priority and fairness** - multi-tenant fairness, weighted priority tiers (e.g. high/medium/low), and safeguards against starvation of lower-weighted tasks, plus no head-of line blocking
- **Visibility** - list executions and see current status, retry count, last error, …
- **Manual completion** by ID (or token): ignore activity return and wait for external completion
- **Dual use** - execute Activities In-Workflow or Standalone with no Worker code changes
## What is a Temporal Activity?
An Activity is a normal function or method ("Activity Function") that executes a single, well-defined action (either short or long running), such as calling another service, transcoding a media file, or sending an email message.
If an Activity Function execution fails, it is automatically retried using a Retry Policy. Any future execution starts from an initial state (except [Heartbeats](https://docs.temporal.io/encyclopedia/detecting-activity-failures#activity-heartbeat)). Activity code can be non-deterministic. We recommend that it be [idempotent](https://docs.temporal.io/activity-definition#idempotency) or have idempotency detection.
An Activity author creates an [Activity Definition](https://docs.temporal.io/activity-definition) and registers the Activity with a Temporal Worker with an [Activity Type](https://docs.temporal.io/activity-definition#activity-type). Activity Functions are executed by Worker Processes. When the Activity Function returns, the Activity Worker sends the results back to the Temporal Service.
When an Activity caller starts an Activity, an [Activity Execution](https://docs.temporal.io/activity-execution) is created that orchestrates the execution of the Activity. There are 3 types of Activity Executions based on how an Activity is invoked: Workflow, Local, and Standalone.
- **Workflow Activities** are [Activity Execution](https://docs.temporal.io/activity-execution)s orchestrated by a Workflow. Activity results are persisted to the Workflow and Events are added to the Workflow Execution's Event History, for example the [ActivityTaskCompleted](https://docs.temporal.io/references/events#activitytaskcompleted) Event. For other Activity-related Events, see [Activity Events](https://docs.temporal.io/workflow-execution/event#activity-events).
- [**Local Activities**](https://docs.temporal.io/local-activity) are [Activity Execution](https://docs.temporal.io/activity-execution)s that execute in the same process as the [Workflow Execution](https://docs.temporal.io/workflow-execution) that spawns them.
- **NEW: Standalone Activities** are [Activity Execution](https://docs.temporal.io/activity-execution)s that are invoked outside of a Workflow, in a standalone fashion, using the Temporal SDK client. Existing Activity Functions may be invoked as Standalone.
### Isolated ID Space for Standalone Activities
Standalone Activities have a separate ID Space from Workflows and other Temporal primitives. This means use of conflict policy: (USE_EXISTING, …), reuse policy: (REJECT_DUPLICATES, …) will only observe the Standalone Activity ID Space.

## Pricing - Cloud Actions
Standalone Activities are billed the same [Activity actions](https://docs.temporal.io/cloud/actions#activities) as In-Workflow Activity Executions, 1 action to execute an Activity, plus retries and heartbeats.
The existing [storage pricing model](https://docs.temporal.io/cloud/pricing#payg-storage-pricing) (Active vs. Retained) applies to Standalone Activities where a Standalone Activity Execution may be in one of two states: Open (Active Storage) or Closed (Retained Storage). This is similar to how [Workflow Storage works today](https://docs.temporal.io/cloud/pricing#storage), but for a Standalone Activity only the mutable state contributes to storage usage as there is no Workflow event history.
## Retention
The retention period of the Namespace governs how long Closed Standalone Activities are persisted.
# High Level SDK Developer Experience
The Activity authorship and registration experience remains exactly the same as it is today.
## Same authorship experience
[Activity definition](https://docs.temporal.io/develop/typescript/core-application#develop-activities) is the same as today.
```javascript
export async function greet(name: string): Promise<string> {
  return `👋 Hello,${name}!`;
}
```
## Same registration experience
[Activity worker registration](https://docs.temporal.io/develop/typescript/core-application#register-types) is the same as today.
```javascript
import * as activities from './activities';

async function run() {
  const worker = await Worker.create({
    workflowsPath: require.resolve('./workflows'),
    taskQueue: 'snippets',
    activities,
  });

  await worker.run();
}
```
## Start a Standalone Activity using the SDK Client
In addition to using Activities within a Workflow, Activities can now be invoked as Standalone Activities from a Temporal SDK Client with [Namespace Write permission](https://docs.temporal.io/cloud/users#namespace-level-permissions).
Using the Temporal SDK Client:
```javascript
import { greet } from './activities'; // greet is an activity function.

const c = await Client.create({ /* */ })
const handle = await c.activity.start(greet, {
    id: 'my-business-id',
    retry: { /* */ },
    taskQueue: 'foo',
    startToCloseTimeout: '10 minutes',
    conflictPolicy: activity.ConflictPolicy.USE_EXISTING,
    idReusePolicy: activity.IDReusePolicy.REJECT_DUPLICATES,
})
const res = await handle.result();

// Alternatively do both start() and result() in a single statement:
const res = c.activity.execute(greet, { /* ... */ });
```
## Cancel or Terminate a Standalone Activity
Standalone Activities support cancellation and termination, like Workflows.
The caller can cancel or terminate:
```javascript
await handle.cancel();
await handle.terminate();
```
The Activity handler must heartbeat to receive a cancelled notification:
```javascript
import { sleep, CancelledFailure, heartbeat } from '@temporalio/activity';


export async function runForOneDay(): Promise<void> {
  try {
    for (let i = 0; i < 1440; i++) {
      await sleep(60);           // sleep for 60 seconds
      heartbeat(i + 1);          // send heartbeat details (any)
    }
  } catch (err) {
    switch (true) {
      case err instanceof CancelledFailure:
        // handle cancellation
        break;
      default:
        throw err;
    }
  }
}
```
## Pause, reset, and update Standalone Activity options
The ability to pause, reset, update Activity options, and [complete an activity async](https://docs.temporal.io/develop/typescript/asynchronous-activity-completion) by token/ID would be supported, possibly after the initial launch:
```javascript
await handle.pause(); // + unpause.
await handle.reset();
await handle.updateOptions();
```
## List Standalone Activities
```javascript
for (const activity of c.activity.list('ActivityType = "foo"')) {
  // ...
}
```
# CLI Operator Experience
## temporal activity subcommand
The existing `temporal activity` subcommand will be modified to support standalone activities in addition to the in-workflow activities it already supports today.
New Standalone Activity subcommands will also be added:
- `temporal activity start`
- `temporal activity list`
- `temporal activity result`
- `temporal activity describe`
- `temporal activity cancel`
- `temporal activity terminate`
- `temporal activity delete`
- `temporal activity count`
- `temporal activity execute`
- `temporal activity show`
- the commands above are not supported for other types of Activity Executions (Local, In-Workflow)
The following subcommands will be modified to also support Standalone Activities:
- `temporal activity complete`
- `temporal activity fail`
- `temporal activity pause`
- `temporal activity reset`
- `temporal activity unpause`
- `temporal activity update-options` (TBD - may require different options)
## Addressing Activities
### In-Workflow Activities
In-Workflow activities are addressed with --workflow-id and --activity-id. An Activity –run-id may also be specified.
### Standalone Activities
Addressed with –activity_id, but **without** specifying --workflow-id, since they are standalone. An Activity –run-id may also be specified.
## Search Attributes
Search attributes would be the same custom search attributes defined on a Namespace plus system attributes including ActivityID, ActivityType, ActivityStatus (as they will slightly differ from Workflows). Also Workflow system attributes such as WorkflowID and WorkflowType would not be present on Standalone Activities.
## Standalone Activity Execution Status
- Running
- Completed
- Failed
- Paused
- Canceled
- TimedOut
- Terminated
## Pending Activity State
The [same as all Activities](https://github.com/temporalio/api/blob/467d73d95e81860376d70f63382359c9b3b6fbf1/temporal/api/enums/v1/workflow.proto#L82):
- Scheduled
- Started
- Pause Requested
- Paused
- Cancel Requested
- Unspecified
## Attempt Info
- Last Attempt (timestamp)
- Last Error
- Next Attempt (timestamp)
- …
## Commands: Standalone Activity Specific
The following commands are specific to Standalone Activity Executions. See [Addressing Standalone Activities](about:blank#standalone-activities).
Note: Some of these commands should be made compatible with other types of Activity Execution in the future (list, describe, …).
### Start
Start a new Standalone Activity Execution. Returns the Activity- and Run-IDs:
```
temporal activity start \
  --activity-id foo-activity-001 \  --type foo \  --task-queue demo-task-queue \
  --input '{"userId": 42, "items": ["apple", "orange"], "priority": "high"}'
```
### List
List Standalone Activity Executions. The optional --query limits the output to Activities matching a Query:
```
temporal activity list
temporal activity list
   --query "ExecutionStatus = 'Completed'"

temporal activity list \
   --query "ActivityType = 'YourActivity"
```
### Result
Wait for and print the result of a Standalone Activity Execution:
```
temporal activity result \
    --activity-id YourActivityId
```
### Describe
Display information about a specific Standalone Activity Execution:
```
temporal activity describe \
    --activity-id YourActivityId
```
### Cancel
Use the Standalone Activity ID to cancel an Execution:
```
temporal activity cancel \
    --activity-id YourActivityId
```
### Terminate
Terminate a Standalone Activity Execution:
```
temporal activity terminate \
    --activity-id YourActivityId
```
### Delete
Delete Standalone Activity Executions:
```
temporal activity delete \
    --activity-id YourActivityId
```
### Count
Show a count of Standalone Activity Executions:
```
temporal activity count \
    --query YourQuery
```
### Execute
Establish a new Standalone Activity Execution and direct its progress to stdout:
```
temporal activity execute \
  --activity-id foo-activity-001 \
  --type foo \
  --task-queue demo-task-queue \
  --input '{"userId": 42, "items": ["apple", "orange"], "priority": "high"}'
```
## Cloud Metrics
### Standalone Activity metrics
- `temporal_cloud_v1_activity_cancel_count` - Standalone Activities canceled before completing execution
- `temporal_cloud_v1_activity_failed_count` - Standalone Activities that failed before completion
- `temporal_cloud_v1_activity_success_count` - Standalone Activities that successfully completed
- `temporal_cloud_v1_activity_terminate_count` - Standalone Activities terminated before completing execution
- `temporal_cloud_v1_activity_timeout_count` - Standalone Activities that timed out before completing execution
