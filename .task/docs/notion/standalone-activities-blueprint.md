---
title: "Standalone Activities - Blueprint"
notion_url: "https://www.notion.so/21e8fc567738801aa9bbc2c850175a50"
author: "Engineering lead (Maxim Fateev)"
last_edited: "2025-10-28T05:15:51.290Z"
page_type: design doc
---

<page url="https://www.notion.so/21e8fc567738801aa9bbc2c850175a50" icon="🧍">
<ancestor-path>
<parent-data-source url="collection://63f45e15-1644-4c49-80de-a325a7c1a3af" name="Blueprints"/>
</ancestor-path>
<properties>
{"Author(s)": "engineering lead", "Created time": "2025-06-26T20:50:51.334Z", "Last edited time": "2025-10-28T05:15:50.753Z", "Project Brief": "Review", "Project Name": "Standalone Activities - Blueprint"}
</properties>
<content>

# Decision Record
At a meeting on 2025-07-21 with the engineering team, we decided to go with option 1 for starting activities. We still need to make a call whether we expose a specialized `ListActivities` API or a single generalized `ListArchetypes` CHASM API.
Later on, we decided that we will expose a single `ListActivites` API. `ListArchetypes` is off the table.

## Why is it worth solving?
1. Broadening the set of use cases the Temporal platform can support in a cost effective manner. Activities are a lower cost primitive than the current alternative of using a single-activity workflow wrapper and provide further optimization opportunities that are not easy to provide with workflows.
2. Lowering the bar for Temporal adoption. As we've seen in the wild, platform teams at Stripe and Block have seen success building Temporal adoption by providing a "simple task" abstraction.

## Why is it **not** worth solving?
We should always ask ourselves if a completely new set of APIs is worth adding to the platform. It is a significant lift across the organization, including design, UI, docs, education, enablement, engineering, and a long term support burden. The current workaround of single-activity workflow wrapper may be deemed a reasonable solution. Our competitors would probably not build something like standalone activities since they do not require activity registration. They have an alternative concept of `ctx.step` for side-effects in workflow context where a function could be called inline. For them a standalone activity would look like:
```typescript
export async function myWorkflow() {
  return await workflow.step("do stuff", async () => {
    // TADA! standalone activity...
  });
}
```
This model is easy to extend for when more steps need to be added.

## What is in scope?

```typescript
// High level SDK caller side experience

import { bar } from './activities'; // bar is an activity function.

const c = await Client.create({ /* */ })
const handle = await c.activity.start(bar, {
	id: 'my-business-id',
	retry: { /* */ },
	taskQueue: 'foo',
	startToCloseTimeout: '10 minutes',
	conflictPolicy: activity.ConflictPolicy.USE_EXISTING,
	idReusePolicy: activity.IDReusePolicy.ALLOW_DUPLICATES,
})
await handle.cancel();
await handle.terminate();
await handle.describe(); // With a long poll option.

// Optionally these will be available in SDKs:
await handle.pause(); // + unpause.
await handle.reset();
await handle.updateOptions();
// END Optional block

// Async completion APIs exposure TBD.
await handle.fail();
await handle.complete();
await handle.resolveAsCanceled();
// END async APIs

const res = await handle.result();

// Alternatively:
const res = c.activity.execute(bar, { /* ... */ });

for (const activity of c.activity.list('ActivityType = "foo"')) {
  // ...
}
```

The overall scope of this project is fairly large, the basic functionality would be to expose SDK APIs to start activities from outside of a workflow, but the value of standalone activities is realized when integrated with other platform features, such as Nexus Operations, Scheduled actions, user-defined batch jobs, "detached" activities started from a workflow, invocation and interaction through the CLI and UI.

Work items for MLP pre-release:
- [MLP pre-release] API definitions (both new and updates to existing)
- [MLP pre-release] SDK design and implementation
- [MLP pre-release] CHASM archetype server side implementation
- [MLP pre-release] UI design and implementation - scope roughly equal to "schedules" UI
- [MLP pre-release] CLI support
- [MLP pre-release] Concept docs
- [MLP pre-release] Per-SDK how-to docs
- [MLP pre-release] SDK samples
- [MLP pre-release] Integrate with manual completion methods
- [MLP GA] Pause, reset, update options.

Post MLP items:
- [Post MLP] Model the APIs with Nexus targeting a "system" endpoint
- [Post MLP] Use an IDL to generate the service definitions
- [Post MLP] Model the entire execution of the activity as a multi-stage operation
- [Post MLP] Back user-defined Nexus operations with standalone `ActivityRunOperation`.
- [Post MLP] Activity as a scheduled action.
- [Post MLP] Batch operation on activities.
- [Post MLP] Integration with workflow / namespace rules.
- [Post MLP] Eager start
- [Post MLP] Optimistic start
- [Post MLP] Power workflow activities with the CHASM component
- [Post MLP] Integration with versioning / pinning.
- [Post MLP] Visibility for workflow activities.

## What is **not** in scope?
Extending the activity primitive's capabilities including decoupling liveness from snapshots and activity interactions.

## Success Criteria
The feature is adopted by key customers and becomes a significant (definition TBD) revenue source.

# Design

## Visibility and ID space
Standalone activities will live in their own ID space to avoid colliding with workflows. E.g. it will be possible for a running workflow and a running activity to share the same ID.

The reason for not sharing the ID space is to avoid activities showing up in the workflow list and vice versa and avoid confusing "workflow execution already started" error messages to users starting an activity.

An activity is globally identified by the user provided business ID, which we'll call activity ID. There is also a system-generated run ID, similar to workflow run IDs, where the activity ID can be reused after completion.

We will add separate strongly typed `ListActivityExecutions` API, activities will be filtered internally by specifying `TemporalNamespaceDivision = "activity"`.

System/pre-defined search attributes that will be available for standalone activities:
```plain text
ActivityId
RunId
ActivityType
TaskQueue
StartTime
ExecutionTime
CloseTime
ExecutionStatus
ExecutionDuration
StateTransitionCount
PauseInfo -- when we add pause support
```

### API definitions

We will add a new set of Nexus `ActivityService` API definitions in the `temporalio/api` repo. The definitions may either be proto based or JSON schema based.

Initially the methods on the service would include:
1. `StartActivityExecution`
2. `GetActivityExecutionResult`
3. `RequestCancelActivityExecution`
4. `TerminateActivityExecution`
5. `ListActivityExecutions`
6. `DescribeActivityExecution`

Other methods that are required eventually but may not be included in the MLP:
1. `ExecuteActivity` async Nexus Operation

### Activity Start Request

```protobuf
message StartActivityExecutionRequest {
    string namespace = 1;
    string identity = 2;
    string request_id = 3;

    string activity_id = 4;
    temporal.api.common.v1.ActivityType activity_type = 5;
    temporal.api.activity.v1.ActivityOptions options = 6;
    temporal.api.common.v1.Payloads input = 7;

    temporal.api.enums.v1.IdReusePolicy id_reuse_policy = 8;
    temporal.api.enums.v1.IdConflictPolicy id_conflict_policy = 9;

    temporal.api.common.v1.Memo memo = 10;
    temporal.api.common.v1.SearchAttributes search_attributes = 11;
    temporal.api.common.v1.Header header = 12;
    bool request_eager_execution = 13;
    repeated temporal.api.common.v1.Callback completion_callbacks = 14;
    temporal.api.sdk.v1.UserMetadata user_metadata = 15;
    repeated temporal.api.common.v1.Link links = 16;
    temporal.api.activity.v1.OnConflictOptions on_conflict_options = 17;
    temporal.api.common.v1.Priority priority = 18;
}

message StartActivityExecutionResponse {
    string run_id = 1;
    bool started = 2;
    PollActivityTaskQueueResponse eager_task = 3;
    temporal.api.common.v1.Link link = 4;
}
```

### Activity Execution Info

```protobuf
enum ActivityExecutionStatus {
    ACTIVITY_EXECUTION_STATUS_UNSPECIFIED = 0;
    ACTIVITY_EXECUTION_STATUS_RUNNING = 1;
    ACTIVITY_EXECUTION_STATUS_COMPLETED = 2;
    ACTIVITY_EXECUTION_STATUS_FAILED = 3;
    ACTIVITY_EXECUTION_STATUS_CANCELED = 4;
    ACTIVITY_EXECUTION_STATUS_TERMINATED = 5;
    ACTIVITY_EXECUTION_STATUS_TIMED_OUT = 6;
}

message ActivityExecution {
    string activity_id = 1;
    string run_id = 2;
}

message ActivityExecutionInfo {
    ActivityExecution activity_execution = 1;
    temporal.api.common.v1.ActivityType activity_type = 2;
    temporal.api.enums.v1.ActivityExecutionStatus status = 3;
    temporal.api.enums.v1.PendingActivityState run_state = 4;
    temporal.api.common.v1.Payloads heartbeat_details = 5;
    google.protobuf.Timestamp last_heartbeat_time = 6;
    google.protobuf.Timestamp last_started_time = 7;
    int32 attempt = 8;
    int32 maximum_attempts = 9;
    google.protobuf.Timestamp scheduled_time = 10;
    google.protobuf.Timestamp expiration_time = 11;
    temporal.api.failure.v1.Failure last_failure = 12;
    string last_worker_identity = 13;
    google.protobuf.Duration current_retry_interval = 16;
    google.protobuf.Timestamp last_attempt_complete_time = 17;
    google.protobuf.Timestamp next_attempt_schedule_time = 18;
    bool paused = 19;
    temporal.api.deployment.v1.WorkerDeploymentVersion last_deployment_version = 20;
    temporal.api.common.v1.Priority priority = 21;
    PauseInfo pause_info = 22;
    temporal.api.activity.v1.ActivityOptions activity_options = 23;
    temporal.api.common.v1.Payloads input = 24;
    int64 state_transition_count = 25;
    temporal.api.common.v1.SearchAttributes search_attributes = 26;
    temporal.api.common.v1.Header header = 27;
    bool eager_execution_requested = 28;
    repeated temporal.api.common.v1.Callback completion_callbacks = 29;
    temporal.api.sdk.v1.UserMetadata user_metadata = 30;
    repeated temporal.api.common.v1.Link links = 31;
}
```

## Activity Execution List Info

```protobuf
message ActivityListInfo {
    ActivityExecution activity_execution = 1;
    ActivityType activity_type = 2;
    google.protobuf.Timestamp scheduled_time = 3;
    google.protobuf.Timestamp close_time = 4;
    temporal.api.enums.v1.ActivityExecutionStatus status = 5;
    temporal.api.common.v1.Memo memo = 6;
    temporal.api.common.v1.SearchAttributes search_attributes = 7;
    string task_queue = 8;
    int64 state_transition_count = 9;
    int64 state_size_bytes = 10;
    google.protobuf.Duration execution_duration = 11;
}
```

### CHASM archetype server side implementation
This is where the core logic lives, we will **copy** the current activity code to fit into a CHASM standalone component. The new code should be reusable for standalone activities, in-workflow activity, and potentially future state machines, such as user-defined batch jobs.

These state machines will not write inputs and outputs to a separate history table and instead embed payloads directly into the state machine's mutable state.

### CLI support
`temporal activity` should operate on both standalone and in-workflow activities.

New `temporal activity` sub-commands:
- start
- list
- count
- describe
- result
- cancel
- terminate
- delete
- execute

Modified sub-commands to support standalone activities:
- complete
- fail
- pause
- reset
- unpause
- update-options

We differentiate between a standalone and in-workflow activity by checking the ID provided. In-workflow activities must be addressed with a `--workflow-id`, and standalone activities with an `--activity-id`.

# Production readiness

## Processes for rollout / rollback
This feature will be gated behind a dynamic config.

Once this feature is rolled out and there are open activity executions, rolling back would not be possible.

Server support will be rolled out to all cells as any other release but the feature will be turned off by default.

We will allow the canary account to create standalone activities and exercise the feature in all cells before letting customers use the feature.

## SLAs and SLOs
Same as any other API, we will need to especially watch the `StartActivityExecution` APIs as it becomes as critical as `StartWorkflowExecution`.

## Scalability
We are mostly concerned with WAL write throughput and payload sizes.
</content>
</page>
