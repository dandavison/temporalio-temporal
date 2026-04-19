---
title: "Standalone Activities SDK design - open questions"
notion_url: "https://www.notion.so/2c58fc567738803996b6c43e3332c327"
last_edited: "2025-12-17T17:54:06.021Z"
page_type: design doc
---

---
# Bikeshedding (name choices)
1. ~~`ActivityInfo.in_workflow`~~ ~~calculated property~~
	1. Alternatives: `from_workflow`, `is_workflow_activity`
	2. Should we move it out of `ActivityInfo`?
	3. Final choice: `is_workflow_activity`
# ~~Which fields to expose from `DescribeActivityExecution`~~
1. Exposed fields, always present:
	```java
string activity_id = 1;
string run_id = 2;
temporal.api.common.v1.ActivityType activity_type = 3;
temporal.api.enums.v1.ActivityExecutionStatus status = 4;
string task_queue = 6;
temporal.api.common.v1.RetryPolicy retry_policy = 11;
int32 attempt = 15;
temporal.api.common.v1.Priority priority = 26;
	```
2. Exposed fields, may be null/empty:
	```java
temporal.api.enums.v1.PendingActivityState run_state = 5;
google.protobuf.Duration schedule_to_close_timeout = 7;
google.protobuf.Duration schedule_to_start_timeout = 8;
google.protobuf.Duration start_to_close_timeout = 9;
google.protobuf.Duration heartbeat_timeout = 10;
temporal.api.common.v1.Payloads heartbeat_details = 12;
google.protobuf.Timestamp last_heartbeat_time = 13;
google.protobuf.Timestamp last_started_time = 14;
google.protobuf.Duration execution_duration = 16;
google.protobuf.Timestamp schedule_time = 17;
google.protobuf.Timestamp expiration_time = 18;
google.protobuf.Timestamp close_time = 19;
temporal.api.failure.v1.Failure last_failure = 20;
string last_worker_identity = 21;
google.protobuf.Duration current_retry_interval = 22;
google.protobuf.Timestamp last_attempt_complete_time = 23;
google.protobuf.Timestamp next_attempt_schedule_time = 24;
temporal.api.deployment.v1.WorkerDeploymentVersion last_deployment_version = 25;
temporal.api.common.v1.SearchAttributes search_attributes = 28;
string canceled_reason = 31;

+ string summary (from user_metadata)
	```
3. Omitted fields (need to access raw response to read):
	```java
int64 state_transition_count = 27;
temporal.api.common.v1.Header header = 29;
temporal.api.sdk.v1.UserMetadata user_metadata = 30;
	```
# ~~(Java) Alternative design: make `ActivityClient` independent of `WorkflowClient`~~
Currently, an instance of `ActivityClient` is retrieved by calling `WorkflowClient.newActivityClient`. This means that to call standalone activities, the user must first create `WorkflowClient`. Making the two clients independent would mean the user can create `ActivityClient` directly.
Pros:
- Better separation of concerns.
- More logical (standalone activities don't interact with workflows).
Cons:
- `ActivityClient` shares `WorkflowServiceStubs` and `WorkflowClientOptions` with `WorkflowClient`. Separating these would increase code duplication and add some complexity for users who interact with both workflows and standalone activities.
Decision: Make clients independent.
# ~~(Java) Alternative design for invoking activities~~
1. Use activity stubs for activity invocation.
	Pros:
	- More similar to current design.
	Cons:
	- Can standalone activity stubs be made multi-use like workflow activity stubs are? It would cause too much user friction if the stubs were single-use.
2. ~~Use serializable lambda instead of object proxy.~~
	Pros:
	- Avoids the need to pass `Class<T>` argument.
	Cons:
	- Doesn't work in all JVM environments, especially GraalVM.
Decision: Keep current design.
