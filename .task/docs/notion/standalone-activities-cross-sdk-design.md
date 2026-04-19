---
title: "Standalone Activities Cross-SDK design (draft)"
notion_url: "https://www.notion.so/29a8fc56773880b9beaaf8c69a29842c"
page_type: design doc
---

<page url="https://www.notion.so/29a8fc56773880b9beaaf8c69a29842c">
<ancestor-path>
<parent-page url="https://www.notion.so/1f58fc567738800c8401dc2b5c76885d" title="Standalone Activities"/>
<ancestor-2-page url="https://www.notion.so/78d52aade69b480f962b95a3923f56dd" title="Engineering"/>
</ancestor-path>
<properties>
{"title":"Standalone Activities Cross-SDK design (draft)"}
</properties>
<content>
<table_of_contents color="gray"/>
# Open questions
<page url="https://www.notion.so/2c58fc567738803996b6c43e3332c327">Standalone Activities SDK design - open questions</page>
# Approval list
*Everyone feel free to add your names here.*
.NET (owners: <mention-user url="user://a3cc433e-3aef-47bd-b14a-e48edfcf068c"/>, <mention-user url="user://1dcd872b-594c-81dc-9380-000208f61694"/>)
	- Approved by:
	- Blocked by:
Go (owners: <mention-user url="user://67dfd166-264c-4314-99b6-ede5249851c8"/>, <mention-user url="user://81453f38-d1ed-4d3b-9816-4bea792be095"/>)
	- Approved by: Andrew (defer to Quinn for final approve)
	- Blocked by:
Java (owners: <mention-user url="user://67dfd166-264c-4314-99b6-ede5249851c8"/>, <mention-user url="user://1dcd872b-594c-81dc-9380-000208f61694"/>)
	- Approved by:
	- Blocked by:
Ruby (owners: <mention-user url="user://a3cc433e-3aef-47bd-b14a-e48edfcf068c"/>, <mention-user url="user://276d872b-594c-81d7-a777-000297199272"/>)
	- Approved by:
	- Blocked by:
Python (owners: <mention-user url="user://1ead872b-594c-814f-974e-0002ab8a6713"/>, <mention-user url="user://276d872b-594c-8180-bd54-000260e29608"/>, <mention-user url="user://138d872b-594c-81f4-a6b1-0002cf91fb43"/>)
	- Approved by:
	- Blocked by:
TypeScript (owners: <mention-user url="user://518adca2-9a48-4e1e-88ae-de8c559e168b"/>, <mention-user url="user://138d872b-594c-81f4-a6b1-0002cf91fb43"/>, <mention-user url="user://276d872b-594c-81d7-a777-000297199272"/>)
	- Approved by:
	- Blocked by:
# High-level summary of changes
1. New client calls:
	1. Start activity (async)
	2. Execute activity (wait for result)
	3. Get activity execution result
	4. <span discussion-urls="discussion://29b8fc56-7738-8049-96b7-001c1ee110b4">Get activity execution</span> info
	5. List activity executions (no interceptor for now)
	6. Count activity executions
	7. <span discussion-urls="discussion://29b8fc56-7738-80fb-89a9-001ceb0955eb">Cancel activity execution</span>
	8. Terminate activity
2. Activity info changes:
	1. **BREAKING CHANGE:** `workflow_id`, `workflow_run_id`, `workflow_type`  and `workflow_namespace` fields become nullable.
		1. Workflow activities: always set
		2. Standalone activities: always null
		3. Exact implementation varies by language.
	2. `workflow_namespace` is deprecated in favor of new field `namespace`.
		1. Both fields always have the same value, regardless of whether activity is in workflow or standalone. This behavior will be stated in documentation for `workflow_namespace` field.
	3. New field: <span discussion-urls="discussion://2af8fc56-7738-80e1-b277-001c99f88ebf">`activity_run_id`</span> (optional string).
		1. Workflow activities: always null
		2. Standalone activities: always set
	4. <span discussion-urls="discussion://2c38fc56-7738-8032-80be-001ca80bc2ae">New field: </span>`is_workflow_activity`<span discussion-urls="discussion://2c38fc56-7738-8032-80be-001ca80bc2ae"> (boolean).</span>
		1. Calculated property where possible - details vary by language.
3. Async activities:
	1. Standalone async activities should be addressable by activity ID + optional activity run ID pair. The workflow run ID parameter should not be reused for activity run ID.
4. Serialization context:
	1. Workflow ID becomes optional in activity serialization context, and in any supertypes of activity serialization context.
# Core SDK
## 1. Changes to SDK Proto `coresdk.activity_task.Start`  {toggle="true"}
	1. `workflow_namespace` is renamed to `namespace`
	2. `workflow_type` and `workflow_execution` are empty if activity is standalone.
	3. New field: `string activity_run_id = 18;`  - empty if activity is in workflow.
# .NET
## 1. New methods in `Client.ITemporalClient`  {toggle="true"}
	A. `StartActivityAsync` and `ExecuteActivityAsync` 
	Both methods have multiple overloads for different ways to pass the activity name and arguments, same as `Workflows.Workflow.ExecuteActivityAsync`. One overload takes activity name as a string and arguments as a list or arbitrary objects. The other overloads take a lambda expression describing the activity invocation with arguments in place.
	`StartActivityAsync` can be called with or without generic argument, returning generic `ActivityHandle<TResult>` or non-generic `ActivityHandle`.
	`ExecuteActivityAsync` can be called with or without generic argument. Generic variant returns the activity result deserialized to given type. Non-generic variant waits for completion but doesn’t return result.
	<details>
	<summary>Method signatures</summary>
		```typescript
public Task<ActivityHandle<TResult>> StartActivityAsync<TResult>(
    Expression<Func<TResult>> activityCall,
    ActivityOptions options)

public Task<ActivityHandle> StartActivityAsync(
    Expression<Action> activityCall,
    ActivityOptions options)

public Task<ActivityHandle<TResult>> StartActivityAsync<TActivityInstance, TResult>(
    Expression<Func<TActivityInstance, TResult>> activityCall,
    ActivityOptions options)

public Task<ActivityHandle> StartActivityAsync<TActivityInstance>(
    Expression<Action<TActivityInstance>> activityCall,
    ActivityOptions options)

public Task<ActivityHandle<TResult>> StartActivityAsync<TResult>(
    Expression<Func<Task<TResult>>> activityCall,
    ActivityOptions options)

public Task<ActivityHandle> StartActivityAsync(
    Expression<Func<Task>> activityCall,
    ActivityOptions options)

public Task<ActivityHandle<TResult>> StartActivityAsync<TActivityInstance, TResult>(
    Expression<Func<TActivityInstance, Task<TResult>>> activityCall,
    ActivityOptions options)

public Task<ActivityHandle<TResult>> StartActivityAsync<TActivityInstance>(
    Expression<Func<TActivityInstance, Task>> activityCall,
    ActivityOptions options)
    
public Task<ActivityHandle<TResult>> StartActivityAsync<TResult>(
    string activity,
    IReadOnlyCollection<object?> args
    ActivityOptions options)
    
public Task<ActivityHandle> StartActivityAsync(
    string activity,
    IReadOnlyCollection<object?> args
    ActivityOptions options)

public Task<TResult> ExecuteActivityAsync<TResult>(
    Expression<Func<TResult>> activityCall,
    ActivityOptions options)

public Task ExecuteActivityAsync(
    Expression<Action> activityCall,
    ActivityOptions options)

public Task<TResult> ExecuteActivityAsync<TActivityInstance, TResult>(
    Expression<Func<TActivityInstance, TResult>> activityCall,
    ActivityOptions options)

public Task ExecuteActivityAsync<TActivityInstance>(
    Expression<Action<TActivityInstance>> activityCall,
    ActivityOptions options)

public Task<TResult> ExecuteActivityAsync<TResult>(
    Expression<Func<Task<TResult>>> activityCall,
    ActivityOptions options)

public Task ExecuteActivityAsync(
    Expression<Func<Task>> activityCall,
    ActivityOptions options)

public Task<TResult> ExecuteActivityAsync<TActivityInstance, TResult>(
    Expression<Func<TActivityInstance, Task<TResult>>> activityCall,
    ActivityOptions options)

public Task<TResult> ExecuteActivityAsync<TActivityInstance>(
    Expression<Func<TActivityInstance, Task>> activityCall,
    ActivityOptions options)
    
public Task<TResult> ExecuteActivityAsync<TResult>(
    string activity,
    IReadOnlyCollection<object?> args
    ActivityOptions options)
    
public Task ExecuteActivityAsync(
    string activity,
    IReadOnlyCollection<object?> args
    ActivityOptions options)
		```
	</details>
	B. Other methods
	```c#
public ActivityHandle GetActivityHandle(
		string activityId, string? activityRunId);
		
public ActivityHandle<TResult> GetActivityHandle<TResult>(
		string activityId, string? activityRunId);

#if NETCOREAPP3_0_OR_GREATER
public IAsyncEnumerable<ActivityExecution> ListActivitiesAsync(
		string query, ActivityListOptions? options = null);
#endif

public Task<ActivityExecutionCount> CountActivitiesAsync(
		string query, ActivityListOptions? options = null);
	```
## 2. New types {toggle="true"}
	```c#
namespace Temporalio.Client {
		public record ActivityHandle(
		    ITemporalClient client,
		    string activityId,
		    string? activityRunId)
    {
		    public async Task GetResultAsync(
				    ActivityGetResultOptions? options = null);
				    
		    public async Task<TResult> GetResultAsync<TResult>(
				    ActivityGetResultOptions? options = null);
				    
		    public async Task<ActivityExecutionDescription> DescribeAsync(
				    ActivityDescribeOptions? options = null);
				    
		    public async Task CancelAsync(
				    string? reason = null,
				    ActivityCancelOptions? options = null);
				    
		    public async Task TerminateAsync(
				    string? reason = null,
				    ActivityTerminateOptions? options = null);
		}
		
		public class ActivityHandle<TResult> : ActivityHandle
		{
				public new async Task<TResult> GetResultAsync(
				    ActivityGetResultOptions? options = null);
		}
		
		public class ActivityOptions : ICloneable
		{
				public ActivityOptions();
				public ActivityOptions(string id, string taskQueue);
		
				public string? Id { get; set; } // required
		    public string? TaskQueue { get; set; } // required
		    
		    public TimeSpan? ScheduleToCloseTimeout { get; set; }
				public TimeSpan? ScheduleToStartTimeout { get; set; }
		    public TimeSpan? StartToCloseTimeout { get; set; }
		    public TimeSpan? HeartbeatTimeout { get; set; }
		    public RetryPolicy? RetryPolicy { get; set; }
		    public string? Summary { get; set; }
			  public Priority? Priority { get; set; }
		    public SearchAttributeCollection? SearchAttributes { get; set; }
			  public ActivityIdReusePolicy IdReusePolicy { get; set; } =
				    ActivityIdReusePolicy.AllowDuplicate; // imported from proto
		    public ActivityIdConflictPolicy IdConflictPolicy { get; set; } =
				    ActivityIdConflictPolicy.Fail; // imported from proto
        
        public RpcOptions? Rpc { get; set; }
		
		    public virtual object Clone();
		}
		
		public class ActivityGetResultOptions : ICloneable
		{
				public RpcOptions? Rpc { get; set; }
				public virtual object Clone();
		}
		
		public class ActivityDescribeOptions : ICloneable
		{
				public RpcOptions? Rpc { get; set; }
				public virtual object Clone();
		}
		
		public class ActivityCancelOptions : ICloneable
		{
				public RpcOptions? Rpc { get; set; }
				public virtual object Clone();
		}
		
		public class ActivityTerminateOptions : ICloneable
		{
				public RpcOptions? Rpc { get; set; }
				public virtual object Clone();
		}
		
		public class ActivityListOptions : ICloneable
		{
				public RpcOptions? Rpc { get; set; }
				public virtual object Clone();
		}
		
		public class ActivityCountOptions : ICloneable
		{
				public RpcOptions? Rpc { get; set; }
				public virtual object Clone();
		}
		
		// Not a record so that there's a way to seamlessly add
		// lazy deserialization of future fields, e.g. memo.
		public class ActivityExecution
		{
				protected internal ActivityExecution();
	
				public Api.v1.ActivityExecutionListInfo? RawListInfo { get; init; }
				public string ActivityId { get; init; }
				public string ActivityRunId { get; init; }
				public string ActivityType { get; init; }
				public DateTime? ScheduleTime { get; init; }
				public DateTime? CloseTime { get; init; }
				public ActivityExecutionStatus Status { get; init; }
				public SearchAttributeCollection SearchAttributes { get; init; }
				public string TaskQueue { get; init; }
				public TimeSpan? ExecutionDuration { get; init; }
		}
		
		public class ActivityExecutionDescription {
				protected internal ActivityExecutionDescription(
						DataConverter dataConverter);

				public Api.v1.ActivityExecutionInfo? RawInfo { get; init; }
				
				public PendingActivityState RunState { get; init; }
		    public TimeSpan? ScheduleToCloseTimeout { get; init; }
				public TimeSpan? ScheduleToStartTimeout { get; init; }
		    public TimeSpan? StartToCloseTimeout { get; init; }
		    public TimeSpan? HeartbeatTimeout { get; init; }
				public bool HasHeartbeatDetails { get; init; }
				public RetryPolicy RetryPolicy { get; init; }
				public DateTime? LastHeartbeatTime { get; init; }
				public DateTime? LastStartedTime { get; init; }
				public int Attempt { get; init; }
				public DateTime? ExpirationTime { get; init; }
				public Lazy<Failure>? LastFailure { get; init; }
				public string? LastWorkerIdentity { get; init; }
				public DateTime? CurrentRetryInterval { get; init; }
				public DateTime? LastAttemptCompleteTime { get; init; }
				public DateTime? NextAttemptScheduleTime { get; init; }
				public WorkerDeploymentVersion? LastDeploymentVersion { get; init; }
				public Priority Priority { get; init; }
				public string? CanceledReason { get; init; }
				public Lazy<string>? Summary { get; init; }
				
				public List<T> GetHeartbeatDetails<T>();
				public List<object?> GetHeartbeatDetails(Type type);
		}
		
		public record ActivityExecutionCount(
				long Count,
				IReadOnlyCollection<ActivityExecutionCount.AggregationGroup> Groups)
    {
        public record AggregationGroup(
		        long Count,
		        IReadOnlyCollection<object> GroupValues);
    }
}

namespace Temporalio.Client.Interceptors
{
    public record StartActivityInput(
        string Activity,
        IReadOnlyCollection<object?> Args,
        ActivityOptions Options,
        IDictionary<string, Payload>? Headers);
  
    public record StartActivityOutput(
        string RunId);
        
    public record GetActivityResultInput(
        string ActivityId,
        string? ActivityRunId,
        ActivityGetResultOptions Options);
        
    public record DescribeActivityInput(
        string ActivityId,
        string? ActivityRunId,
        ActivityDescribeOptions Options);
        
    public record CancelActivityInput(
        string ActivityId,
        string? ActivityRunId,
        string? Reason,
        ActivityCancelOptions Options);
        
    public record TerminateActivityInput(
        string ActivityId,
        string? ActivityRunId,
        string? Reason,
        ActivityTerminateOptions Options);
        
    public record ListActivitiesInput(
        string query,
        ActivityListOptions Options);
        
    public record CountActivitiesInput(
        string query,
        ActivityCountOptions Options);
}
	```
## 3. Changes to `Converters.ISerializationContext`  {toggle="true"}
	1. `IHasWorkflow`:
		- Field `WorkflowId` becomes nullable.
	2. `Activity`:
		- Fields `WorkflowId` and `WorkflowType` becomes nullable.
		- New field `string ActivityId`.
## 4. Other changes to existing types {toggle="true"}
	A. `Activities.ActivityInfo` 
	```c#
// new record fields

string? ActivityRunId, // null if in workflow
string Namespace,


// changed record fields

string? WorkflowId, // null if standalone
string? WorkflowNamespace, // deprecated, null if standalone
string? WorkflowRunId, // null if standalone
string? WorkflowType, // null if standalone


// new calculated property

bool IsInWorkflow => WorkflowId is not null;
	```
	B. `Client.AsyncActivityHandle.IdReference` 
	```c#
public record IdReference(
    string? WorkflowId, // null if standalone
    string? RunId, // either workflow run ID or activity run ID or null
    string ActivityId) : Reference;
	```
	C. `Client.Interceptors.ClientOutboundInterceptor`
	New methods:
	```c#
public virtual Task<StartActivityOutput> StartActivityAsync(
		StartActivityInput input);
		
public virtual Task<Payload> GetActivityResultAsync<TResult>(
		GetActivityResultInput input);

public virtual Task<ActivityExecutionDescription> DescribeActivityAsync(
		DescribeActivityInput input);
		
public virtual Task CancelActivityAsync(CancelActivityInput input);

public virtual Task TerminateActivityAsync(TerminateActivityInput input);

public virtual Task<ActivityExecutionCount> CountActivitiesAsync(
		CountActivitiesInput input);
	```
# Go
## 1. New types in `client` module {toggle="true"}
	```go
import activitypb "go.temporal.io/api/activity/v1"

type (
		ActivityHandle interface {
		    ActivityID() string
		    ActivityRunID() string // can be empty
		    Get(ctx context.Context, valuePtr any) error
		    
		    Describe(
		        ctx     context.Context,
		        options DescribeActivityOptions
        ) (ActivityExecutionDescription, error)
        
        Cancel(
						ctx     context.Context,
						options CancelActivityOptions
				) error
				
				Terminate(
						ctx     context.Context,
						options TerminateActivityOptions
				) error
		}
	
		StartActivityOptions struct {
		    ID                     string
		    TaskQueue              string
		    ScheduleToCloseTimeout time.Duration
		    ScheduleToStartTimeout time.Duration
		    StartToCloseTimeout    time.Duration
		    HeartbeatTimeout       time.Duration
		    IDConflictPolicy       enumspb.ActivityIdConflictPolicy
		    IDReusePolicy          enumspb.ActivityIdReusePolicy
		    RetryPolicy            *RetryPolicy
		    SearchAttributes       SearchAttributes
		    Summary                string
		    Priority               Priority
		}
		
		DescribeActivityOptions struct {} // for future compatibility
		
		CancelActivityOptions struct {
		    Reason string
		}
		
		TerminateActivityOptions struct {
		    Reason string
		}
		
		ActivityExecutionMetadata struct {
				// nil if part of ActivityExecutionDescription
		    RawExecutionListInfo    *activitypb.ActivityExecutionListInfo
				ActivityID              string
				ActivityRunID           string
				ActivityType            string
				ScheduledTime           time.Duration
				CloseTime               time.Duration
				Status                  enumspb.ActivityExecutionStatus
				SearchAttributes        SearchAttributes
				TaskQueue               string
				ExecutionDuration       time.Duration
		}
	
		ActivityExecutionDescription struct {
				ActivityExecutionMetadata
				RawExecutionInfo        *activitypb.ActivityExecutionInfo
				RunState                enumspb.PendingActivityState
				LastHeartbeatTime       time.Duration
				LastStartedTime         time.Duration
				Attempt                 int
				RetryPolicy             *RetryPolicy
				ExpirationTime          time.Duration
				LastWorkerIdentity      string
				CurrentRetryInterval    time.Duration
				LastAttemptCompleteTime time.Duration
				NextAttemptScheduleTime time.Duration
				LastDeploymentVersion   WorkerDeploymentVersion
				Priority                Priority
				EagerExecutionRequested bool
				CanceledReason          string
				dc                      converter.DataConverter
		}
)

func (a *ActivityExecutionDescription) HasHeartbeatDetails() bool
// valuePtr must be a pointer to an array
func (a *ActivityExecutionDescription) HeartbeatDetails(valuePtr any) error

func (a *ActivityExecutionDescription) LastFailure() error
func (a *ActivityExecutionDescription) Summary() (string, error)

type (		
		ListActivitiesOptions struct {
		    Query string
		}
		
		CountActivitiesOptions struct {
		    Query string
		}
		
		CountActivitiesResult struct {
		    Count  int64
		    Groups []ActivityAggregationGroup
		}
		
		ActivityAggregationGroup struct {
		    GroupValues []any
		    Count       int64
		}
)
	```
## 2. Changes to `client.Client` interface  {toggle="true"}
	```go
// New methods

ExecuteActivity(
    ctx      context.Context,
    options  StartActivityOptions,
    activity any,
    args     ...any
) (ActivityHandle, error)

GetActivityHandle(
    activityID    string,
    activityRunID string // can be empty
) (ActivityHandle, error)

ListActivities(
		ctx     context.Context,
		options ListActivitiesOptions
) iter.Seq2[*ActivityExecutionMetadata, error]

CountActivities(
		ctx context.Context,
		options CountActivitiesOptions
) (*CountActivitiesResult, error)



// Semantic-only changes

// workflowID can be empty. If it's empty, then activityID refers to
// non-workflow activity, and runID refers to activity run ID.
CompleteActivityByID(
    ctx context.Context,
    namespace  string,
    workflowID string,
    runID      string,
    activityID string,
    result     interface{},
    err        error
) error

// workflowID can be empty. If it's empty, then activityID refers to
// non-workflow activity, and runID refers to activity run ID.
RecordActivityHeartbeatByID(
    ctx        context.Context,
    namespace  string,
    workflowID string, 
    runID      string,
    activityID string,
    details    ...interface{}
) error
	```
## 3. Changes to `activity.Info`  type {toggle="true"}
	```go
// New fields

ActivityRunID string // empty if in workflow
Namespace     string

// New method

func (i *Info) IsWorkflowActivity() bool // true if i.WorkflowExecution.ID == ""

// Deprecated fields

WorkflowNamespace string // empty if standalone

// Semantic-only changes

WorkflowExecution WorkflowExecution // both ID and RunID empty if standalone
WorkflowType      *WorkflowType // nil if standalone
	```
## 4. Changes to `testsuite` module {toggle="true"}
	```go
// New method:
// SetRunActivitiesInWorkflow sets how activities are run in test environment.
// If set to true, activities are run inside a fake workflow.
// If set to false, activities are run without a workflow.
// Defaults to true.
func (t *TestActivityEnvironment) SetRunActivitiesInWorkflow(
		runActivitiesInWorkflow bool
) t *TestActivityEnvironment

// Documentation change: panics if SetRunActivitiesInWorkflow is set to false
func (t *TestActivityEnvironment) ExecuteLocalActivity(
		activityFn interface{}, args ...interface{}
) (converter.EncodedValue, error)
	```
## 5. Changes to `interceptor` module {toggle="true"}
	A. New methods in `ClientOutboundInterceptor` interface
	```go
ExecuteActivity(
		context.Context, *ClientExecuteActivityInput
) (ActivityHandle, error)

GetActivityResult(
		context.Context, *ClientGetActivityResultInput
) error

DescribeActivity(
		context.Context, *ClientDescribeActivityInput
) (ActivityExecutionDescription, error)

CancelActivity(
		context.Context, *ClientCancelActivityInput
) error

TerminateActivity(
		context.Context, *ClientTerminateActivityInput
) error

CountActivities(
		context.Context, *ClientCountActivitiesInput
) (CountActivitiesResult, error)
	```
	B. New types
	```go
type (
		ClientExecuteActivityInput struct {
		    Options      *StartActivityOptions
		    ActivityType string
		    Args         []any
		}
		
		ClientGetActivityResultInput struct {
		    ActivityID    string
		    ActivityRunID string
		    valuePtr      any
		}
		
		ClientDescribeActivityInput struct {
		    ActivityID    string
		    ActivityRunID string
		    Options       *DescribeActivityOptions
		}
		
		ClientCancelActivityInput struct {
		    ActivityID    string
		    ActivityRunID string
				Options       *CancelActivityOptions
		}
		
		ClientTerminateActivityInput struct {
		    ActivityID    string
		    ActivityRunID string
				Options       *TerminateActivityOptions
		}
		
		ClientCountActivitiesInput struct {
		    ActivityID    string
		    ActivityRunID string
				Options       *CountActivitiesOptions
		}
)
	```
# Java
## 1. New types {toggle="true"}
	<details>
	<summary>A. `io.temporal.client`</summary>
		```java
/*
Example use:

@ActivityInterface
interface MyActivity {
  @ActivityMethod
  String activity(int a, int b);
}

ActivityClient client = ...;
ActivityOptions options = ...;

// sync execution
String result = client.execute(
	MyActivity.class, MyActivity::activity, options, 1, 2);

// async execution with handle
ActivityHandle<String> handle = client.start(
	MyActivity.class, MyActivity::activity, options, 1, 2);
String result = handle.getResult();

// async execution with future
CompletableFuture<String> resultFut = client.executeAsync(
	MyActivity.class, MyActivity::activity, options, 1, 2);
String result = resultFut.get();

// sync execution by string
String result = client.newActivityClient().execute(
	"MyActivity.activity", String.class, options, 1, 2);
	
// get result through typed handle
ActivityHandle<String> handle = client.getHandle(
	"activityId", null, String.class);
String result = handle.getResult();

// get result through untyped handle
UntypedActivityHandle handle = client.getHandle(
	"activityId", null);
String result = handle.getResult(String.class);

*/			
interface ActivityClient {
	public static ActivityClient newInstance();
	public static ActivityClient newInstance(
		ActivityClientOptions options);

	ActivityCompletionClient newActivityCompletionClient();
	Stream<ActivityExecutionMetadata> listExecutions(String query);	
	ActivityExecutionCount countExecutions(String query);
		```
		<details>
		<summary>  `// getHandle, start, execute and executeAsync`</summary>
			```java
	/// Obtains untyped handle to existing activity execution.
	UntypedActivityHandle getHandle(
			String activityId,
			@Nullable String activityRunId);
	
	/// Obtains typed handle to existing activity execution.
	<R> ActivityHandle<R> getHandle(
			String activityId,
			@Nullable String activityRunId,
			Class<R> resultClass);
	
	/// Obtains typed handle to existing activity execution.
	/// For use with generic return types.
	<R> ActivityHandle<R> getHandle(
			String activityId,
			@Nullable String activityRunId,
			Class<R> resultClass,
			@Nullable Type resultType);
	
	/// Asynchronously starts activity.
	UntypedActivityHandle start(
			String activity,
			ActivityOptions options,
			@Nullable Object... args);
	
	<R> ActivityHandle<R> start(
			String activity,
			Class<R> resultClass,
			ActivityOptions options,
			@Nullable Object... args);
	
	<R> ActivityHandle<R> start(
			String activity,
			Class<R> resultClass,
			Type resultType,
			ActivityOptions options,
			@Nullable Object... args);
			
	<I> ActivityHandle<Void> start(
			Class<I> activityInterface,
			Functions.Proc1<I> activity,
			ActivityOptions options);
			
	<I, A1> ActivityHandle<Void> start(
			Class<I> activityInterface,
			Functions.Proc2<I, A1> activity,
			ActivityOptions options,
			A1 arg1);
			
	<I, R> ActivityHandle<R> start(
			Class<I> activityInterface,
			Functions.Func1<I, R> activity,
			ActivityOptions options);
			
	<I, A1, R> ActivityHandle<R> start(
			Class<I> activityInterface,
			Functions.Func2<I, A1, R> activity,
			ActivityOptions options,
			A1 arg1);
	
	/// Synchronously executes activity. Ignores result.	
	void execute(
			String activity,
			ActivityOptions options,
			@Nullable Object... args);
		
	/// Synchronously executes activity.
	<R> R execute(
			String activity,
			Class<R> resultClass,
			ActivityOptions options,
			@Nullable Object... args);
		
	<R> R execute(
			String activity,
			Class<R> resultClass,
			Type resultType,
			ActivityOptions options,
			@Nullable Object... args);
			
	<I> void execute(
			Class<I> activityInterface,
			Functions.Proc1<I> activity,
			ActivityOptions options);
			
	<I, A1> void execute(
			Class<I> activityInterface,
			Functions.Proc2<I, A1> activity,
			ActivityOptions options,
			A1 arg1);
			
	<I, R> R execute(
			Class<I> activityInterface,
			Functions.Func1<I, R> activity,
			ActivityOptions options);
			
	<I, A1, R> R execute(
			Class<I> activityInterface,
			Functions.Func2<I, A1, R> activity,
			ActivityOptions options,
			A1 arg1);
	
	/// Asynchronously executes activity. Returns a void future (ignores result).
	CompletableFuture<Void> executeAsync(
			String activity,
			ActivityOptions options,
			@Nullable Object... args);
		
	/// Asynchronously executes activity. Returns a future with result.
	<R> CompletableFuture<R> executeAsync(
			String activity,
			Class<R> resultClass,
			ActivityOptions options,
			@Nullable Object... args);
		
	<R> CompletableFuture<R> executeAsync(
			String activity,
			Class<R> resultClass,
			Type resultType,
			ActivityOptions options,
			@Nullable Object... args);
			
	<I> CompletableFuture<Void> executeAsync(
			Class<I> activityInterface,
			Functions.Proc1<I> activity,
			ActivityOptions options);
			
	<I, A1> CompletableFuture<Void> executeAsync(
			Class<I> activityInterface,
			Class<I> activityInterface,
			Functions.Proc2<I, A1> activity,
			ActivityOptions options,
			A1 arg1);
			
	<I, R> CompletableFuture<R> executeAsync(
			Class<I> activityInterface,
			Functions.Func1<I, R> activity,
			ActivityOptions options);
			
	<I, A1, R> CompletableFuture<R> executeAsync(
			Class<I> activityInterface,
			Functions.Func2<I, A1, R> activity,
			ActivityOptions options,
			A1 arg1);
			
	// Additional overloads of start, execute and executeAsync
	// for Proc3...Proc7, Func3...Func7 (up to 6 activity arguments).
			```
		</details>
		```java
}

public interface UntypedActivityHandle {
  String getActivityId();
	
	/// Present if the handle was returned by `start` method
	/// or if it was set when calling `getActivityHandle`.
	/// Null if `getActivityHandle` was called with null run ID
	/// - in that case, use `describe` to get current run ID.
	@Nullable String getActivityRunId();
	
	<R> R getResult(Class<R> resultClass);
	<R> R getResult(Class<R> resultClass, @Nullable Type resultType);
	<R> CompletableFuture<R> getResultAsync(Class<R> resultClass);
	<R> CompletableFuture<R> getResultAsync(
		Class<R> resultClass, @Nullable Type resultType);
	ActivityExecutionDescription describe();
	void cancel();
	void cancel(@Nullable String reason);
	void terminate();
	void terminate(@Nullable String reason);
}

public interface ActivityHandle<R> extends UntypedActivityHandle {
	public static <R> ActivityHandle<R> fromUntyped(
		UntypedActivityHandle handle, Class<R> resultClass);
	public static <R> ActivityHandle<R> fromUntyped(
		UntypedActivityHandle handle,
		Class<R> resultClass,
		@Nullable Type resultType);

	public R getResult();
	public CompletableFuture<R> getResultAsync();
}

public final class ActivityClientOptions {
  private String namespace;
  private DataConverter dataConverter;
  private List<ActivityClientInterceptor> interceptors;
  private String identity;
  private List<ContextPropagator> contextPropagators;
	
	// + public getter for each field
	
	private ActivityClientOptions(...);
	
	public Builder newBuilder();
	
	public static class Builder {
		// setter for each field
		
		public ActivityOptions build();
	}
}

public final class ActivityOptions {
  private String id;
	private String taskQueue;
	private Duration scheduleToCloseTimeout;
	private Duration scheduleToStartTimeout;
	private Duration startToCloseTimeout;
	private Duration heartbeatTimeout;
	private RetryOptions retryOptions;
	private String summary;
	private Priority priority;
	private SearchAttributes searchAttributes;
	private ActivityIdReusePolicy idReusePolicy;
	private ActivityIdConflictPolicy idConflictPolicy;
	
	// + public getter for each field
	
	private ActivityOptions(...);
	
	public Builder newBuilder();
	
	public static class Builder {
		// setter for each field
		
		public ActivityOptions build();
	}
}

public class ActivityExecutionMetadata {
	public ActivityExecutionMetadata(
			@Nullable ActivityExecutionListInfo info);
			
	@Nullable
	public ActivityExecutionListInfo getRawListInfo();
	
	public String getActivityId();
	public String getActivityRunId();
	public String getActivityType();
	@Nullable
	public Instant getScheduledTime();
	@Nullable
	public Instant getCloseTime();
	public ActivityExecutionStatus getStatus();
	public SearchAttributes getSearchAttributes();
	public String getTaskQueue();
	@Nullable
	public Instant getExecutionDuration();
}

public class ActivityExecutionDescription extends ActivityExecutionMetadata {
	public ActivityExecutionDescription(
			@Nonnull ActivityExecutionInfo info,
			@Nonnull DataConverter dataConverter);
			
	public ActivityExecutionInfo getRawInfo();
	
	public PendingActivityState getRunState();
  @Nullable
	public Duration getScheduleToCloseTimeout();
	@Nullable
	public Duration getScheduleToStartTimeout();
  @Nullable
	public Duration getStartToCloseTimeout();
  @Nullable
	public Duration getHeartbeatTimeout();
	public boolean hasHeartbeatDetails();
	public <T> List<T> getHeartbeatDetails(Class<T> valueType);
	public RetryOptions getRetryOptions();
	@Nullable
	public Instant getLastHeartbeatTime();
	@Nullable
	public Instant getLastStartedTime();
	public int getAttempt();
	@Nullable
	public Instant getExpirationTime();
	@Nullable
	public String getLastWorkerIdentity();
	@Nullable
	public Duration getCurrentRetryInterval();
	@Nullable
	public Instant getLastAttemptCompleteTime();
	@Nullable
	public Instant getNextAttemptScheduleTime();
	@Nullable
	public WorkerDeploymentVersion getWorkerDeploymentVersion();
	public Priority getPriority();
	@Nullable
	public String getCanceledReason();
	@Nullable
	public boolean hasSummary();
	@Nullable
	public String getSummary();
}

public class ActivityExecutonCount {
	public ActivityExecutonCount(
			long count, List<AggregationGroup> groups);
	
	public long getCount();
	
	/// Returns unmodifiable list.
	public List<AggregationGroup> getGroups();
	
	public static class AggregationGroup {
		public AggregationGroup(long count, List<?> groupValues);
	
		public long getCount();
		
		/// Returns unmodifiable list.
		public List<?> getGroupValues();
	}
}
		```
	</details>
	B. `io.temporal.`<span discussion-urls="discussion://2c58fc56-7738-80ed-82aa-001c5b305422">`workflow`</span>`.Functions` 
		```java
@FunctionalInterface
public interface Proc7<T1, T2, T3, T4, T5, T6, T7>
    extends TemporalFunctionalInterfaceMarker, Serializable {
  void apply(T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, T6 t6, T7 t7);
}

@FunctionalInterface
public interface Func7<T1, T2, T3, T4, T5, T6, T7, R>
    extends TemporalFunctionalInterfaceMarker, Serializable {
  R apply(T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, T6 t6, T7 t7);
}
		```
	C. `io.temporal.common.interceptors`
		```java
public interface ActivityClientInterceptor {
	UntypedActivityHandle start(ActivityStartInput input);
	Payload getResult(ActivityGetResultInput input);
	ActivityExecutionDescription describe(ActivityDescribeInput input);
	void cancel(ActivityCancelInput input);
	void terminate(ActivityTerminateInput input);
	ActivityExecutonCount count(ActivityCountInput input);
	
	static class ActivityStartInput {
		public ActivityStartInput(
        @Nonnull String activityId,
        @Nonnull String activityType,
        @Nonnull client.ActivityOptions options,
        @Nonnull Object[] arguments,
        @Nonnull Header header);
     
     public String getActivityId();
     public String getActivityType();
     public client.ActivityOptions getOptions();
     public Object[] getArguments();
     public Header getHeader();
	}
	
	static class ActivityGetResultInput {
		public ActivityGetResultInput(
      @Nonnull String activityId,
      @Nullable String activityRunId);
      
    public String getActivityId();
    public @Nullable String getActivityRunId();
	}
	
	static class ActivityDescribeInput {
		public ActivityDescribeInput(
      @Nonnull String activityId,
      @Nullable String activityRunId);
      
    public String getActivityId();
    public @Nullable String getActivityRunId();
	}
	
	static class ActivityCancelInput {
		public ActivityCancelInput(
      @Nonnull String activityId,
      @Nullable String activityRunId,
      @Nullable String reason);
      
    public String getActivityId();
    public @Nullable String getActivityRunId();
    public @Nullable String getReason();
	}
	
	static class ActivityTerminateInput {
		public ActivityTerminateInput(
      @Nonnull String activityId,
      @Nullable String activityRunId,
      @Nullable String reason);
      
    public String getActivityId();
    public @Nullable String getActivityRunId();
    public @Nullable String getReason();
	}
	
	static class ActivityCountInput {
		public ActivityCountInput(
      @Nonnull String query);
      
    public String getQuery();
	}
}

public class ActivityClientCallsInterceptorBase
		implements ActivityClientCallsInterceptor {
	
	protected final WorkflowClientCallsInterceptor next;
	
	public ActivityClientCallsInterceptorBase(
			ActivityClientCallsInterceptor next);
	
  @Override
  public UntypedActivityHandle start(ActivityStartInput input) {
    return next.start(input);
  }
  
  // etc. for other methods
}
		```
## 2. Changes to existing types {toggle="true"}
	A. `io.temporal.client.WorkflowClient`
	```java
// New method

ActivityClient newActivityClient();
	```
	B. `io.temporal.activity.ActivityInfo` 
	```java
// New methods

@Nullable String getActivityRunId();
@Nullable String getWorkflowRunId();
boolean isInWorkflow();

// Changed methods

@Deprecated @Nullable String getRunId(); // replaced by getWorkflowRunId
@Nullable String getWorkflowId();
@Nullable String getWorkflowType();

	```
	C. `io.temporal.activity.ActivityOptions` 
		- Documentation change: options for workflow activities only.
	D. `io.temporal.client.ActivityCompletionClient` 
		```java
// New methods

<R> void complete(
		String activityId,
		Optional<String> activityRunId,
		R result)
		throws ActivityCompletionException;

void completeExceptionally(
		String activityId,
		Optional<String> activityRunId,
		Exception result)
		throws ActivityCompletionException;

<V> void reportCancellation(
		String activityId,
		Optional<String> activityRunId,
		V details)
		throws ActivityCompletionException;

<V> void heartbeat(
		String activityId,
		Optional<String> activityRunId,
		V details)
		throws ActivityCompletionException;
		```
	E. `io.temporal.common.interceptors.WorkflowClientInterceptor`
		```java
// New method

ActivityClientCallsInterceptor activityClientCallsInterceptor(
		ActivityClientCallsInterceptor next);
		```
## 3. Changes to `io.temporal.payload.context`  {toggle="true"}
	1. <span discussion-urls="discussion://2c78fc56-7738-80d4-a326-001c931f00ae">In </span><span discussion-urls="discussion://2c78fc56-7738-80d4-a326-001c931f00ae">`HasWorkflowSerializationContext`</span><span discussion-urls="discussion://2c78fc56-7738-80d4-a326-001c931f00ae">, field </span>`workflowId` is made nullable<span discussion-urls="discussion://2c78fc56-7738-80d4-a326-001c931f00ae">.</span>
	2. In `ActivitySerializationContext`, fields `workflowId` and `workflowType` are made nullable.
# Ruby
The design is written in terms of signature files (.rbs).
## 1. New types {toggle="true"}
	A. `Temporalio`
		```ruby
module ActivityIDReusePolicy
	type enum = Integer
  # maps to Api::Enums::V1::ActivityIdReusePolicy
end

module ActivityIDConflictPolicy
	type enum = Integer
  # maps to Api::Enums::V1::ActivityIdConflictPolicy
end
		```
		<empty-block/>
	B. `Temporalio.Client`
		```ruby
class ActivityHandle
  attr_reader id: String
  attr_reader run_id: String?
  attr_reader result_hint: Object?
  
  def initialize: (
    client: Client,
    id: String,
    run_id: String?,
    result_hint: Object?
  ) -> void

  def result: (
    ?rpc_options: RPCOptions?
  ) -> Object?

  def describe: (
    ?rpc_options: RPCOptions?
  ) -> ActivityExecution::Description

  def cancel: (
    ?String? reason,
    ?rpc_options: RPCOptions?
  ) -> void

  def terminate: (
    ?String? reason,
    ?rpc_options: RPCOptions?
  ) -> void
end

class ActivityExecution
	def initialize: (untyped raw) -> void

	def raw: -> untyped
	def activity_id: -> String
	def activity_run_id: -> String
	def activity_type: -> String
	def scheduled_time: -> Time?
	def close_time: -> Time?
	def status: -> ActivityExecutionStatus
	def search_attributes: -> SearchAttributes
	def task_queue: -> String
	def execution_duration: -> duration?
	
	class Description < ActivityExecution
	  def initialize: (
		  untyped raw,
		  Converters::DataConverter data_converter
	  ) -> void
	  
	  def run_state: -> PendingActivityState
		def schedule_to_close_timeout: -> duration?
		def schedule_to_start_timeout: -> duration?
		def start_to_close_timeout: -> duration?
		def heartbeat_timeout: -> duration?
		def has_heartbeat_details?: -> bool
		def retry_policy: -> RetryPolicy
		def last_heartbeat_time: -> Time?
		def last_started_time: -> Time?
	  def attempt: -> Integer
	  def last_failure: -> ???
		def expiration_time: -> Time?
		def last_worker_identity: -> String?
		def current_retry_interval: -> duration?
		def last_attempt_complete_time: -> Time?
		def next_attempt_schedule_time: -> Time?
		def last_deployment_version: -> WorkerDeploymentVersion?
		def priority: -> Priority
		def canceled_reason: -> String?
		def summary: -> String?
	end
end

class ActivityExecutionCount
  attr_reader count: Integer
  attr_reader groups: Array[AggregationGroup]

  def initialize: (
	  Integer count,
	  Array[AggregationGroup] groups
  ) -> void

  class AggregationGroup
    attr_reader count: Integer
    attr_reader group_values: Array[Object?]

    def initialize: (
	    Integer count,
	    Array[Object?] group_values
    ) -> void
  end
end

module ActivityExecutionStatus
	type enum = Integer
  # maps to Api::Enums::V1::ActivityExecutionStatus
end
		```
	C. `Temporalio.Error`
		```ruby
class ActivityAlreadyStartedError < Failure
  attr_reader activity_id: String
  attr_reader activity_type: String
  attr_reader activity_run_id: String?

  # @!visibility private
  def initialize(
	  activity_id: String,
	  activity_type: String,
	  activity_run_id: String?
  ) -> void
end
		```
## 2. New methods in `Client` {toggle="true"}
	```ruby
def start_activity(
	singleton(Activity::Definition) | Activity::Definition::Info
	| Symbol | String activity,
	*Object? args,
	id: String, 
	task_queue: String,
	?schedule_to_close_timeout: duration?,
	?schedule_to_start_timeout: duration?,
	?start_to_close_timeout: duration?,
	?heartbeat_timeout: duration?,
	?id_reuse_policy: ActivityIDReusePolicy # default ALLOW_DUPLICATE,
	?id_conflict_policy: ActivityIDConflictPolicy # default FAIL,
	?retry_policy: RetryPolicy?,
	?search_attributes: SearchAttributes?,
	?summary: String?,
	?priority: Priority, # default Priority.default
  ?arg_hints: Array[Object]?,
  ?result_hint: Object?,
  ?rpc_options: RPCOptions?
) -> ActivityHandle

def execute_activity(
	singleton(Activity::Definition) | Activity::Definition::Info
	| Symbol | String activity,
	*Object? args,
	id: String, 
	task_queue: String,
	?schedule_to_close_timeout: duration?,
	?schedule_to_start_timeout: duration?,
	?start_to_close_timeout: duration?,
	?heartbeat_timeout: duration?,
	?id_reuse_policy: ActivityIDReusePolicy # default ALLOW_DUPLICATE,
	?id_conflict_policy: ActivityIDConflictPolicy # default FAIL,
	?retry_policy: RetryPolicy?,
	?search_attributes: SearchAttributes?,
	?summary: String?,
	?priority: Priority, # default Priority.default
  ?arg_hints: Array[Object]?,
  ?result_hint: Object?,
  ?rpc_options: RPCOptions?
) -> Object?

def activity_handle(
	String activity_id,
	?activity_run_id: String?,
  ?result_hint: Object?
) -> ActivityHandle

def list_activities(
	String query,
	?rpc_options: RPCOptions?
) -> Enumerator[ActivityExecution, ActivityExecution]

def count_activities(
	String query,
	?rpc_options: RPCOptions?
) -> ActivityExecutionCount
	```
## 3. Changes to other types {toggle="true"}
	A. `Temporalio.Activity.Info`
		```ruby
# New items

attr_reader activity_run_id: String? # nil if in workflow
attr_reader in_workflow?: bool
attr_reader namespace: String


# Changed items

attr_reader workflow_id: String? # nil if standalone
attr_reader workflow_run_id: String? # nil if standalone
attr_reader workflow_type: String? # nil if standalone

# Deprecated, nil if standalone
attr_reader workflow_namespace: String?
		```
	B. `Temporalio.Client.ActivityIDReference` - new definition.
		```ruby
class ActivityIDReference
  attr_reader activity_id: String
  attr_reader activity_run_id: String?
  attr_reader workflow_id: String?
  attr_reader workflow_run_id: String?
  
  # either activity_run_id or workflow_run_id
  attr_reader run_id: String?

  def initialize: (
	  workflow_id: String,
	  run_id: String?,
	  activity_id: String
  ) -> void
  | (
    activity_id: String,
    activity_run_id: String?
  ) -> void
end
		```
	C. `Temporalio.Client.Interceptor`
		```ruby
# New methods in OutboundInterceptor

def start_activity: (StartActivityInput input) -> ActivityHandle

def describe_activity: (
	DescribeActivityInput input
) -> ActivityExecution::Description

def cancel_activity: (CancelActivityInput input) -> void

def terminate_activity: (TerminateActivityInput input) -> void

def count_activities: (
	CountActivitiesInput input
) -> ActivityExecutionCount

# New types in Interceptor

class StartActivityInput
  attr_reader activity: String
  attr_reader args: Array[Object?]
  attr_reader activity_id: String
  attr_reader task_queue: String
  attr_reader schedule_to_close_timeout: duration?
  attr_reader schedule_to_start_timeout: duration?
  attr_reader start_to_close_timeout: duration?
  attr_reader heartbeat_timeout: duration?
  attr_reader id_reuse_policy: ActivityIDReusePolicy
  attr_reader id_conflict_policy: ActivityIDConflictPolicy
  attr_reader retry_policy: RetryPolicy?
  attr_reader search_attributes: SearchAttributes?
  attr_reader summary: String?
  attr_reader arg_hints: Array[Object]?
  attr_reader result_hint: Object?
  attr_reader rpc_options: RPCOptions?

  def initialize: (...) -> void
end

class DescribeActivityInput
  attr_reader activity_id: String
  attr_reader activity_run_id: String?
  attr_reader rpc_options: RPCOptions?

  def initialize: (...) -> void
end

class CancelActivityInput
  attr_reader activity_id: String
  attr_reader activity_run_id: String?
  attr_reader reason: String?
  attr_reader rpc_options: RPCOptions?

  def initialize: (...) -> void
end

class TerminateActivityInput
  attr_reader activity_id: String
  attr_reader activity_run_id: String?
  attr_reader reason: String?
  attr_reader rpc_options: RPCOptions?

  def initialize: (...) -> void
end

class CountActivitiesInput
  attr_reader query: String
  attr_reader rpc_options: RPCOptions?

  def initialize: (...) -> void
end
		```
# Python
## 1. New types {toggle="true"}
	`temporalio.common`
		```python
# Maps to temporalio.api.enums.v1.ActivityIdReusePolicy
class ActivityIDReusePolicy(IntEnum):
    ...
		
# Maps to temporalio.api.enums.v1.ActivityIdConflictPolicy
class ActivityIDConflictPolicy(IntEnum):
    ...
		
# Maps to temporalio.api.enums.v1.ActivityExecutionStatus
class ActivityExecutionStatus(IntEnum):
    ...
		```
	`temporalio.client` 
	```python
class ActivityHandle(Generic[ReturnType]):
    @property
    def activity_id(self) -> str:
		    ...

    @property
    def activity_run_id(self) -> Option[str]:
		    ...
    
    async def result(
        self,
        *,
        rpc_metadata: Mapping[str, Union[str, bytes]] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> ReturnType:
		    ...
    
    async def describe(
        self,
        *,
        rpc_metadata: Mapping[str, Union[str, bytes]] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> ActivityExecutionDescription:
		    ...
    
    async def cancel(
        self,
        *,
        reason: Optional[str] = None,
        rpc_metadata: Mapping[str, Union[str, bytes]] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None:
		    ...
    
    async def terminate(
        self,
        *,
        reason: Optional[str] = None,
        rpc_metadata: Mapping[str, Union[str, bytes]] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None:
		    ...

@dataclass(frozen=True)
class ActivityExecution:
    activity_id: str
    activity_type: str
    activity_run_id: Optional[str]
    close_time: Optional[datetime]
    execution_duration: Optional[timedelta]
    namespace: str # not present in proto, copied from calling client
    raw_info: Union[
	    temporalio.api.activity.v1.ActivityExecutionListInfo,
	    temporalio.api.activity.v1.ActivityExecutionInfo
    ]
    scheduled_time: datetime
    search_attributes: temporalio.common.SearchAttributes
    state_transition_count: Optional[int] # not always present on List operation, see proto docs for details
    status: temporalio.common.ActivityExecutionStatus
    task_queue: str

@dataclass(frozen=True)
class ActivityExecutionDescription(ActivityExecution):
		attempt: int
    canceled_reason: Optional[str]
    current_retry_interval: Optional[timedelta]
    eager_execution_requested: bool
    expiration_time: datetime
    heartbeat_details: Sequence[Any]
    last_attempt_complete_time: Optional[datetime]
    last_failure: Optional[Exception]
    last_heartbeat_time: Optional[datetime]
    last_started_time: Optional[datetime]
    last_worker_identity: str
    retry_policy: Optional[temporalio.common.RetryPolicy]
    next_attempt_schedule_time: Optional[datetime]
    paused: bool
    run_state: Optional[temporalio.common.PendingActivityState]
    
    
class ActivityExecutionAsyncIterator:
    def __init__(
        self,
        client: Client,
        input: ListActivitiesInput,
    ) -> None:
		    ...

    @property
    def current_page_index(self) -> int:
		    ...

    @property
    def current_page(
        self,
    ) -> Optional[Sequence[ActivityExecution]]:
		    ...

    @property
    def next_page_token(self) -> Optional[bytes]:
		    ...

    async def fetch_next_page(
		    self, *, page_size: Optional[int] = None
    ) -> None:
		    ...

    def __aiter__(self) -> ActivityExecutionAsyncIterator:
		    ...

    async def __anext__(self) -> ActivityExecution:
		    ...
		    
@dataclass
class ActivityExecutionCount:
    count: int
    groups: Sequence[ActivityExecutionCountAggregationGroup]

@dataclass
class ActivityExecutionCountAggregationGroup:
    count: int
    group_values: Sequence[temporalio.common.SearchAttributeValue]
	```
## 2. New methods in `client.Client` {toggle="true"}
	A. Start activity methods:
		1. Same set of methods as for workflow activities:<br>`start_activity`, `start_activity_class`, `start_activity_method`, `execute_activity`, `execute_activity_class`, `execute_activity_method`
		2. All `start_activity` methods are async and return `ActivityHandle[Any]`.
		3. All `execute_activity` methods are async, wait for activity completion and return the result as `Any`.
		4. All listed methods have overloads for specific activity types (`str`, various `Callable` types), same as workflow activities. For `Callable` overloads, a specific return type is used instead of `Any`.
		5. All listed methods have the same set of arguments.
	```python
# Bolded arguments are differences from the prototype.
async def start_activity(
    self,
    activity: Any,
    *,
    args: Sequence[Any] = [],
    # Note: workflow's start_activity() has activity_id argument instead of id.
    # The mismatch is intentional - because activity_id should never be set
    # except in rare circumstances, but this id should always have meaningful
    # value, giving them different names avoids potential copy-paste errors.
    id: str, 
    task_queue: str,
    result_type: Optional[type] = None,
    schedule_to_close_timeout: Optional[timedelta] = None,
    schedule_to_start_timeout: Optional[timedelta] = None,
    start_to_close_timeout: Optional[timedelta] = None,
    heartbeat_timeout: Optional[timedelta] = None,
    id_reuse_policy: temporalio.common.ActivityIDReusePolicy = temporalio.common.ActivityIDReusePolicy.ALLOW_DUPLICATE,
    id_conflict_policy: temporalio.common.ActivityIDConflictPolicy = temporalio.common.ActivityIDConflictPolicy.FAIL,
    retry_policy: Optional[temporalio.common.RetryPolicy] = None,
    search_attributes: Optional[temporalio.common.TypedSearchAttributes] = None,
    summary: Optional[str] = None,
    # no static_details (matches workflow activity API)
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
    rpc_metadata: Mapping[str, Union[str, bytes]] = {},
    rpc_timeout: Optional[timedelta] = None,
) -> temporalio.client.ActivityHandle[Any]:
		...
	```
	B. Other methods:
	```python
def list_activities(
    self,
    query: Optional[str] = None,
    *,
    limit: Optional[int] = None,
    page_size: int = 1000,
    next_page_token: Optional[bytes] = None,
    rpc_metadata: Mapping[str, Union[str, bytes]] = {},
    rpc_timeout: Optional[timedelta] = None,
) -> ActivityExecutionAsyncIterator:
		...

async def count_activities(
    self,
    query: Optional[str] = None,
    *,
    rpc_metadata: Mapping[str, Union[str, bytes]] = {},
    rpc_timeout: Optional[timedelta] = None,
) -> ActivityExecutionCount:
		...

@overload
def get_activity_handle(
    self,
    activity_id: str,
    *
    activity_run_id: Optional[str] = None
) -> ActivityHandle[Any]:
		...

@overload
def get_activity_handle(
    self,
    activity_id: str,
    *
    result_type: type[ReturnType],
    activity_run_id: Optional[str] = None,
) -> ActivityHandle[ReturnType]:
		...

def get_activity_handle(
    self,
    activity_id: str,
    *
    result_type: Optional[type] = None,
    activity_run_id: Optional[str] = None
) -> ActivityHandle[ReturnType]:
		...
	```
## 3. New methods in `client.OutboundInterceptor` {toggle="true"}
	```python
async def start_activity(
		self, input: StartActivityInput
) -> ActivityHandle[Any]:
		...

async def get_activity_result(
    self, input: GetActivityResultInput[ReturnType]
) -> ReturnType:
    ...

async def describe_activity(
    self, input: DescribeActivityInput
) -> ActivityExecutionDescription:
    ...

async def cancel_activity(
		self, input: CancelActivityInput
) -> None:
		...

async def terminate_activity(
		self, input: TerminateActivityInput
) -> None:
		...

def list_activities(
    self, input: ListActivitiesInput
) -> ActivityExecutionAsyncIterator:
		...

async def count_activities(
    self, input: CountActivitiesInput
) -> ExecutionCount:
		...

@dataclass
class StartActivityInput:
    activity_type: str
    args: Sequence[Any]
    id: str
    task_queue: str
    result_type: Optional[type]
    schedule_to_close_timeout: Optional[timedelta]
    schedule_to_start_timeout: Optional[timedelta]
    start_to_close_timeout: Optional[timedelta]
    heartbeat_timeout: Optional[timedelta]
    id_reuse_policy: temporalio.common.ActivityIDReusePolicy
    id_conflict_policy: temporalio.common.ActivityIDConflictPolicy
    retry_policy: Optional[temporalio.common.RetryPolicy]
    search_attributes: Optional[temporalio.common.TypedSearchAttributes]
    summary: Optional[str]
    priority: temporalio.common.Priority
    headers: Mapping[str, temporalio.api.common.v1.Payload]
    rpc_metadata: Mapping[str, Union[str, bytes]]
    rpc_timeout: Optional[timedelta]

@dataclass
class DescribeActivityInput:
		activity_id: str
		activity_run_id: Optional[str]
    rpc_metadata: Mapping[str, Union[str, bytes]]
    rpc_timeout: Optional[timedelta]

@dataclass
class GetActivityResultInput[ReturnType]:
		activity_id: str
		activity_run_id: Optional[str]
		result_type: type[ReturnType]
    rpc_metadata: Mapping[str, Union[str, bytes]]
    rpc_timeout: Optional[timedelta]

@dataclass
class CancelActivityInput:
		activity_id: str
		activity_run_id: Optional[str]
    reason: Optional[str]
    rpc_metadata: Mapping[str, Union[str, bytes]]
    rpc_timeout: Optional[timedelta]
    
@dataclass
class TerminateActivityInput:
		activity_id: str
		activity_run_id: Optional[str]
    reason: Optional[str]
    rpc_metadata: Mapping[str, Union[str, bytes]]
    rpc_timeout: Optional[timedelta]
    
@dataclass
class ListActivitiesInput:
    query: Optional[str]
    limit: Optional[int]
    page_size: int
    next_page_token: Optional[bytes]
    rpc_metadata: Mapping[str, Union[str, bytes]]
    rpc_timeout: Optional[timedelta]

@dataclass
class CountActivitiesInput:
    query: Optional[str]
    rpc_metadata: Mapping[str, Union[str, bytes]]
    rpc_timeout: Optional[timedelta]
	```
## 4. Other changes to existing types {toggle="true"}
	A. `activity.Info`
	```python
# new fields
namespace: str
activity_run_id: Optional[str] # None if in workflow

@property
def in_workflow(self) -> bool:
    return workflow_id is not None

# changed signature
workflow_id: Optional[str] # None if standalone
workflow_namespace: Optional[str] # DEPRECATED, None if standalone
workflow_run_id: Optional[str] # None if standalone
workflow_type: Optional[str] # None if standalone
	```
	B. `client.AsyncActivityIDReference`
	```python
# changed signature
workflow_id: Optional[str]
	```
## 5. Serialization context (TODO) {toggle="true"}
	<empty-block/>
# TypeScript
## Sample usage {toggle="true"}
	```typescript
// Existing Client API
const client = new Client({
    connection: await Connection.connect({ address: 'localhost:7233' }),
});


// Can access the untyped activity client as a property on the `client` object.
// That already exists (for AsyncActivityCompletionClient), but gets reporposed
// to also include untyped StandaloneActivityClient (the later extends the former).
// client.activity


/* Example use:

interface MyActivity {
  doWork(x: number): Promise<string>;
}

const opts = {
	taskQueue: 'task-queue',
	startToCloseTimeout: '1 minute'
}; 

// Typed activity start
const handle = await client.activity.typed<MyActivity>().start(
	'doWork', { id: 'activity1', ...opts }, 1
);
const result = await handle.result();

// Typed activity execution
const result = await client.activity.typed<MyActivity>().execute(
	'doWork', { id: 'activity2', ...opts }, 1
);

// Untyped activity execution
const result = await client.activity.execute(
	'doWork', { id: 'activity3', ...opts }, 'abc'
);

const result = await client.activity.execute(
	doWorkFunction, { id: 'activity3', ...opts }, 'abc'
);

*/
	```
## 1. New module `client/src/activity-client.ts`  {toggle="true"}
	```typescript
import { AsyncCompletionClient, ActivityNotFoundError } from './async-completion-client'

export ActivityNotFoundError;
export type ActivityClientOptions = BaseClientOptions;
export type LoadedActivityClientOptions = LoadedWithDefaults<ActivityClientOptions>;

export class ActivityClient
	extends AsyncCompletionClient
	implements TypedActivityClient<UntypedActivities>
{
  public constructor(options?: ActivityClientOptions);
  
  // Pass in activities optionally for JS callers
  /// where the activity implementations are available.
  public createTypedClient<T>(activities?: T): TypedActivityClient<T> {
	  return this;
  }
  
  public async start<I extends any[] = any[], O = any>(
	  activity: string | ActivityFunction<I, O>, options: ActivityOptions<I>
  ): Promise<ActivityHandle<O>>;
  
  public async execute<I extends any[] = any[], O = any>(
	  activity: string | ActivityFunction<I, O>, options: ActivityOptions<I>
  ): Promise<O>;
  
  // FIXME: On Workflow Client, the Handle's generic is the signature of
  //        the workflow funciton, not it's output. Should we do the same?
  public getHandle<O = any>(
	  activityId: string, activityRunId?: string
  ): ActivityHandle<O>;
  
  // string or ListOptions?
  public list(query: string|ListOptions): AsyncIterable<ActivityExecutionInfo>;
  
  public async count(string query): Promise<CountActivityExecutions>;
}


interface TypedActivityClient<T> {
  start<K extends ActivityKey<T>>(
	  activity: K,
	  options: Replace<ActivityOptions, ActivityArgsOptions<T[K]>,
  ): Promise<ActivityHandle<ActivityResult<T, K>>>;
  
  start<F extends T[ActivityKey<T>]>(
	  activity: F,
	  options: Replace<ActivityOptions, ActivityArgsOptions<F>,
  ): Promise<ActivityHandle<ActivityFuncResult<F>>>;
  
  execute<K extends ActivityKey<T>>(
	  activity: K,
	  options: Replace<ActivityOptions, ActivityArgsOptions<T[K]>,,
  ): Promise<ActivityResult<T, K>>;

  execute<F extends T[ActivityKey<T>]>(
	  activity: F,
	  options: Replace<ActivityOptions, ActivityArgsOptions<F>,
  ): Promise<ActivityFuncResult<F>>;
}

// An instance of ActivityHandle is bound to the client it's created with.
export interface ActivityHandle<R> {
  readonly activityId: string;
  readonly activityRunId: string;
  result(): Promise<R>;
  describe(): Promise<ActivityDescription>;
  cancel(string reason): Promise<void>;
  terminate(string reason): Promise<void>;
}

export interface ActivityOptions {
	id: string;
  taskQueue: string;
  heartbeatTimeout?: Duration;
  retry?: RetryPolicy;
  startToCloseTimeout?: Duration;
  scheduleToStartTimeout?: Duration;
  scheduleToCloseTimeout?: Duration;
  summary?: string;
  priority?: Priority;
  idReusePolicy?: ActivityIdReusePolicy;
  idConflictPolicy?: ActivityIdConflictPolicy;
  typedSearchAttributes?: SearchAttributePair[] | TypedSearchAttributes;
}

export type ActivityKey<T> = {
  [K in keyof T & string]:
	  T[K] extends ActivityFunction<any, any> ? K : never;
}[keyof T & string];

export type ActivityFunction<T> = {
  [K in keyof T & string]:
	  T[K] extends ActivityFunction<any, any> ? T[K] : never;
}[keyof T & string];

export type ActivityArgs<T, K extends ActivityKey<T>> =
	T[K] extends ActivityFunction<infer P, any> ? P : never;

export type ActivityFuncArgs<F> =
	F extends ActivityFunction<infer P, any> ? P : never;
	
export type ActivityResult<T, K extends ActivityKey<T>> =
	T[K] extends ActivityFunction<any, infer R> ? R : never;

export type ActivityFuncResult<F> =
	F extends ActivityFunction<any, infer R> ? R : never;

export type ActivityArgsOptions<F extends ActivityFunction> =
  (Parameters<F> extends [any, ...any[]]
    ? {
        /**
         * Arguments to pass to the Activity
         */
        args: Parameters<F> | Readonly<Parameters<F>>;
      }
    : {
        /**
         * Arguments to pass to the Activity
         */
        args?: Parameters<F> | Readonly<Parameters<F>>;
      });


	```
## 2. New types in `client.types` {toggle="true"}
	```typescript
export interface CountActivitiesResult {
  readonly count: number;
  readonly groups: {
    readonly count: number;
    readonly groupValues: TypedSearchAttributeValue<SearchAttributeType>[];
  }[];
}

export type RawActivityExecutionInfo = proto.temporal.api.activity.v1.IActivityExecutionInfo;
export type RawActivityExecutionListInfo = proto.temporal.api.activity.v1.IActivityExecutionListInfo;

export interface ActivityExecutionInfo {
	rawListInfo?: RawActivityExecutionListInfo;
	activityId: string;
	activityRunId: string;
	activityType: string;
	scheduledTime: Date;
	closeTime: Date;
	searchAttributes: TypedSearchAttributes;
	taskQueue: string;
	stateTransitionCount?: number;
	stateSizeBytes: number;
	executionDuration: Date;
}

export interface ActivityExecutionDescription extends ActivityExecutionInfo {
	rawInfo: RawActivityExecutionInfo;
	lastHeartbeatTime: Date;
	lastStartedTime: Date;
	attempt: number;
	retryPolicy: RetryPolicy;
	expirationTime: Date;
	lastWorkerIdentity: string;
	currentRetryInterval: Date;
	lastAttemptCompleteTime: Date;
	nextAttemptScheduleTime: Date;
	lastDeploymentVersion: WorkerDeploymentVersion;
	priority: Priority;
  eagerExecutionRequested: bool;
  canceledReason: string;
};

// Maps to temporal.api.enums.v1.ActivityIdReusePolicy
export const ActivityIdReusePolicy { ... }
export type ActivityIdReusePolicy =
	(typeof ActivityIdReusePolicy)[keyof typeof ActivityIdReusePolicy];
export const [encodeActivityIdReusePolicy, decodeActivityIdReusePolicy] =
	makeProtoEnumConverters<...>(...);

// Maps to temporal.api.enums.v1.ActivityIdConflictPolicy
export const ActivityIdConflictPolicy { ... }
export type ActivityIdConflictPolicy =
	(typeof ActivityIdConflictPolicy)[keyof typeof ActivityIdConflictPolicy];
export const [encodeActivityIdConflictPolicy, decodeActivityIdConflictPolicy] =
	makeProtoEnumConverters<...>(...);

// Maps to temporal.api.enums.v1.ActivityExecutionStatus
export const ActivityExecutionStatus { ... }
export type ActivityExecutionStatus =
	(typeof ActivityExecutionStatus)[keyof typeof ActivityExecutionStatus];
export const [encodeActivityExecutionStatus, decodeActivityExecutionStatus] =
	makeProtoEnumConverters<...>(...);

	```
## 3. Changes to `client.interceptors`  {toggle="true"}
	A. `ClientInterceptors` has new field `activity?: ActivityClientInterceptor[];`.
	B. New types:
	```typescript
export interface ActivityClientInterceptor {
  start?: (
	  input: ActivityStartInput, next: Next<this, 'start'>
	) => Promise<ActivityHandle>;
	
  getResult?: (
	  input: ActivityGetResultInput, next: Next<this, 'getResult'>
	) => Promise<any>;
	
  describe?: (
	  input: ActivityDescribeInput, next: Next<this, 'describe'>
	) => Promise<ActivityExecutionDescription>;
	
  cancel?: (
	  input: ActivityCancelInput, next: Next<this, 'cancel'>
	) => Promise<void>;
	
  terminate?: (
	  input: ActivityTerminateInput, next: Next<this, 'terminate'>
	) => Promise<void>;
	
  list?: (
	  input: ActivityListInput, next: Next<this, 'list'>
	) => AsyncIterable<ActivityExecutionInfo>;
	
  count?: (
	  input: ActivityCountInput, next: Next<this, 'count'>
	) => Promise<CountActivitiesResult>;
}

export interface ActivityStartInput {
	readonly activityType: string;
	readonly args: any[];
	readonly options: ActivityOptions;
	readonly headers: Headers;
}

export interface ActivityGetResultInput {
	readonly activityId: string;
	readonly activityRunId: string;
	readonly headers: Headers;
}

export interface ActivityDescribeInput {
	readonly activityId: string;
	readonly activityRunId: string;
	readonly headers: Headers;
}

export interface ActivityCancelInput {
	readonly activityId: string;
	readonly activityRunId: string;
	readonly reason: string;
	readonly headers: Headers;
}

export interface ActivityTerminateInput {
	readonly activityId: string;
	readonly activityRunId: string;
	readonly reason: string;
	readonly headers: Headers;
}

export interface ActivityListInput {
	readonly query: string;
	readonly headers: Headers;
}

export interface ActivityCountInput {
	readonly query: string;
	readonly headers: Headers;
}
	```
## 4. Other changes to existing types {toggle="true"}
	A. `activity.Info` 
	```typescript
// new fields

readonly namespace: string;
readonly activityRunId?: string; // undefined if in workflow
readonly inWorkflow: boolean; // calculated property: false if workflowExecution is undefined

// changed fields

readonly activityNamespace: string // deprecated, same as namespace
readonly workflowNamespace?: string // deprecated, undefined if standalone
readonly workflowExecution?: interface { ... } // undefined if standalone
readonly workflowType?: string // undefined if standalone
	```
	B. `client.async-completion-client.FullActivityId` 
	```typescript
export interface FullActivityId {
  workflowId?: string; // undefined if standalone
  runId?: string; // either workflow run ID or activity run ID
  activityId: string;
}
	```
	C. `common.activity-options.ActivityOptions` 
		- Documentation change: options for starting an activity in workflow.
			- Open questions:
				- Should the package be moved to `workflow.activity-options` and the old one deprecated?
				- Should `client.activity-client.ActivityOptions` have its own package, or moved to `client.types`?
<empty-block/>
<empty-block/>
</content>
</page>