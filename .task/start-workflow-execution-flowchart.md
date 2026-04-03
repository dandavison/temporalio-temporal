# StartWorkflowExecution: Conflict and Reuse Policy Flowchart

```mermaid
flowchart TD
    Start["StartWorkflowExecution(requestID, workflowID,<br/>ReusePolicy, ConflictPolicy)"]

    Start --> Prepare["prepare(): validate request,<br/>apply defaults (ConflictPolicy=FAIL),<br/>migrate TerminateIfRunning → TerminateExisting"]
    Prepare --> PrepareNew["prepareNewWorkflow(): generate new RunID,<br/>create WorkflowSnapshot + events"]
    PrepareNew --> LockCurrent["lockCurrentWorkflowExecution()"]
    LockCurrent --> CreateBrandNew["createBrandNew():<br/>persistence.CreateWorkflowModeBrandNew"]

    CreateBrandNew --> BrandNewOK{Success?}
    BrandNewOK -->|Yes| ReturnStartNew([Started=true, new RunID<br/><b>StartNew</b>])

    BrandNewOK -->|"No: CurrentWorkflowConditionFailedError<br/>(workflow with this ID already exists)"| HandleConflict

    %% ── Request-ID dedup ──────────────────────────────────────
    HandleConflict["handleConflict()"]
    HandleConflict --> CheckRequestID{"requestID found in<br/>current workflow's<br/>RequestIDs?"}

    CheckRequestID -->|Yes| CheckEventType{"RequestIDInfo.EventType<br/>== WORKFLOW_EXECUTION_STARTED?"}
    CheckEventType -->|Yes| ReturnDeduped1(["Started=true*, existing RunID<br/><b>StartDeduped</b><br/>(idempotent retry)"])
    CheckEventType -->|No| ReturnDeduped2(["Started=false, existing RunID<br/><b>StartDeduped</b><br/>(requestID used by non-start event)"])

    %% ── New request: check current workflow state ─────────────
    CheckRequestID -->|No| ResolveDup["ResolveDuplicateWorkflowID()"]
    ResolveDup --> CheckState{"Current workflow<br/>execution state?"}

    %% ── RUNNING workflows: apply ConflictPolicy ──────────────
    CheckState -->|"RUNNING<br/>(CREATED or RUNNING)"| ConflictPolicy{"ConflictPolicy?"}

    ConflictPolicy -->|FAIL| ErrAlreadyStarted1([WorkflowExecutionAlreadyStartedError<br/><b>StartErr</b>])

    ConflictPolicy -->|USE_EXISTING| UseExisting["handleUseExistingWorkflow():<br/>optionally attach requestID,<br/>callbacks, links to existing run"]
    UseExisting --> ReturnReused(["Started=false, existing RunID,<br/>status=RUNNING<br/><b>StartReused</b>"])

    ConflictPolicy -->|TERMINATE_EXISTING| TermReuseCheck{"MinimalReuseInterval<br/>elapsed since current<br/>workflow start?"}
    TermReuseCheck -->|No| ErrBusy1([ResourceExhausted<br/>BUSY_WORKFLOW<br/><b>StartErr</b>])
    TermReuseCheck -->|Yes| TerminateAndCreate["GetAndUpdateWorkflowWithNew():<br/>terminate current run +<br/>create new run in one transaction"]
    TerminateAndCreate --> TermOK{Success?}
    TermOK -->|Yes| ReturnStartNewTerm(["Started=true, new RunID<br/><b>StartNew</b>"])
    TermOK -->|"No: ErrWorkflowCompleted<br/>(race: workflow completed<br/>between check and terminate)"| ReturnUnavailable(["Unavailable error<br/>(client retries from top)"])

    %% ── COMPLETED workflows: apply ReusePolicy ───────────────
    CheckState -->|COMPLETED| ReusePolicy{"ReusePolicy?"}

    ReusePolicy -->|ALLOW_DUPLICATE| ReuseIntervalCheck1{"MinimalReuseInterval<br/>elapsed?"}
    ReuseIntervalCheck1 -->|Yes| CreateAsCurrent["createAsCurrent():<br/>persistence.CreateWorkflowModeUpdateCurrent"]
    ReuseIntervalCheck1 -->|No| ErrBusy2([ResourceExhausted<br/>BUSY_WORKFLOW<br/><b>StartErr</b>])

    ReusePolicy -->|ALLOW_DUPLICATE_FAILED_ONLY| CheckFailedStatus{"Previous close status<br/>∈ {Failed, Canceled,<br/>Terminated, TimedOut}?"}
    CheckFailedStatus -->|Yes| ReuseIntervalCheck2{"MinimalReuseInterval<br/>elapsed?"}
    ReuseIntervalCheck2 -->|Yes| CreateAsCurrent
    ReuseIntervalCheck2 -->|No| ErrBusy3([ResourceExhausted<br/>BUSY_WORKFLOW<br/><b>StartErr</b>])
    CheckFailedStatus -->|"No (Completed,<br/>ContinuedAsNew)"| ErrAlreadyStarted2([WorkflowExecutionAlreadyStartedError<br/><b>StartErr</b>])

    ReusePolicy -->|REJECT_DUPLICATE| ErrAlreadyStarted3([WorkflowExecutionAlreadyStartedError<br/><b>StartErr</b>])

    CreateAsCurrent --> ReturnStartNewReuse(["Started=true, new RunID<br/><b>StartNew</b>"])

    %% ── ZOMBIE state ─────────────────────────────────────────
    CheckState -->|ZOMBIE| ErrInternal(["Internal error<br/>(invalid state)"])

    %% ── Styling ──────────────────────────────────────────────
    classDef success fill:#d4edda,stroke:#28a745,color:#000
    classDef error fill:#f8d7da,stroke:#dc3545,color:#000
    classDef dedup fill:#fff3cd,stroke:#ffc107,color:#000
    classDef reused fill:#cce5ff,stroke:#007bff,color:#000

    class ReturnStartNew,ReturnStartNewTerm,ReturnStartNewReuse success
    class ErrAlreadyStarted1,ErrAlreadyStarted2,ErrAlreadyStarted3,ErrBusy1,ErrBusy2,ErrBusy3,ErrInternal,ReturnUnavailable error
    class ReturnDeduped1,ReturnDeduped2 dedup
    class ReturnReused reused
```

## Legend

| Color | Meaning |
|-------|---------|
| Green | New workflow created (`Started=true`) |
| Blue | Existing workflow reused (`Started=false`, `UseExisting`) |
| Yellow | Request-ID dedup (idempotent retry) |
| Red | Error returned to caller |

## Key Implementation References

- Policy resolution: `service/history/api/workflow_id_dedup.go`
- Starter orchestration: `service/history/api/startworkflow/api.go`
- `FailedWorkflowStatuses` = {Failed, Canceled, Terminated, TimedOut} — defined in `service/history/consts/const.go:131-136`
- `MinimalReuseInterval`: dynamic config per namespace; prevents rapid sequential starts of the same workflow ID
- Default `ConflictPolicy` = `FAIL` (applied in `prepare()` if unset)
- Deprecated `ReusePolicy=TerminateIfRunning` is migrated to `ConflictPolicy=TerminateExisting` + `ReusePolicy=AllowDuplicate` before any policy evaluation
