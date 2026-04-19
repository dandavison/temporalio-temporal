---
title: "[1-pager] Standalone Activity Export"
notion_url: "https://www.notion.so/3308fc56773880ddb0cfd73408b7d07e"
last_edited: "2026-04-08T23:50:31.810Z"
page_type: spec
---

**Status:** 1-pager Approved
**Drivers:** Phil Prasek, Dustin Cote
**Parent feature:** [PRD: Standalone Activities](https://www.notion.so/temporalio/1ee8fc567738806d8b6fe8e2eeae0fc4)
GA H2 2026

> **Compound blocker note:** Block and Stripe both need Export + Start Delay + Java/Ruby SDK. Export alone does not unblock either account. All three must converge.

## **Problem Statement**
### **The Job-to-be-Done**
When I execute standalone activities for payment processing, compliance-sensitive operations, or auditable business logic, I need a durable record of every execution (start, result, retries, failures) exported to my data lake so my compliance and audit teams can verify what happened, when, and why.
Today, Workflow Execution Export covers workflow histories. Standalone activity executions have no parent workflow, so they fall outside the existing export pipeline entirely. There is no workaround - customers cannot reconstruct execution records from metrics or logs alone.
### **Initial Evidence**
- **Block** (Owner: Andrew Eidenshink, SA: Josh Smith) - 32.1B ops total (Java 87%, Go 11%, Python <1%). Primary SDK is Java (no SAA support yet). Needs audit trail for all activity executions. Hard blocker - won't adopt SAA without it. Also blocked on Java SDK + Start Delay.
- **Stripe** (Owner: Conrad Vogel, SA: Jimmy Jea) - 76.0B ops total (Ruby 51%, Java 41%, Go 4%, Python 1%). Primary SDKs are Ruby/Java (neither has SAA support yet). Compliance requirement for activity execution records. Hard blocker - won't adopt SAA without it. Also blocked on Ruby/Java SDK + Start Delay.
	> "Joseph explicitly stated they will use Exports for compliance."
- **Rippling** (Owner: Tom Kadlick, SA: Taylor Khan) - Go. "We can't just use the standalone activity because we need to emit that metadata for our own purposes... we literally have an extra action just to recreate our image of what's in temporal's activity queue." Described orchestration costs "on par with compute." Partial blocker - can't adopt SAA without activity execution metadata/observability. Export is part of the solution.
- **Roblox** - Evaluating Temporal for escrow/ledgering system (30-50M workflows/day, ~5 activities each). "Need for SOX compliance and auditability" is a gating requirement. If they adopt SAA for simpler financial operations, export would be mandatory.
### **Who and How Many**
- **2 hard-blocked accounts** (Block, Stripe) - among the largest Temporal customers
- **1 partial blocker** (Rippling) - can't adopt SAA without activity execution metadata; building Postgres alternative
- **1 future requirement** (Roblox) - SOX compliance gating requirement for planned financial workflows (30-50M workflows/day)
- **Segment:** Digital Native (Fintech subsegment) + G2K / FinServ (Roblox financial services)
- **Generalizable:** Any regulated industry customer (FinServ, Healthcare, Insurance) will need execution audit trails before adopting SAA.
## **Impact Classification**
- **Bucket:** Table Stakes
- **Rationale:** Export for SAA is not a differentiator - it's the minimum bar for compliance-sensitive customers. Workflow Export already exists; SAA Export extends coverage to a new execution type. Without it, SAA is unusable for any workload subject to auditing requirements, which includes most G2K / FinServ use cases and large Digital Native fintech accounts.
- **Conviction level:** High - Three accounts independently identified this as a hard blocker with no workaround.
## **Strategic Alignment**
- **Target Segment:** Digital Native (Fintech), G2K / FinServ
- **FY27 Objectives:**
	- **Primary:** Earn Enterprise Credibility (Obj 4) - compliance and audit capabilities are table stakes for enterprise adoption of any new primitive
	- **Secondary:** Sharpen & Scale Temporal Use Cases (Obj 2) - SAA targets simple tasks, job queue replacement, and payment processing use cases, all of which require audit trails in regulated environments
- **Use Case:** Payment processing, compliance-sensitive batch operations, auditable fire-and-forget tasks
## **High-Level Solution**
### **Concept**
Extend the existing Workflow Execution Export pipeline to cover Standalone Activity executions. SAA executions do not generate event histories and lack a parent workflow context.
The export system needs to: (1) capture SAA execution events in the same format as workflow execution exports, (2) route them through the existing export sink configuration (S3, GCS, etc.), and (3) expose them via the same Cloud API that customers use to configure and manage exports today. The goal is parity with workflow export - not a new system.
### **Required Teams**

| Team | Responsibility |
|---|---|
| **Server / History** | SAA event history capture and export pipeline integration |
| **Cloud Platform** | Export sink configuration for SAA execution types |
| **SDKs** | No SDK changes expected - export is server-side |
| **Docs** | Update export documentation to cover SAA executions |

### **Rough LOE**
**M (Medium)** - The export infrastructure exists. Primary work is extending the existing pipeline to a new execution type, adding SAA-specific event schemas, and ensuring sink routing handles SAA executions. No new infrastructure required, but needs careful design to handle SAA's lack of parent workflow context and potentially high execution volumes (Block: 32B ops, Stripe: 76B ops).
### Pricing
Export pricing is currently 1 action per Workflow exported. SAA in general have significantly less data to export because Workflows can contain many Activities but SAA is only a single Activity. The initial pricing proposal is to charge 1 action per SAA exported and accept the potentially higher margin. A more rigorous pricing decision should be made after answering:
- what is the COGS for export (Workflows only) today?
- will customers avoid adopting SAA because export is too expensive to meet their needs at 1 action per SAA exported?
- does the future roadmap for export (i.e. filtering, on platform analytics) make 1 action per SAA exported reasonable because it will only be done by less price sensitive customers?
## **Key Risks & Open Questions**

| Risk/Question | Why It Matters | How to Resolve |
|---|---|---|
| SAA volume vs. workflow volume | SAA targets high-throughput simple tasks. Export at 76B+ ops scale could overwhelm existing sinks. | Capacity modeling with Cloud Platform team. May need sampling or filtering options. |
| Export schema for parentless executions | Existing export schema assumes parent workflow. SAA has no parent. | Design review with Server team. Define SAA-specific export event types. |
| Dependency on SDK availability | Block needs Java, Stripe needs Ruby/Java. Export alone doesn't unblock them. | Track as parallel workstream. Export should ship before or alongside SDK GA to avoid serial blocking. |
| Start Delay also required | Both Block and Stripe need Start Delay + Export. One without the other still blocks adoption. | Coordinate shipping timeline with Start Delay work. |
| Filtering and retention controls | Enterprises will want to filter which SAA executions get exported and set retention policies. | Include configurable filters in design. Can be v2 if basic export ships first. |

## **Recommendation**
**Pursue** - but coordinate with Java/Ruby SDK and Start Delay timelines. Export is necessary but not sufficient for Block and Stripe - all three blockers (Export + SDK + Start Delay) must resolve for adoption. Prioritize Export design now so it ships alongside or before SDK GA. The compliance requirement is absolute and generalizable across regulated segments (now validated by 5 accounts including Rippling, Roblox, and an unnamed fintech with SOX needs), making this a prerequisite for SAA's enterprise credibility story.

# Appendix: Workflow Export Analysis

## Part 1: Current Workflow Export - Complete Reference
### Overview
Temporal Cloud Workflow History Export ships closed workflow execution histories to customer-owned object storage (AWS S3, GCP GCS) on an hourly cadence. Public Preview status with production-ready delivery guarantees.

**Key docs and references:**
- Blog: [Introducing Workflow History Export](https://temporal.io/blog/introducing-workflow-history-export)
- Docs: [Cloud Export](https://docs.temporal.io/cloud/export) | [Setup](https://docs.temporal.io/cloud/export/setup)
- Proto schema: [temporalio/api - export/v1/message.proto](https://github.com/temporalio/api/blob/master/temporal/api/export/v1/message.proto)

### How It Works
1. **Scope:** Per-namespace configuration. Only **closed** workflow executions (completed, failed, terminated, timed out, canceled) are exported.
2. **Cadence:** Hourly, beginning 10 minutes after the hour. Up to 24 hours for a closed workflow to appear.
3. **Delivery guarantee:** At-least-once (duplicates possible across export windows).
4. **Storage:** AWS S3 or GCP GCS bucket owned by the customer.
5. **Billing:** One billable action per exported workflow execution. Displayed separately per namespace on invoices.
6. **HA behavior:** Export configuration is region-pinned. Does NOT failover with namespace. Resumes with full backfill after regional recovery.

### Data Format - Proto3 Binary
```protobuf
// temporal/api/export/v1/message.proto
syntax = "proto3";
package temporal.api.export.v1;

import "temporal/api/history/v1/message.proto";

message WorkflowExecution {
    temporal.api.history.v1.History history = 1;
}

message WorkflowExecutions {
    repeated WorkflowExecution items = 1;
}
```

## Part 2: Before Picture - What Exists Today
### Standalone Activity Export: Nothing

| Capability | Status |
|---|---|
| Export closed SAA executions | Not available |
| SAA events in export pipeline | Not available |
| SAA event history format | Not defined |
| SAA in export proto schema | Not defined |
| SAA in export sink configuration | Not available |
| SAA in export monitoring | Not available |

**Gap:** Standalone activity executions generate event histories (start, heartbeat, completion/failure/timeout) but these are completely invisible to the export pipeline. There is no workaround.

### Who Is Blocked

| Account | Ops | Why Export | Other Blockers |
|---|---|---|---|
| **Block** | 32.1B | PII detection: export -> S3 -> Wiz scan -> notify owner. Runs on every namespace. | Java SDK, Start Delay |
| **Stripe** | 76.0B | "Joseph explicitly stated they will use Exports for compliance" | Ruby/Java SDK, Start Delay |

**Combined:** 108B ops, two top-10 accounts, both hard-blocked.

## Part 3: Recommendation - Incorporating SAA into Export
### Design Principle
**Extend, don't reinvent.** The export infrastructure (sinks, scheduling, monitoring, APIs) already works. SAA export should be a new execution type flowing through the same pipeline, not a parallel system.

### 3.1 Proto Schema Extension
```protobuf
// temporal/api/export/v1/message.proto (proposed)
syntax = "proto3";
package temporal.api.export.v1;

import "temporal/api/history/v1/message.proto";

message WorkflowExecution {
    temporal.api.history.v1.History history = 1;
}

// NEW: Standalone activity execution export
message ActivityExecution {
    temporal.api.history.v1.History history = 1;
}

// Renamed from WorkflowExecutions to reflect broader scope
message ExportedExecutions {
    repeated WorkflowExecution workflow_items = 1;
    repeated ActivityExecution activity_items = 2;
}

// Preserved for backward compatibility
message WorkflowExecutions {
    repeated WorkflowExecution items = 1;
}
```

### 3.2 SAA Export Format
A standalone activity execution generates a different event sequence than a workflow. Proposed SAA export format:
```
1. ActivityExecutionStarted          (NEW event type)
   - activity_type, task_queue, input, retry_policy, timeouts
   - schedule_to_close_timeout, start_to_close_timeout
   - search_attributes, memo
   - caller identity (who started the SAA)
   - start_delay (if configured)
2. ActivityTaskScheduled
   - Same as workflow-embedded activity scheduling
3. ActivityTaskStarted
   - worker_identity, attempt, current_attempt_scheduled_time
4. [ActivityTaskHeartbeat]*          (0 or more heartbeat events, if recorded)
   - heartbeat_details payload
5. ActivityTaskCompleted | Failed | TimedOut | Canceled | Terminated
   - result/failure/timeout details
6. ActivityExecutionCompleted        (NEW event type)
   - final_status, close_time
   - retry_count (total attempts)
```

### 3.3 Configuration Changes
**Option A (Recommended): Automatic inclusion**
- Export is namespace-scoped. When export is enabled for a namespace, all closed execution types (workflows + SAA) are exported.
- No new configuration knobs. Simplest for customers and consistent with "export everything for compliance."

**Option B: Per-type toggle**
- Add an execution type filter to export sink configuration: `execution_types: [WORKFLOW, ACTIVITY]`
- More granular but adds complexity.

### 3.9 Implementation Sequence
1. **Proto schema** - Add `ActivityExecution` and `ExportedExecutions` to `temporal/api/export/v1/message.proto`
2. **Server/History** - Capture SAA event history in tiered storage (prerequisite for export)
3. **Export pipeline** - Include closed SAA executions in hourly export batch
4. **File format** - Write `ExportedExecutions` for namespaces with SAA activity; continue `WorkflowExecutions` for workflow-only namespaces
5. **Billing** - Meter SAA exports as actions with `execution_type=activity` label
6. **Monitoring** - Add `execution_type` dimension to export metrics
7. **Docs** - Update export docs with SAA event history format and Parquet conversion example
8. **UI** - Show SAA export count alongside workflow export count in namespace dashboard

### 3.10 Success Criteria
- Block can run their PII scanning pipeline (export -> S3 -> Wiz -> notify) on SAA executions identically to workflows
- Stripe can demonstrate compliance audit trail for all SAA executions to their auditors
- Existing workflow export consumers are unaffected (zero breaking changes)
- Export cost for SAA is visible and predictable before customers enable it
