# Buffering & Batching: Raw Source Compendium

*Compiled 2026-03-22. All content verbatim from Slack and Notion with author attributions and source links.*

---

# Table of Contents

1. [Notion Documents](#1-notion-documents)
   1. [Jessica's One-Pager: Buffering + Batching](#11-jessicas-one-pager-buffering--batching)
   2. [Proposal: Buffering — Accept Work Now, Execute Later](#12-proposal-buffering--accept-work-now-execute-later)
   3. [Proposal: Batch Semantics — Manage a Group of Workflows as a Unit](#13-proposal-batch-semantics--manage-a-group-of-workflows-as-a-unit)
   4. [Meeting: March 11, 2025](#14-meeting-march-11-2025)
   5. [Customer Conversations: Burst Starts](#15-customer-conversations-burst-starts)
   6. [Internal Customer Conversation Guide: Burst Starts](#16-internal-customer-conversation-guide-burst-starts)
   7. [COOLCAT Draft One-Pager](#17-coolcat-draft-one-pager)
   8. [Collection Use Cases](#18-collection-use-cases)
   9. [Flow Controls Across Temporal](#19-flow-controls-across-temporal)
   10. [SAA Crew Sync 2026-03-18 (async dedupe relevance)](#110-saa-crew-sync-2026-03-18)
   11. [Roey's Collections / Streams notes](#111-roey-collections-streams)
2. [Slack: #crew-buffering-batching Channel](#2-slack-crew-buffering-batching-channel)
3. [Slack: Stripe Use Case Threads](#3-slack-stripe-use-case-threads)
4. [Slack: Netflix Use Case Thread](#4-slack-netflix-use-case-thread)
5. [Slack: Rippling Batch Use Case](#5-slack-rippling-batch-use-case)
6. [Slack: Tao's Customer Use Case (Standalone Activities)](#6-slack-taos-customer-use-case)
7. [Slack: Async Dedup Thread](#7-slack-async-dedup-thread)
8. [Slack: COOLCAT Design Thread](#8-slack-coolcat-design-thread)
9. [Additional Slack Findings](#9-additional-slack-findings)

---

# 1. Notion Documents

---

## 1.1 Jessica's One-Pager: Buffering + Batching

**Source:** [Notion](https://www.notion.so/temporalio/Buffering-Batching-Handling-Work-Temporal-Can-t-Immediately-Process-31e8fc567738808c91e4d06bf989c581)
**Driver:** Jessica Laughlin | **Status:** 1-pager Draft | **Created:** 2026-03-09

> **Note:** "Batch" is overloaded with Temporal's existing Batch Operations (terminate/cancel/signal on existing workflows). This 1-pager uses "batch" in the industry-standard sense: submitting a large group of Workflows for execution at once. Final naming TBD.

### Problem

Temporal has no way to accept work it can't immediately process. Whether a user starts or signals a large burst of Workflows, hits an unexpected traffic spike, or loses connectivity during an outage, the result is the same: Workflows aren't created, Signals aren't delivered, and the burden of recovery is pushed to the application.

Today, this surfaces in three ways:

1. **Intentional bursts get rejected.** Nightly batch runs, backfills, or fan-outs from an upstream job hit `RESOURCE_EXHAUSTED` limits. 137 accounts (representing 86% of all platform actions; 61% excluding OpenAI) hit this via `StartWorkflow` in the last 30 days.
2. **Traffic spikes have no graceful degradation.** When inbound volume unexpectedly exceeds APS limits, the server rejects requests immediately. The SDK retries with a backoff, but if the spike outlasts the retry window, Workflows will not be created.
3. **Unavailability means lost work.** When Temporal is unreachable, Workflows cannot be created. During a network outage, Block lost ~11,480 Workflow starts that had to be manually recovered.

Without a way to accept work it can't immediately process, Temporal pushes customers toward the same workaround: put a queue between their systems and Temporal. Stripe, OpenAI, and Coupang use Kafka. Netflix uses SQS. Meta built a MySQL workaround. 25+ named customers across all four customer segments are affected.

**Our biggest customers are building infrastructure to work around our infrastructure.** Temporal should encourage customer growth, not resist it.

#### Impact Classification and Strategic Alignment

**Core Loop**: The accounts hitting this ceiling are the center of our target: high-value, high-volume use cases. This removes the workaround tax for those customers and makes the platform resilient to the conditions they already face.

**Revenue tailwind** ($1M+ from existing accounts, plus unlocks data orchestration market):
The 137 throttled accounts generate 86% of all platform actions. Two dynamics:
1. **Eliminating expansion friction.** Accounts at their APS ceiling face an engineering tax to add bursty workloads. Stripe routes every bursty workload through Kafka. OpenAI built custom queueing. That tax slows adoption of new use cases and pushes some workloads to other platforms entirely.
2. **New workload capture.** Prospects evaluating for high-volume use cases (Lululemon designing a Kafka replacement, Finch Legal wanting 5-10x their current 500 TPS) hit burst limits during evaluation.

Conservative estimate: 2% additional actions growth across accounts responsible for ~$60M in actions ARR = $1.2M/year.

**Market expansion prerequisite.** This is also the foundation of the planned Batch primitive. Without burst absorption and batch semantics, Temporal cannot serve data orchestration workloads (scheduled ETL, bulk processing, periodic fan-outs). This is the market where Temporal competes with Airflow, Prefect, and Dagster.

FY27 alignment:
- Objective 2: Sharpen & Scale Use Cases
- Objective 4: Earn Enterprise Credibility

### Solution and Requirements

This document proposes a solution to the first user problem: intentional bursts get rejected. But the implementation must be able to solve the second and third without rearchitecting.

Temporal needs two new capabilities:
1. **Burst absorption.** Persist work on arrival, decoupling acceptance from execution. Buffered durably, drained at a sustainable rate.
2. **Batch semantics.** Submit, track, and manage a group of Workflows as a unit.

#### How it works

Users submit Workflows as a Batch. Temporal accepts them at rates far above what it can execute immediately. Target: acceptance latency competitive with Kafka/SQS (single-digit ms per item). Workflows then start at a platform-controlled rate.

Batches must scale to: 1M (SailPoint), "millions" (Roblox), 500M (OpenAI).

**Supported Batch operations:**
- **Create**: open a Batch, submit Workflow start requests (same params as `StartWorkflow`)
- **Query**: aggregate status (pending, started, completed, failed counts) and per-Workflow status. Failed Workflows queryable with failure reasons.
- **Cancel**: stop pending Workflows from starting, cancel running Workflows
- **Pause/Resume**: stop draining new Workflows from the buffer; running Workflows unaffected

#### Guarantees
1. **Durable acceptance.** Once acknowledged, a Batch survives crashes, restarts, and failovers.
2. **No silent loss.** Every accepted Batch and Workflow reaches a terminal state (completed, failed, or cancelled).
3. **Batch tracking.** Each Batch has a client-provided ID for querying full status.
4. **Cancellation completeness.** Cancel stops pending and cancels already-running.
5. **Idempotent submission.** Retrying with the same Batch ID doesn't recreate the Batch.

#### Out of Scope, But Design for Extensibility
- Reactive overflow buffering
- Unavailability recovery
- Streaming/open-ended submission
- SignalWorkflow and standalone Activity support (Meta needs 10K signals/sec; current Batch Operations cap at 50 RPS)
- User-configurable drain rate (Stripe has requested)

#### Out of Scope
- Mutation of Batches after submission is complete
- Recurring Batches (composition with Schedules)
- Ordering, prioritization, or dependency guarantees between Workflows
- Fire-and-forget delivery tier

### Open Questions

#### Data Gaps

| Data point | Why it matters | What we know |
|---|---|---|
| Burst duration | Seconds vs hours changes buffer/storage requirements | Snap: sustained minutes. OpenAI: "in seconds". Most: unknown. |
| Burst frequency | Daily/weekly/event-driven affects capacity planning | Stripe: weekly (payday). Most: unknown. |
| Latency tolerance | "Start in seconds" vs "start within an hour" are different architectures | No customer has stated this explicitly |
| Processing rate needs | Max execution rate actually needed post-burst | Stripe: "process 100/sec". Only explicit data point. |
| Batch size distribution | Determines API design, default limits, memory/storage design | Upper end known (SailPoint 1M, OpenAI 500M). Typical batch size unknown. |

#### Proposed Defaults
- **Cost model:** Meter starts at execution, not submission. Buffering is platform overhead. Invalid requests not billed.
- **APS consumption**: Batched Workflows consume APS at execution, not submission.
- **Drain rate**: Platform-controlled, not configurable.
- **Batch priority**: Direct (non-Batch) starts take priority over Batch draining.
- **Batch completion**: Complete when all Workflows reach terminal state (including continue-as-new chains and child Workflows).
- **Partial submission**: Accepted Workflows are never lost, even if connection drops mid-submission.

#### Design Questions
1. Extensibility for traffic spikes and unavailability?
2. Architecture tiering (3x vs 1000x+ execution rate)?
3. API shape: streaming, chunked, or repeated RPCs?
4. Submission rate limits?
5. Partial submission recovery?
6. Drain rate mechanics: fixed or variable?
7. Batch priority: explicit priority levels between Batches?
8. Batch TTL for abandoned Batches?
9. Observability surfaces?

#### Business Questions
1. Pricing model: billing event for submission/buffering, or absorbed into action pricing?
2. Submission abuse: if buffering is free, what prevents unbounded invalid submissions?

### Customer Evidence (Appendix)

**Tier 1 — Explicitly asking:** OpenAI (Kafka buffer, 1M+ "in seconds"), Stripe (Kafka buffer, "accept 1000/sec, process 100/sec"), Netflix (SQS for backpressure)

**Tier 2 — Hitting the wall:** Snap (3x APS spikes), Coupang (12K APS, Kafka), Rippling (6x APS spikes, Redis queues), Meta (100Ks of signals, MySQL workaround), Roblox (millions of workflows to signal), Airbnb (throttled on StartWorkflow), Block (Kafka consumer, ~11,480 lost starts during outage), SailPoint (P1 from 1M workflows), Lovable (billing backfill maxed 12K APS), Intuit (bursty starts, RESOURCE_EXHAUSTED), Redo (rate limited, production impact)

**Tier 3 — Adjacent pain:** Relativity, Brex, Q2, Neon, Hebbia, Rivian, Harvey, Twilio, Zapier, Finch Legal, Lululemon

**Community/OSS:** Vaibhav Gupta (20K workflows, OOM), Aaron Redmond (100M signalWithStart), Ivan Bautrukevich (60M workflows/day), Andriy Lupa (1M workflows/day via signalWithStart)

### Inline Comments (27 discussions, 42 total comments)

<details>
<summary>Discussion 1: "Users submit Workflows as a Batch" (4 comments, unresolved)</summary>

**Unknown user** (2026-03-10 17:58):
> does this need to be explicit? iiuc in some case the customer can't predict the burst volume.... that was certainly Snap's use case.

**Jessica Laughlin** (2026-03-11 15:15):
> Good callout - Snap's case is a traffic spike, not a planned batch, so they can't pre-define what to submit. The problem statement now describes three failure modes: intentional bursts, traffic spikes, and unavailability. This phase solves the first (intentional batches with a defined submission), but the infrastructure should extend to reactive cases like Snap's without rearchitecting. Adding streaming/drip submission to Out of Scope to make this explicit.

**Roey Berman** (2026-03-11 17:39):
> Agree this is different from a batch for some of the use cases. The couple of use cases I've seen require a queue to decouple Temporal frontend's rate limiting from producers.

**Sergey Bykov** (2026-03-11 18:55):
> "but the infrastructure should extend to reactive cases like Snap's without rearchitecting." I'm struggling to see this connection.
</details>

<details>
<summary>Discussion 2: "Temporal has no way to accept work it can't immediately process." (3 comments, unresolved)</summary>

**Dan Davison** (2026-03-13 13:25):
> The way I'd phrase this is that Temporal does so much when it accepts work that it can't match the write throughput of Kafka/SQS. I get what the current wording means, but it's a bit confusing because accepting work to be processed later is exactly what Temporal is designed for.
>
> I.e. in Temporal's current implementation, although StartWorkflow is basically just a persistence write and the actual processing is async, we do expensive work on acceptance: e.g. some CPU-bound work in Frontend, an RPC hop to History adding several serialization/deserialization steps, some CPU-bound work in History, 2 WAL writes, etc.
>
> In contrast, to match the write throughput of Kafka/SQS, we'd basically have to (I assume) append to WAL and not much else.

**Yimin Chen** (2026-03-14 00:15):
> This is actually a good point to continue digging. Now we have TQ priority, I could imagine we leverage that feature to mark those ok-to-be-buffered workflows as low priority. Then we really just need to also treat to-be-buffered StartWorkflowExecution API calls as low priority so spike of them won't impact other normal live traffic, then we essentially has a buffer feature.

**Paul Nordstrom** (2026-03-14 17:39):
> Adding an item to a task queue is a hugely more expensive thing compared to appending to the WAL. so I believe Dan's approach is much more realistic if our goal is to get anywhere near the processing cost of say Kafka (which of course it absolutely is). CHASM's very raison d'etre is to make these sorts of things efficient. Whether via implementation of a native queue CAT (e.g. OPFQ) or through capture of operations in (say) something like COOLCAT, we can outperform task queues by an order of magnitude.
</details>

<details>
<summary>Discussion 3: "Ordering, prioritization, or dependency guarantees" (3 comments, unresolved)</summary>

**Unknown user** (2026-03-10 13:32):
> even if we're not changing anything, it would be worth understanding how this interacts with priority + fairness, right? eg, do the ergonomics allow someone to submit a batch as essentially "work on this when not pre-empted by something else". maybe this is already implicit?

**Jessica Laughlin** (2026-03-10 14:31):
> yes, great question! in the "Proposed Defaults" section, I'm claiming that this should be our default behavior: "work on this when not pre-empted by something else". I can make this bullet clearer.

**Roey Berman** (2026-03-11 17:42):
> 100% this should interact with priority and fairness. I don't think we can make this out of scope at this point.
</details>

<details>
<summary>Discussion 4: "Meter starts at execution, not submission." (3 comments, unresolved)</summary>

**Yimin Chen** (2026-03-10 02:32):
> Currently, we only charge for successful start. Failed StartWorkflowExecution API call is not charged. We have to be careful about edge case where user submit 100M invalid start workflow requests.

**Jessica Laughlin** (2026-03-10 14:36):
> added open business question below from this, thank you!

**Roey Berman** (2026-03-11 17:46):
> Agree with Yimin, and don't fully understand the proposal here.
</details>

<details>
<summary>Discussion 5: Revenue Potential (2 comments, unresolved)</summary>

**Unknown user** (2026-03-10 13:27):
> seems low - rough framing of revenue on order of $100k over 3 years for 12+ person months of eng. understandably a feature where revenue attribution is hard, but over 3 years $1m is on the order of 0.1% of total revenue - that doesn't seem crazy at all, does it?

**Jessica Laughlin** (2026-03-10 14:19):
> great question! I updated the revenue potential to $1M+ ARR in next three years, and added my reasoning to the impact classification section.
</details>

<details>
<summary>Discussion 6: APS consumption / rate limits (2 comments, unresolved)</summary>

**Yimin Chen** (2026-03-10 02:33):
> Will it have a separate rate limit?

**Unknown user** (2026-03-10 05:48):
> second this. Will these be standard primitives or live elsewhere (either from the product perspective or purely limit perspective)
</details>

<details>
<summary>Discussion 7: "Drain rate: platform-controlled, not configurable" (2 comments, unresolved)</summary>

**Unknown user** (2026-03-10 05:50):
> how will we control this rate? Is it fixed, or variable? Will there be different iterations based on if customers want their batch workflows completed in minutes vs hours vs days?

**Jessica Laughlin** (2026-03-10 15:03):
> good questions! I dropped into design open questions.
</details>

<details>
<summary>Discussion 8: Pricing model (2 comments, unresolved)</summary>

**Unknown user** (2026-03-10 05:52):
> Once we have the shape of the solution we will need to evaluate. The ideal goal would be to reduce the cost of batch workflows by running them more efficiently. This can either come from more efficient queue/architecture or by allowing us to spread out rate limit spikes that force the use of TRUs.

**Jessica Laughlin** (2026-03-10 14:45):
> okay great, sounds good to me.
</details>

<details>
<summary>Discussion 9: "Traffic spikes have no graceful degradation" (1 comment, unresolved)</summary>

**Unknown user** (2026-03-12 00:53):
> For traffic spikes, is the intended extensibility model a reactive overflow buffer (server automatically starts absorbing when capacity is exceeded, transparent to the caller), or does this also require callers to opt into a buffered submission mode? The architecture implications are quite different.
</details>

<details>
<summary>Discussion 10: "During a network outage" (1 comment, unresolved)</summary>

**Unknown user** (2026-03-13 18:55):
> I get that we're saying, "durability of a batch of work shouldn't be coupled to our availability", but to me, this reads more as if we're intending to solve the problem of availability at *ingestion* time, which I don't believe we are (e.g., by integrating into the customer's own network, like their Kafka queueing solutions).
</details>

<details>
<summary>Discussion 11: "Our biggest customers are building infrastructure..." (1 comment, unresolved)</summary>

**Jessica Laughlin** (2026-03-11 14:47):
> saving comment from unknown user: "huge anti-patterns we should both eliminate the need for and make sure users know is not necessary. probably covered later, but this emphasizes the value of GTM and having a best-practices reference available to devs (and coding agents)"
</details>

<details>
<summary>Discussion 12: "workload through Kafka. OpenAI built custom queueing." (1 comment, unresolved)</summary>

**Unknown user** (2026-03-12 23:40):
> What percentage of the use-cases are bulk ETL style workloads vs steady stream with occasional bursts?
</details>

<details>
<summary>Discussion 13: "This is also the foundation of the planned Batch primitive." (1 comment, unresolved)</summary>

**Sergey Bykov** (2026-03-11 18:50):
> I don't understand this statement. To me, batch and burst are two different cases. I don't think we should conflate them.
</details>

<details>
<summary>Discussion 14: "Supported Batch operations:" (1 comment, unresolved)</summary>

**Roey Berman** (2026-03-11 17:43):
> What about flow control (e.g. rate limiting)?
</details>

<details>
<summary>Discussion 15: "Create" (1 comment, unresolved)</summary>

**Roey Berman** (2026-03-11 17:40):
> Does it need to be an explicit `create`? Why not `add` and we create it if it doesn't exist?
</details>

<details>
<summary>Discussion 16: "StartWorkflow" (1 comment, unresolved)</summary>

**Roey Berman** (2026-03-11 17:41):
> This is just one type of action that users may want to submit. There's also `StartActivityExecution` `SignalWorkflowExecution` and many more.
</details>

<details>
<summary>Discussion 17: "Query" (1 comment, unresolved)</summary>

**Roey Berman** (2026-03-11 17:41):
> Is there a specific retention that you are considering here for full failures or results?
</details>

<details>
<summary>Discussion 18: "Failed Workflows queryable with failure reasons" (1 comment, unresolved)</summary>

**Unknown user** (2026-03-13 19:00):
> It'd be great to support some sort of convenient retry mechanism here, either retrying as part of the existing batch request, or some sort of, "query and retry the returned results in a new batch" semantic.
</details>

<details>
<summary>Discussion 19: "Reactive overflow buffering" + "Unavailability recovery" (1 comment, unresolved)</summary>

**Sergey Bykov** (2026-03-11 18:53):
> Why are these out of scope? Above we say, "Temporal needs two new capabilities" but we only propose one of them in the end.
</details>

<details>
<summary>Discussion 20: "SignalWorkflow and standalone Activity support" (1 comment, unresolved)</summary>

**Dan Davison** (2026-03-13 13:28):
> I.e. a single batch could contain a heterogeneous mix of execution types, right?
</details>

<details>
<summary>Discussion 21: "Latency tolerance" data gap (1 comment, unresolved)</summary>

**Unknown user** (2026-03-12 22:21):
> Would be useful to gather data on: 1. Acceptable processing submission to start execution latency tolerance. 2. Max acceptable time before considering it a timeout style failure.
</details>

<details>
<summary>Discussion 22: Proposed Defaults (1 comment, unresolved)</summary>

**Alex Stanfield** (2026-03-15 00:14):
> If we allow: 1. append to collection 2. optionally process events in order -- This would enable event processing where ordering is important. Example: Consume from a kafka topic, for each message kick off a collection with a single SAA. If there is an ID conflict append to the collection. This could allow events to be processed in order.
</details>

<details>
<summary>Discussion 23: "Buffering is platform overhead" (1 comment, unresolved)</summary>

**Unknown user** (2026-03-12 22:26):
> This can quickly become expensive for large ETL style workloads, why not keep the cost model on-par but a percentage of their current Kafka usage?
</details>

<details>
<summary>Discussion 24: "Direct (non-Batch) starts take priority over Batch draining." (1 comment, unresolved)</summary>

**Unknown user** (2026-03-13 19:06):
> Should we reuse matching fairness/priority queueing for this, versus something batch-specific? If so, and I think that we should, the question becomes, "do we make that explicit to the customer", which I think we also likely would want to.
</details>

<details>
<summary>Discussion 25: "Batch completion" (1 comment, unresolved)</summary>

**Jessica Laughlin** (2026-03-10 14:35):
> @Yimin Chen updated this -- is it clearer?
</details>

<details>
<summary>Discussion 26: "Submission abuse" (1 comment, unresolved)</summary>

**Jessica Laughlin** (2026-03-10 14:36):
> @Yimin Chen -- added this question per your comment above, thank you!
</details>

<details>
<summary>Discussion 27: OpenAI Slack link (1 comment, unresolved)</summary>

**Dan Davison** (2026-03-22 12:53):
> This Slack link is going to a Snap thread
</details>

---

## 1.2 Proposal: Buffering — Accept Work Now, Execute Later

**Source:** [Notion](https://www.notion.so/temporalio/3248fc5677388157b2cbc4def089146d)
**Parent:** Buffering + Batching: Handling Work Temporal Can't Immediately Process

**Start millions of Workflows without building a queue in front of Temporal.**

### The problem

When you need to start millions of Workflows from a background job, Temporal rejects what it can't immediately process. So you put a queue in front of Temporal and build the retry, pagination, backoff, and reconciliation yourself.

> In our Feb 2026 sync, your team described pumping millions of records into Kafka just to pace Workflow starts - building and maintaining a consumer that Temporal should make unnecessary.

### How it would work

Temporal accepts every Workflow start request on arrival and starts Workflows as capacity allows.

### Walkthrough: a background backfill job

| Step | Today | With buffering |
|------|-------|----------------|
| 1. Your job scans the database, finds 2M records that need to turn into Workflows | Same | Same |
| 2. Your job sends 2M Workflow starts | Build a Kafka consumer to dequeue, pace, and retry. You own this infrastructure. | Temporal accepts each item immediately and persists it. Drains at sustainable rate behind the scenes. |
| 3. 50 Workflows fail to start (duplicate Workflow ID, invalid arguments) | Failures may surface in consumer logs, DLQ, or silently disappear depending on your retry logic. | Temporal marks each as failed with a reason. No silent loss. |
| 4. Job is done. Did all 2M Workflows execute? | You diff your database against Temporal to find gaps. Some may have been silently dropped by your consumer. | Temporal accepted all 2M durably. Any that couldn't start are marked failed with a reason. You monitor Workflow completion the same way you do today. |

### The contract

**We guarantee:**
- **Durable acceptance.** Survives crashes and failovers.
- **No silent loss.** Every request either starts or fails with a reason.
- **Acceptance speed.** Acceptance latency competitive with Kafka/SQS.
- **Non-disruptive.** Direct starts are never slowed by buffered work draining. Buffered Workflows start as capacity becomes available.

**We don't guarantee:**
- **Start latency.** Workflows start when capacity is available, not on a deadline.
- **User-controlled drain rate** in v1.
- **Unlimited buffer size.** Platform limits apply.

**Needs design:** How you check on buffered Workflows before they start. Today, a started Workflow is a running Workflow - buffering adds a new state in between.

### What this doesn't do

- No way to manage a group of Workflows as a unit (query progress, cancel, pause). Buffering handles individual Workflow starts, not groups.
- No dependencies between items. One buffered Workflow can't wait for another to complete.
- No guaranteed start order. Buffered Workflows may start in any order, regardless of acceptance order.
- Not a job scheduler. This doesn't decide when to start work - your application triggers it.

### Best fit when

1. The Kafka/queue overhead is the sharpest pain - more than the lack of lifecycle management for groups of Workflows.
2. You would use this for new workloads going forward. Some existing Kafka-buffered workloads might migrate too.
3. "Backfill when idle" (buffered Workflows drain behind direct traffic) is acceptable for most use cases.
4. Submission speed matters - if acceptance latency were 100ms+ per item, this wouldn't be competitive with Kafka.

### Specific feedback we're looking for

1. Your highest-volume job today: how many Workflows per burst, how long does the burst last, how often does it run?
2. What acceptance latency per item do you need? Kafka/SQS return in single-digit ms.
3. What latency between acceptance and first Workflow execution is acceptable?
4. Buffered Workflows drain behind direct StartWorkflow calls. Does that work, or do some workloads need to drain faster?
5. Which workloads would use this first? Would you migrate existing Kafka-buffered workloads?
6. Where does this rank against everything else you're solving with Temporal?
7. Buffered Workflows sit in an "accepted but not yet started" state. How would you want to check on them?

**Comments:** None.

---

## 1.3 Proposal: Batch Semantics — Manage a Group of Workflows as a Unit

**Source:** [Notion](https://www.notion.so/temporalio/3248fc56773881e490d9c7c5eac429b2)
**Parent:** Buffering + Batching: Handling Work Temporal Can't Immediately Process

**Manage millions of Workflows as a group without building your own tracking infrastructure.**

### The problem

When you start thousands or millions of Workflows from a single job, there's no way to manage them as a group. You can't ask "is this batch done?" or "cancel everything I just started." So you build tracking and reconciliation yourself.

> In our Feb 2026 sync, your team described needing a batch ID to track groups of Workflows - especially for use cases like external partner data where records arrive in bulk and need to be managed as a unit.

### How it would work

Temporal tracks a batch of Workflow starts as a single unit with a shared lifecycle.

### Walkthrough: an external data ingestion job

| Step | Today | With batch semantics |
|---|---|---|
| 1. External partner drops a file with 500K records that each need a Workflow | Same | Same |
| 2. Your job sends 500K Workflow starts | Call StartWorkflow in a loop. Track IDs yourself (database table, Redis set, etc.). | Add Workflow starts to a batch. Temporal tracks membership. You still pace to stay within rate limits. |
| 3. You need to cancel the batch | Query your tracking store for all IDs you recorded. Loop through and cancel each. Any you missed keep running. | Cancel the batch. Temporal cancels pending and running Workflows in the batch. |
| 4. Job is done. Did all 500K Workflows execute? What failed? | Poll your tracking store. Diff against Temporal. Investigate failures individually. | Query the batch. Temporal returns counts by status. Drill into failed items - each has a reason. |

### The contract

**We guarantee:**
- **Batch tracking.** Every Workflow submitted to a batch is tracked. Query returns full accounting: completed, failed, cancelled, running, pending.
- **Batch completion.** When Temporal reports a batch as complete, every Workflow in the batch has finished.
- **Cancellation.** Cancel stops pending items and cancels running Workflows in the batch.
- **Idempotent submission.** Client-provided batch ID. No double-create.

**We don't guarantee:**
- **Batch-level retry policy.** Individual Workflow retry policies still apply.
- **Unlimited batch size.** Platform limits apply.
- **Fire-and-forget (untracked) submission** in v1. Every item is tracked until it finishes.

### What this doesn't do

- No burst absorption or queueing. If you submit faster than Temporal can process, you still hit rate limits.
- No dependencies between items within a batch.
- No guaranteed processing order.
- No cross-batch dependencies.
- Not a job scheduler.

### Best fit when

1. The reconciliation overhead is the sharpest pain.
2. You need to cancel or query groups of Workflows regularly.
3. "Is this batch done?" is a question you currently answer with custom infrastructure.
4. You have multiple use cases that submit groups of Workflows and each has its own tracking logic.

### Specific feedback we're looking for

1. How large are your batches and how many run concurrently per namespace?
2. Do you submit all items upfront or stream them in over time?
3. If you cancel a batch mid-flight, how fast do you need running Workflows to stop?
4. At your batch sizes, do you need to query individual items, or are aggregate counts enough?
5. How do you want to know a batch is done — poll, webhook, or something else?
6. Which use case would you batch first? What does your tracking infrastructure look like today?
7. Where does this rank against everything else you're solving with Temporal?

**Comments:** None.

---

## 1.4 Meeting: March 11, 2025

**Source:** [Notion](https://www.notion.so/temporalio/Meeting-March-11-2025-3208fc56773880129d8cef1138cacc78)
**Location:** Buffering and Batching > Workstream Home Pages > Product

### Attendees
Jessica Laughlin, Maxim Fateev, Paul Nordstrom, Sergey Bykov, Yimin Chen

### Goal
Align on the problem statement outlined in Jessica's one-pager.

### Next Steps
- Group is aligned on solving this problem at a high level, but would like more detail on the actual V1 solution shape before hopping into engineering design.
- Jessica to clearly sketch out the exact contract/guarantees OpenAI requires for their first (and additional) use cases.
- From there:
  - Additional Engineering discussions about design options.
  - Jessica will identify group of 3-5 customers that will have a problem solved by our approach to OpenAI's use case to bring them in as design partners.

### Discussion Points (paraphrased)

**Sergey:** Concerned that buffering + batching are getting conflated into a single problem in the doc.
- **Jessica:** Definitely two problems. Put in one doc because we'll need to solve a sliver of both for the OpenAI use case, but could make that clearer.
- **Yimin:** There are at least two different scenarios described in the docs. Bursts may need different behavior but same StartWorkflow API, backfills/other types of work will need new API.

**Max:** Can we just give OpenAI an iterator to solve the intentional batch problem? Could just be an Activity call of "give me the next batch." Easiest engineering solution, and we need flow control consumption anyway.
- **Jessica:** Would this eliminate their need for Kafka, though?
- **Max:** Depends on their current architecture.

**Max/Paul:** We should be pressing harder for what their ideal interface would be to really understand more about their use case -- where the data resides, who owns it, the exact process they are going through. We need to dig deeper on these calls.
- **Yimin:** Is the best UX here some version of the StartWorkflow API, but magic? Not clear.

**Max:** Another dimension we can explore for OpenAI use case -- relaxing constraints around latency and durability for batch processing. Can make things cheaper or lower latency, optionally. Let user choose between tradeoffs (like throughput v. latency).

**Comments:** None.

---

## 1.5 Customer Conversations: Burst Starts

**Source:** [Notion](https://www.notion.so/temporalio/3258fc567738819baa39e424d6fc6316)
**Parent:** Buffering + Batching: Handling Work Temporal Can't Immediately Process

### Problem
Customers are starting more workflows than Temporal can immediately process.

**Which solution depends on what specifically hurts:**
- "Temporal rejects my starts" → **Flow control.** Temporal paces starts instead of rejecting them. Customer keeps their queue.
- "I had to build a queue I don't want" → **Buffering.** Temporal accepts all starts instantly and tracks each one. No queue needed.
- "I can't track or manage a group of workflows" → **Batching.** Temporal manages a group of workflows as a unit.

### Customer Conversations Database

#### OpenAI
- **Pain point:** Temporal rejects my starts, Had to build a queue I don't want
- **Pain level:** Work around
- **Design partner:** Yes
- **Customer said:** Said they want buffering. Reduces "the burden of starting a Workflow." Currently rate-limiting internal teams on Temporal usage.
- **Temporal read:** Flow control may be enough. Keeping Kafka regardless (other consumers upstream). Main value is reducing consumer complexity, not replacing the queue.
- **Desired outcome:** Internal teams can use Temporal without rate limiting. Kafka consumer code gets simpler.

#### Twilio
- **Pain point:** Temporal rejects my starts, Can't control which starts get throttled
- **Pain level:** Work around
- **Customer said:** Built SQS in front of Temporal to control ingress rate. Want per-tenant rate limiting on StartWorkflow - reject abusive tenants, backpressure upstream. Also need dispatch throttling for downstream APIs (thundering herd). Using fairness today for task dispatch, working well.
- **Temporal read:** Flow control (intake + dispatch). Not buffering - they want rejection, not queuing. Dispatch side is Brandon/Roey scope.
- **Desired outcome:** Per-tenant rate limiting on StartWorkflow ingress. Reject above threshold. Dedicated APS allocation for ingress vs processing.

#### Meta
- **Pain point:** Can't track or manage a group of workflows
- **Customer said:** Need to Signal millions of Workflows to upgrade versions, want the whole process done in ~15 minutes
- **Temporal read:** Batching - need to operate on a group of Workflows as a unit. Current Batch Op bottlenecks on serial Visibility (ES) listing. With 1B retained Workflows, list latency degrades to seconds/page. Workaround: run concurrent Batch Ops with sharded queries.
- **Desired outcome:** Signal 1-2M running Workflows for version upgrades in <15min (~10k signals/sec)

### Customers to check in with

| Customer | What we know | What's missing |
|----------|-------------|----------------|
| Stripe | Kafka buffer for bursty starts, "accept 1000/sec, process 100/sec" | Pain level, desired outcome, would flow control be enough? |
| Snap | 3x APS spikes, repeated RESOURCE_EXHAUSTED | Do they have a workaround? How much does it hurt? |
| SailPoint | P1 from starting 1M workflows | What they built, desired outcome |
| Redo | Rate limited on starts, production impact | What they built, desired outcome |
| Airbnb | Throttled on StartWorkflow | Everything else |
| Lovable | Billing backfill maxed 12K APS | Was it a real problem or just a note? |
| Intuit | Bursty starts, RESOURCE_EXHAUSTED | Pain level, workaround |
| Rippling | 6x APS spikes, Redis queues | Was Redis built for Temporal specifically? |
| Coupang | 12K APS, Kafka buffer | "Coordinated events" - starts or signals? |

**Comments:** None.

---

## 1.6 Internal Customer Conversation Guide: Burst Starts

**Source:** [Notion](https://www.notion.so/temporalio/3258fc567738811797c3fbbb1ebadc4b)
**Parent:** Buffering + Batching: Handling Work Temporal Can't Immediately Process

**Private prep doc. Do not share externally.**

#### Opening
"We're exploring how Temporal should handle high-volume Workflow workloads. We want to understand what's working and what's not."

#### Questions — Today
1. What happens when you need to start a large number of workflows?
2. Did you have to build anything to make that work? Which part is most painful?

#### Questions — What would help
1. Where does the burst come from? A database scan, a queue like Kafka, an API call, a cron job?
2. If your process crashes mid-burst, what happens today? How do you recover the ones that didn't get started?
3. If Temporal controlled the pace of your starts without rejecting them - but you still managed your own queue/file system/database - would that solve it?
4. If Temporal accepted all your starts instantly and tracked each one's status, but you still managed "is the whole job done" yourself - does that solve it?
5. Do you ever need to cancel, pause, or check progress on an entire group as a unit?

#### Questions — Shape
1. How many workflows per burst? How often?
2. If we solved this, what would you stop building or maintaining?
3. What latency between acceptance and first execution is acceptable?

#### Questions — Severity + priority
1. Is this something you work around, or is it limiting what you can do with Temporal?
2. How often does this bite you? Has it caused an incident or blocked a new use case?
3. Where does this rank against everything else you're solving with Temporal?

#### Listen for (don't ask)
- "Need to know when they're all done" = batching
- "Just need Temporal to stop rejecting" = buffering or intake flow control
- "We have Kafka/SQS/db and that's fine, just stop throttling us" = intake flow control
- Signal/signalWithStart pain = adjacent scope
- Child workflow fan-out = different pattern
- "The problem is what happens after they start" = dispatch flow control (Brandon/Roey's team)

<details>
<summary>Comments (1 resolved discussion)</summary>

On the word "queue" in question 3 under "What would help":

**Maxim Fateev** (2026-03-16 19:47):
> It can be something else. File/DB/etc

**Jessica Laughlin** (2026-03-16 20:26):
> great point, fixed!

*(Discussion resolved. Page text updated to read "your own queue/file system/database")*
</details>

---

## 1.7 COOLCAT Draft One-Pager

**Source:** [Notion](https://www.notion.so/temporalio/Operation-Collection-CAT-COOLCAT-Collection-Of-Operation-Locators-2508fc56773880e592fcc253b1d0c282)
**Status:** 1-pager Draft | **Created:** 2025-08-15

### Overview

The Operation Collection (OC) is a CAT (Chasm ArcheType) component designed to manage and/or execute a large-scale set of Temporal operations in bulk. An OC instance tracks Operation Elements (OEs) -- each representing a discrete action such as starting a workflow, sending a signal, executing a standalone activity, invoking a Nexus operation, or any other abstract Temporal operation. It provides a centralized orchestration layer for bulk operations, with APIs to create, control, query, and scale execution from small batches to billions of items.

Some specific use cases:
1. Buffering of (say) signals for high-volume operation bursts
2. Large batches of operations of any kind that exceed the event limit of workflows
3. Load capture and replay for testing
4. Too many more to list

While "performing" a batch of operations is a very powerful tool, even that may be subsumed by the value of simply *tracking* a large collection of operations. Because this component will (of course) track the progress of, and statistics for the collection -- *whether or not it triggered the operations.*

So, you can construct an empty OC, and allow orthogonally run operations to join it, allowing you to accumulate statistics about whatever "group" of operations you care about. This is the "passive mode" for an OC. It enables "slicing and dicing" of (say) workflows by whatever dimensions the customer wants. You could have a collection of all the workflows run on behalf of a single customer (i.e. an OC for each customer) that would gather statistics about just that customer. And that data can be both system-level information, but also *business data*. The OC could gather summary payloads from each WF run and feed that back to an analyzer, billing collector, etc.

### Problem Statement
- Current Temporal APIs focus on individual workflow/activity invocations, making large-scale batch orchestration cumbersome.
- No unified mechanism for filtered bulk control (pause, cancel, throttle) across heterogeneous Temporal operations.
- Some customer scenarios require a central control point, advanced filters, and extreme scalability.

### Customer Feedback
- Speculative/internal: Internal needs for signal collection propagation and batch management.
- Potential customer interest in high-volume batch workflow start and monitoring.

### Use Cases
- Signal Collection Propagation -- history engine distributes to child workflows.
- Batch Manager Application -- control large sets of workflows/signals. Continue-as-New signal handoff.
- Worker Heartbeat Processing.
- WALKER Checkpoint Processing.
- KEP (Knowledge Engineering and Processing) -- Thought Experiments for various AI use cases
- Durable-on-admitted Workflow Update
- Fanout

### Developer Experience

```go
oc := temporal.NewOperationCollection(
    opts{concurrencyLimit: 100},
    operations{activity1, activity2, activity3, ...},
    subscription{OnCompletion, myCallback},
)
oc.Start()
```

### Scope & Key Features
Core: billions of items, sync/async OEs, canonical filters, recursive operations, retries, idempotency.
Control (global/filtered): Start, Pause, Cancel, Query.
State Model: Created → Started → Canceled/Completed/Failed, with subscriptions to OC/OE events.
N.B. Since an OC is itself an OE, an entire OC can be executed as the target of a Temporal Schedule!

### What's it good for?
1. Besides its internal uses:
   1. It's a poor-man's Batch toolkit (or the v0 of a real one)
   2. Sentinel
   3. Useful as a component in many known use cases (Stripe, Twilio, Block)
   4. It's so fundamental it's hard to imagine all the possibilities: job creation and management (throttling, concurrency management, cancellation, observation, etc.); critical monitoring and analysis we have never been able to accomplish (note, NOT limited to jobs created through CC)

### Dependencies
- Core CHASM: "Coperations" (abstraction of an Operation), raw "Collection" primitive, State machine subscription mechanism (in Workflow, at least), Semaphore for some features
- Nexus: "ION" (Instance-Oriented Nexus operations)

### Launch Plan
- Phase 1: Prototype API
- Phase 2: Integrate with Temporal for internal use
- Phase 3: Performance tuning, testing
- Phase 4: Public Preview
- Phase 5: GA

**Comments:** None.

---

## 1.8 Collection Use Cases

**Source:** [Notion](https://www.notion.so/temporalio/2b18fc567738805e9654d6e3a45b1a36)

### Next steps
- Inventory use cases + opportunities (Paul to share, Ben to follow up with Drew re: customers)
- Create RFC / proposal (Paul, Roey, Lina, Keith to sketch out high level direction + implementation)
- Get RFC feedback from target customers

### Related docs
- [Paul's COOLCAT doc](https://www.notion.so/temporalio/2508fc56773880e592fcc253b1d0c282)
- Roey's doc (page `2a38fc567738807a9647f19361b03aee`) — inbox pattern

### Customer use cases

**Deutsche Bank** — Clearing use case. 1M+ transactions. All must pass before any items make progress. Variable conditions depending on item data (human review for $1B+ txn).

**Rippling** — Buffer, standalone activities in sequence, maybe priority and fairness.

**Israeli Ministry of Defense** — Data pipeline challenges, exploring Temporal as solution. POC stage. Looking for advice on optimizing workflows, handling batch processing, and scaling.

**Roblox** — Want support for batching individual workflows automatically without rewriting everything. Client side batching (e.g. Batch Orchestra) is cost prohibitive: ideally 10x cheaper.

**Snyk** — Main use case: fan-out batch job to run security scans on customer repos, daily. Fans out to per-customer then per-repo workflows.

**Square** — Batching becomes super efficient: hundreds/thousands of transactions per minute consolidated into one workflow per batch instead of per-transaction.

**Instant Labs** — Submit batches of tasks; a payload is a batch of work that the next transition can fan out.

**TrustLayer** — Document processing: splitting documents into pages, starting a workflow per page. Large documents generate many workflows at once.

**Qlik** — Want to batch up 100 or 1,000 at a time.

**SixMap** — Trying to chunk/batch work, consolidate into a unit of work.

**Mem Labs** — Wondering if 25 users sending different IDs can be batched together into one workflow.

**Vobile** — Instead of scheduling 1K processes, schedule in batches of hundreds containing 10-20 object IDs each for the crawler.

**Comments:** None.

---

## 1.9 Flow Controls Across Temporal

**Source:** [Notion](https://www.notion.so/temporalio/3118fc56773880d0ab95c00557b23b2f)

### Goal

Think about flow controls (rate limit, throttle, concurrency) across Temporal holistically so that users can reason about how they compose.

### Terminology (from Inngest)
- **Rate-limiting** — before work is queued (pre-task-dispatch): protects your system from getting overrun; reject/resource exhausted shunting of traffic BEFORE it gets queued
- **Throttling** — task dispatch: queue/accept work but then throttle task dispatch; protect underlying APIs/DBs
- **Concurrency** — task dispatch: limit max concurrent use of some finite resource

### Matrix

#### Namespace
- **Rate limiting:** Exists, not customer settable. APS (default 500, auto-scales on 7-day usage), RPS (default 2000), OPS (default 4000). When exceeded: priority-based rejection, ResourceExhausted gRPC error, SDK auto-retries. Customer settable capacity only: Provisioned Capacity (2-12 TRUs) or On-Demand.
- **Throttling:** No namespace-level dispatch throttling today.
- **Concurrency:** Exists, not customer settable. 20K concurrent pollers per NS, per-WF limits (2000 pending Activities, 10 in-flight Updates, etc.).

#### Nexus Endpoint
- **Rate limiting:** Exists, not customer settable. Nexus requests count toward NS RPS in both caller and handler NS. Proposed: per-caller NS fixed RPS limits (Phase 1).
- **Throttling:** Exists, not customer settable. Internal server mechanism (rate limiter, concurrency limiter, circuit breaker).
- **Concurrency:** Exists, not customer settable. 30 in-flight Nexus Ops per WF, 100 endpoints per account.

#### Task Queue
- **Rate limiting:** System-level only (matching.rps default 1200/host).
- **Throttling:** Exists, customer settable. `maxTaskQueueActivitiesPerSecond`, `queue-rps-limit`, `fairness-key-rps-limit-default`.
- **Concurrency:** Per-worker only. `maxConcurrentActivityExecutionSize` (default 200), `maxConcurrentWorkflowTaskExecutionSize` (default 200).

### Identity Propagation Gaps

| Layer | Identifier available | Gap |
|---|---|---|
| Namespace Rate Limit | Caller NS, API key / mTLS cert | No per-principal rate limit |
| Nexus Endpoint | Caller namespace ID | Proposed: per-caller NS RPS |
| TQ Throttling | Fairness key (user-defined, max 64 bytes) | No automatic link from Nexus caller to fairness key |
| Worker Concurrency | N/A (per-worker process) | Not coordinated across workers |

**Comments:** None.

---

## 1.10 SAA Crew Sync 2026-03-18

**Source:** [Notion](https://www.notion.so/temporalio/3278fc567738806db01cc37596f2c5ac)
**Attendees:** Dan, Andrew, Ben, Paul, Fred, Rory, Sean

*Only the buffering/batching-relevant sections are included here.*

### Section 8: GA and Post-GA Feature Priorities

**Post-GA / Under Consideration:** Schedules, Nexus integration, Export, **Batch operations**, **Async dedupe**, Migrate workflow activities to CHASM.

> "Async dedupe" proposes accepting activity start requests without checking for ID conflicts (skip the read). This is described as "adjacent to the buffering/batching one-pager."

<details>
<summary>Comments (2 discussions, both unresolved)</summary>

On "Start Delay" (GA Features):
**Unknown user** (2026-03-19):
> This is initially committed for post GA, are we planning to move it up?

On "Once CHASM schedules exist" (Post-GA):
**Unknown user** (2026-03-18):
> We should prioritize development sooner on this and not wait for Schedules to be fully rolled out.
</details>

---


## 1.11 Roey collections-streams

**Source:** [Notion](https://www.notion.so/temporalio/2a38fc5677388014a7a1db72c2d02775?v=2a38fc56773880db8be9000cb2c8aefc&p=2a38fc567738807a9647f19361b03aee&pm=s)

A primitive for accumulating arbitrary data.
A collection and stream have a lot in common in the underlying implementation but may differ in the user facing API. We may want to build this as a single user-exposed primitive or multiple different ones.
There are different flavors of this primitive for different use cases and it’s unclear without further design to tell where the implementation would go.
From the interface perspective, the superset of functionality for a collection (array/list) and stream consists of the following methods (assuming the primitive is immutable and does not provide dedupe semantics):
• Read items (from, to)
  ◦ + paginate from cursor
• Append items
• Poll from cursor
• Trim head
• etc…
Key open implementation questions are what capacity and throughput is this built for? Does this primitive need to be partitioned across shards? How does this interact with different flow control (priority / fairness / RL / semaphore / circuit breaker)? How does the execution and shard lock affect throughput?

Unlocks:
- Inbox (e.g. durable admitted updates)
- Batch
- CHASM workflow port
- AI streaming
- AI conversation history
- Nexus progress notifications
- Action buffer

Notes:
- Action buffer: Run actions with concurrency of N (workflow, activity, nexus op), buffering future requests until the buffer limit is exceeded (potentially also cycling out the old requests).
- Durable-on-admitted update: Ensure that updates are durably held before allowing update to be accepted/executed (requires inbox).
- Multi-stage nexus operations: Start an Operation, return incremental progress for each stage.
Relevant for MCP integration with Nexus, as well as improving the experience for a variety of use cases.
[Nexus Multi-Stage Ops](https://www.notion.so/Nexus-Multi-Stage-Ops-1758fc56773880619c06c760fc18a43f?pvs=21)

---

# 2. Slack: #crew-buffering-batching Channel

**Source:** [#crew-buffering-batching](https://temporaltechnologies.slack.com/archives/C0AKXKW1H53) (Channel ID: C0AKXKW1H53)
**Created:** 2026-03-09

### Members joined (2026-03-09 through 2026-03-18)
Jessica Laughlin, Maxim Fateev, Tushar Roy, Paul Nordstrom, Yimin Chen, Sergey Bykov, Collin Cook, Taylor Khan, Brandon Chavis, Ben Echols, Harani Reddy Mukkala, Dan Davison, Preeti Somal, Alfred Landrum, Sean Kane, Alex Stanfield

---

**Tushar Roy** (2026-03-09):
> @Jessica Laughlin summary of the meeting?

> **Jessica Laughlin:** will write one up and share!

---

**Jessica Laughlin** (2026-03-09):
> dropped summary notes [here](https://www.notion.so/temporalio/Meeting-March-11-2025-3208fc56773880129d8cef1138cacc78). for folks who were in the meeting, feel free to update or add anything I missed.
>
> the first question to answer (posed by @Maxim Fateev) is if an iterator could solve this use case faster. from chatting with @Tushar Roy, the answer to this is:
> - yes, but they don't really need Temporal's help to do that
> - that doesn't address the other related use cases

---

**Maxim Fateev** (2026-03-09):
> They need Temporal help if we implemented back pressure

<details>
<summary>Thread</summary>

**Tushar Roy:** Explain this a little bit more, please
</details>

---

**Tushar Roy** (2026-03-10):
> @Jessica Laughlin lets sync tomorrow. I sent an invite. I caught up Paul/Sergey in my 1:1 regarding buffering and also saw your notes. Lets make sure you and I are aligned on various proposals out there so we can present it properly to OAI.

---

**Jessica Laughlin** (2026-03-10):
> FYI, @Paul Nordstrom and I are meeting with Taylor on Monday to chat through Rippling's batch use case. if anyone else wants to join, let me know!
>
> https://temporaltechnologies.slack.com/archives/C054VD4BZFD/p1773277061358009

<details>
<summary>Thread</summary>

**Paul Nordstrom:** yes, from a product perspective they are really separate. It's only at the engineering level that they have shared components to coordinate.
</details>

---

**Jessica Laughlin** (2026-03-10):
> just caught up with @Tushar Roy! I'm going to split buffering and batching into two separate proposals, each with clear contracts, so we can put them in front of customers and determine which is more urgent. I'll share the updated docs here once they're ready.

<details>
<summary>Thread (5 messages)</summary>

**Jessica Laughlin** (2026-03-13):
> back, and split things into two proposals: [buffering](https://www.notion.so/temporalio/Proposal-Buffering-Accept-Work-Now-Execute-Later-3248fc5677388157b2cbc4def089146d) and [batching](https://www.notion.so/temporalio/Proposal-Batch-Semantics-Manage-a-Group-of-Workflows-as-a-Unit-3248fc56773881e490d9c7c5eac429b2).
>
> I kept things at a capability level and didn't dive into API design. @Tushar Roy is this at the right level for you? does it cover everything?

**Tushar Roy:** when do you plan to run this by other customers?

**Jessica Laughlin:** I'd share a non-OpenAI version with other customers this week, if it looks roughly correct to you

**Tushar Roy:** In buffering use case, maybe you should explicitly callout that we would need a new contract to communicate status of buffered WFs
</details>

---

**Yimin Chen** (2026-03-11):
> Now we have TQ priority, I could imagine we leverage that feature to mark those ok-to-be-buffered workflows as low priority. Then we really just need to also treat to-be-buffered StartWorkflowExecution API calls as low priority so spike of them won't impact other normal live traffic, then we essentially has a buffering feature.
>
> This essentially use namespace's unused capacity limit to buffer low priority workflows which can use available capacity up to their limit to process them as fast as they can while not impacting live traffic. This may be good enough to solve that occasional spikes (as long as they are not too crazy), but won't be enough to solve the large backfill case.
> Is that a dumb idea? or does it make sense?

---

**Jessica Laughlin** (2026-03-14):
> @Tushar Roy and I spoke with Taylan from OpenAI this morning.
>
> of the two proposals (buffering and batching), Taylan wants buffering first. it won't eliminate their Kafka layer - some of those queues have other consumers upstream - but it would reduce what he called "the burden of starting a Workflow." he's been rate-limiting internal teams that were causing issues with burst starts, which pushed those teams to build their own queuing.
>
> my next step: put the buffering proposal in front of ~5 more customers hitting similar problems to verify the shape before we get back in the room to create a blueprint/design.

---

**Jessica Laughlin** (2026-03-14):
> independently, @Paul Nordstrom raised a question: buffering may require batch machinery under the hood. should that change which one we ship to customers first?

<details>
<summary>Thread</summary>

**Tushar Roy:** We dont know that part yet

**Tushar Roy:** The buffering which @Sergey Bykov and I were thinking do not need any batch semantics
</details>

---

**Tushar Roy** (2026-03-14):
> ```
> Buffering vs. Batching: Temporal presented two new features: "Buffering" (Temporal accepts all work now, executes later, like a queue) and "Batching" (treating a group of workflows as a unit for submission).
> Buffering Preference: Taylan expressed a preference for the "Buffering" approach, noting it would simplify existing Kafka-based queuing for teams and reduce the burden of managing retries and DLQs.
> Buffering API Considerations: Discussion around whether buffering should be an explicit API call or triggered by failed StartWorkflow calls, and the complexities of SignalWithStart in a buffered context.
> ```
>
> These are direct notes from the gong

<details>
<summary>Thread (30+ messages — major discussion)</summary>

**Tushar Roy:** https://us-11514.app.gong.io/call?id=6120936069767025036 Folks are welcome to hear convo here. Batching vs buffering was discussed from 9:50

**Paul Nordstrom:** [image: "This call is private - You can request access from Collin Cook (the call owner)."]

**Tushar Roy:** @Collin Cook Can you give access to @Paul Nordstrom @Sergey Bykov @Maxim Fateev to this gong call

**Jessica Laughlin:** I think OpenAI specifically has different permissions

**Collin Cook:** Everyone should be added and able to listen now

**Maxim Fateev:** IMHO flow control when consuming from Kafka is closer to "batching".

**Tushar Roy:** Yes but this assumes our customers have Kafka cluster. Our goal is to build something which gets rid of this Kafka clusters

**Maxim Fateev:** I think we misrepresented "batching". We can absolutely implement rate limiting of ingestion from their Kafka, and it would solve the problem they have. I listened to the conversation and didn't hear a single argument towards buffering.

**Tushar Roy:** but what if they want to get rid of kafka?

**Maxim Fateev:** They didn't ask for it. And the only use case was that they consume from some other place and publish to Kafka

**Tushar Roy:** I agree that Kafka events can be batched but what if people dont want kafka

**Maxim Fateev:** We can help them to rate limit consumption from that other place

**Maxim Fateev:** What is the source?

**Tushar Roy:** yes they didnt ask but we have to build for the future. I m pretty sure they will ask

**Maxim Fateev:** If it is a DB the batching is much better

**Tushar Roy:** Yes I agree that what they described was batching if using existing kafka queue or DB

**Maxim Fateev:** buffering is much more complicated project. Just thinking about DLQ and other things we need to implement the moment we put a queue in the system

**Tushar Roy:** Also agree with that it is more complicated.

**Maxim Fateev:** Also we don't even need "batch as an entity" to rate limit Kafka consumption.

**Maxim Fateev:** We can just introduce a rate limiter on ingestion

**Maxim Fateev:** Not rate limiter, but flow controller

**Jessica Laughlin:** I'm connecting with ~5 customers this week who are hitting burst start limits or have put queues in front of Temporal. [here](https://www.notion.so/temporalio/Internal-Customer-Conversation-Guide-Burst-Starts-3258fc567738811797c3fbbb1ebadc4b) is my conversation guide. my goal is to figure out whether these customers need buffering, intake flow control, batching, or something else entirely.

**Sergey Bykov:** @Tushar Roy are you still concerned about supporting SignalWithStart for buffering? I can't think of any additional issue there over StartWorkflowExecution. Less sure about signals because of the potential ordering issues.

**Tushar Roy:** I havent given full thought about it, just wanted to express that we have mostly thought about StartWF only

**Maxim Fateev:** SignalWithStart should work fine if we buffer

**Maxim Fateev:** But we need DLQ anyway
</details>

---

**Jessica Laughlin** (2026-03-17):
> @Maxim Fateev could I ask you to explain this? I don't think I understand what you mean here:
>
> ```
> Also we don't even need "batch as an entity" to rate limit Kafka consumption.
> We can just introduce a rate limiter on ingestion
> Not rate limiter, but flow controller
> ```

<details>
<summary>Thread</summary>

**Jessica Laughlin:** do you mean like a Kafka Connector that would read from a customer's Kafka topic, start a Workflow per message, and throttle the rate to match Temporal's capacity?

**Maxim Fateev:** Yes. But we could provide some API to help with this flow control. For example slow down start workflow calls
</details>

---

# 3. Slack: Stripe Use Case Threads

## 3.1 Stripe Thread 1 — Bulk Work Ingestion

**Source:** [#crew-storage-flow-control](https://temporaltechnologies.slack.com/archives/C0741B07RFC/p1772142963827009) (2026-02-27)

**Brandon Chavis** (Feb 27):
> @Jessica Laughlin just shared some interesting feedback from OpenAI, they have requirements that can be (at least partially) addressed by some of our flow control initiatives. However, it seems like they want both front end rate limiting + request queueing, which I don't think we've included in our early ideas for a hypothetical rate limiter feature
>
> ```
> Bulk Work Ingestion is a different problem: there's no intake valve. These customers (OpenAI, Stripe, Snap) don't want to run a single batch operation - they have millions of external records that each need to become a workflow. The pain is:
> - StartWorkflow one-at-a-time doesn't scale to millions
> - No backpressure mechanism - you can overwhelm the server
> - Customers build Kafka as a DIY buffer layer (infra cost + complexity)
> - Need fire-and-forget semantics (don't wait for each to schedule)
> ```

**Roey Berman:**
> How would this be addressed by our flow control primitives? Sounds like what they want is a queue in front of Temporal.

**Jessica Laughlin:**
> yeah, they currently have Kafka in front of Temporal as a DIY buffer! and would prefer not to.

**Phil Prasek:**
> if their namespace isn't provisioned to accept 1M/sec and auto scaling takes time and they're not throttling themselves with a DIY Kafka buffer, then how would Temporal Cloud protect itself from getting overrun and becoming unreliable for other customers on that cell without rate limiting?

**Roey Berman:**
> Temporal cloud already has rate limits in place in the frontend.

**Brandon Chavis:**
> I think the need is customer configurable rate limits on the front end

**Roey Berman:**
> Agree we can offer some sort of queue, but we have to be careful with the semantics there. Would the queue be before the frontend where the limits get enforced? That would set a precedent.

**Preeti Somal:**
> @Paul Nordstrom @Yimin Chen @Tushar Roy for visibility, the bulk ingestion pattern has come up consistently over the past few years - including from Snap. would be great to bring all the knowledge around it together.

**Yimin Chen:**
> If this thing is not transparent, as in user know that these requests are buffered and there is no expectation that the to-be-started workflow ID is still NOT_FOUND if they try to do anything about it via server APIs. Then, this problem can be modeled as a use case of the collection/batch (aka coolcat). User would dump those requests into the collection, and they can control how fast they want them to be processed etc.
>
> What we discussed 2~3 years ago, was trying to make it transparent, that user just start workflow normally, and when server under stress would automatically offload them into some kind of queue in front of server frontend. That has lots of problems about API contract and subsequent call expectations.

**Tushar Roy:**
> Critical requirement is that this entire batch needs to be buffered before it hits our cellular infrastructure and then based on capacity of our infra, we can process these requests slowly.

<details>
<summary>Extended Tushar/Maxim debate on where to buffer (50+ messages)</summary>

**Tushar Roy:** @Yimin Chen where would that collection go while its being processed?

**Roey Berman:** @Yimin Chen the collection idea is subject to the 4mb grpc limit. If your workflow inputs are large, you might not be able to submit more than a handful at a time. You would still be subject to frontend rps limits.

**Tushar Roy:** All ChasmObject use CDS and entire server stack. If customer start 1 million single item WFs in 1 min, it would all go via entire stack in CDS. How would we have such kind of capacity?

**Yimin Chen:** I was hoping we need 10% or 5% capacity to buffer the request compare to fully process them.

**Tushar Roy:** Even if its 10%, it is a lot of capacity and it is not 10%. Remember customer want to queue millions in short time without thinking about it.

**Tushar Roy:** CHASM is not 10%. It is basically almost a cost of full action. Less than StartWF cost but still pretty high.

**Tushar Roy:** I support the argument that if we can batch 1000 WFs in single WF, then CHASM object with large payload support could work very well

**Tushar Roy:** Taylan today very clearly said, they dont want to batch at all and just fire startWFs at rapid pace in fire and forget fashion. We should take their WFs and eventually slowly process it. That is one of the 4 scenarios

**Maxim Fateev:** I think we should add start that doesn't create a workflow task. Then have a controller that initiates execution of workflows in a rate or parallelism controlled manner.

**Roey Berman:** But workflow tasks go to matching backlog, that's already a pretty good queue. And you get priority and fairness out of the box.

**Maxim Fateev:** What If we use a separate queue for the first workflow task?

**Tushar Roy:** @Maxim Fateev where would this start WFs be buffered while they wait for processing? That is the core of this problem.

**Maxim Fateev:** They are just normal workflows. So they will be buffered in history service and cds/walker

**Tushar Roy:** But I dont have capacity in CDS/WALKER to absorb 100x traffic for this WF buffering.

**Tushar Roy:** We dont run with that kind of headroom. Usually we run with 50% headroom for spikes from customers.

**Maxim Fateev:** If you have 200k actions capacity then you can process more than 200k starts per second

**Tushar Roy:** There is not a single cell which has 200K actions spare capacity

**Tushar Roy:** Our spare capacity is designed for normal spikes from customers. Not for 100x-1000x spikes from single namespace.

**Tushar Roy:** In today's example OAI wanted us to buffer 1M of these StartWF in matter of seconds. If I sent this 1M WF through my cellular infrastructure in matter of seconds, it would just disrupt everything.

**Maxim Fateev:** I doubt they really need this. It is a batch job, not an external traffic spike

**Tushar Roy:** But point is that is why they want us to buffer, so they can send all that WF our way and it becomes our problem.

**Tushar Roy:** We need to design for the future where we have somewhat infinitely scalable and elastic queue in front of our cells, which can absorb these loads and we can process them slowly

**Maxim Fateev:** A problem that I see all the time is start rate and execution rate cannot be rate limited separately

**Maxim Fateev:** I'm not convinced that adding another persistent queue is the right answer

**Maxim Fateev:** Temporal is already scalable and pretty efficient.

**Tushar Roy:** Temporal is reliable and efficient for things it does today. Just dumb queueing is not what it is efficient at

**Maxim Fateev:** Why? Putting start records to the wal should be pretty efficient

**Tushar Roy:** Putting start record in WAL is efficient if we had spare capacity. As I said before we dont carry that much spare capacity.

**Tushar Roy:** And WAL dont scale up in matter of seconds to absorb that capacity

**Maxim Fateev:** But you will for your queue anyway

**Tushar Roy:** I never said I will use our WAL for the queue. It is an option. But we have to explore what else is there in the market. Remember this is dumb queue which absorbs and spits out when we want it to.

**Tushar Roy:** My WAL in the cell run with just enough capacity so we can absorb some spikes from the customer. It is not designed to absorb 10x-20x capacity in matter of seconds. Same with history and frontend.

**Tushar Roy:** We dont know how much StartWF customers may queue. If we release the feature like that, customers expect unlimited starts WFs

**Maxim Fateev:** Any queueing technology with "unlimited capacity" will require significant hardware

**Tushar Roy:** Not if they are multi-tenant and we are just small portion of their total RPS like Kinesis.

**Tushar Roy:** S3 today is practically unlimited capacity. Not because they are very elastic. Only because they are multi tenant and no single customer spike can hit them fast enough to overwhelm their system.

**Maxim Fateev:** I didn't realize that you want to use a cloud service for this. I doubt that you can send 1 million requests per second to Kinesis for a short spike and don't pay them anything for carrying that capacity

**Tushar Roy:** We will have to see. Worse case we can have our own WAL in front of our cell if that makes it cheaper and efficient. But characteristic of WAL in front of our cell will be different than our current cell WAL which is fully optimized for writing and not for reading.

**Paul Nordstrom** (Mar 1):
> Like Tushar, I believe the appropriate place to buffer this state is in S3. I think we can build something that absorbs practically any spike they want to send us. I don't think Slack is the place to design this though. When are we going to get together for a brainstorming session?

**Tushar Roy:** @Paul Nordstrom @Maxim Fateev - can we sync on this while in offsite?
</details>

---

## 3.2 Stripe Thread 2 — Tech Day Follow-up

**Source:** [#crew-storage-flow-control](https://temporaltechnologies.slack.com/archives/C0741B07RFC/p1773059264386509) (2026-03-07)

**Brandon Chavis** (Mar 7):
> We need to follow up after the Stripe tech day last week. That summary is here: https://temporaltechnologies.slack.com/archives/C03HRBUJM3M/p1772734810772619
>
> Personally, I feel conflicted about our next steps on the flow control project after our session. In our discussions leading up to the tech day, it was unanimous that task dispatch throttling was top of the heap. However, it seems like brendan was overridden in our flow control session and a classical rate limiter was highlighted as being _more_ appealing.

**Roey Berman:**
> I was confused on the call about what they want. We need to understand if they are willing to be design partners and if paying an action per webhook is even on the table for them.

**Brandon Chavis:**
> We do [have other customers]. Twilio, Rippling, and Relativity are vocal in their need for something here.
>
> Relativity is firmly in favor of task dispatch throttling. Twilio wants both a rate limiter/queue + task dispatch throttling. Rippling probably needs Concurrency and Ordering the most.

**Brandon Chavis:**
> I think it's a _safe_ bet to build Task Dispatch Throttling, it will be adopted by the customers mentioned here.

**Conrad Vogel:**
> Jeff Schoner (jeffschoner@stripe.com) is the person you're referring to, Brandon. Agreed on the need for Stripe's specific alignment and design partnership here.

**Jimmy Jea:**
> I think the Q3 timeline is more that they will fully own webhooks by end of H1. they are already building a new flow control mechanism but are purposely making it composable. so I don't think we necessarily need something by Q3. we should prioritize quality over velocity

**David Reiss:**
> What is the difference between "task dispatch throttling" and "classical rate limiting" here? I thought those were synonymous (in our context)

**David Reiss:**
> Also, this channel is for a totally unrelated "flow control" project (cds<->database). We should move the conversation somewhere else

**Brandon Chavis** (Mar 8):
> We are taking this channel over, it is now about rate limiting (kidding but also maybe not)

---

# 4. Slack: Netflix Use Case Thread

**Source:** [#field-engineering-internal](https://temporaltechnologies.slack.com/archives/C04NYM5D3U6/p1762295432405379) (2025-11-05)

**Steve Androulakis:**
> Netflix are thinking of adopting SQS in front of their workflow signals to avoid signaling a workflow too quickly. I've already given the advice:
>
> > There's no inherent way to avoid accumulation of signals on a workflow @SignalMethod. Any backpressure would come from another mechanism e.g. SQS queueing. Or as mentioned here, sharding to lower the amount of signals any one workflow receives (though this still risks hitting limits in any one shard).
>
> They're wondering if we're ever going to deliver a mechanism to rate limit / introduce backpressure on signals so they don't have to introduce a component / change strategy to support their high signal volume

**Steve Androulakis:**
> quote from Netflix:
>
> > I'll say that I do think the lack of control over signal TPS is an odd thing in an event driven system. Often times, a big reason to use an event driven system is the ability to apply back pressure and/or smooth out inconsistent traffic patterns so that downstream systems aren't browned out.

**Maxim Fateev:**
> The solution is to support higher throughput per workflow instance

**Maxim Fateev:**
> We might introduce explicit queues or streams at some point for large collection processing

**Steve Androulakis:**
> That would be awesome. It sounds like there's no product plan yet, but I'll add it as a request from Netflix

**Paul Nordstrom:**
> It's far from a product plan but there is a one pager proposal for this.
> https://www.notion.so/temporalio/Operation-Collection-CAT-COOLCAT-Collection-Of-Operation-Locators-2508fc56773880e592fcc253b1d0c282

**Joshua Smith (Josh):**
> 🙏 Been hoping for something like this for a long time.

---

# 5. Slack: Rippling Batch Use Case

**Source:** [Slack](https://temporaltechnologies.slack.com/archives/C054VD4BZFD/p1773277061358009) (2026-03-09)

**Paul Nordstrom:**
> Who can talk to me about the Rippling "batch" use case(s)? We're doing exploration of a batch handler product spike and Samar suggested that this was a particularly interesting use case. @Jessica Laughlin FV

**Taylor Khan:**
> I can do that

**Paul Nordstrom:**
> I will schedule something

---

# 6. Slack: Tao's Customer Use Case

**Source:** [#sg-standalone-activities](https://temporaltechnologies.slack.com/archives/C08GZQN1C80/p1774071197820989) (2026-03-18)

**Tao Guo** (Staff Solutions Architect):
> Hey team, just wanted to surface some feedback I picked up while chatting to a customer about standalone activity vs SQS.
>
> The use case is a spiky endpoints for ingesting audit data via a REST endpoint. It can go over the default 400 APS but they struggle to justify the cost of additional TRU, especially when comparing with a SQS based solution - much cheaper at scale.
>
> I'm curious anyone here has some suggestions to better position standalone activity in this case.

*(No thread replies)*

---

# 7. Slack: Async Dedup Thread

**Source:** [Slack](https://temporaltechnologies.slack.com/archives/C09EB1D10UD/p1765831561982189) (2025-12-15)

*Thread about Coinbase SAA latency requirements, leading to async dedup discussion.*

**Phil Prasek:**
> [Coinbase SAA question about <200ms latency. They have an internal service with a 90ms latency SLO processing 200-600 million actions/day.]

**Phil Prasek:**
> Tags Dan Davison, Fred Tzeng, and Roey. Asks: Will `StartActivityExecution` p99 match `StartWorkflowExecution` (~54ms)? What's the Temporal overhead latency? Should SAA honor `request_eager_start`?

**Roey Berman:**
> Start latencies should be comparable to workflows and we have eager and optimistic start planned for post GA.

**Maxim Fateev:**
> Standalone activity should have lower latency as it doesn't have event log.

**Maxim Fateev:**
> We could also eliminate a db read if we can dedupe them asynchronously.

**Roey Berman:**
> We still guarantee ID uniqueness so unsure how to dedupe asynchronously. Might have lower latencies due to writing only to mutable state if payload is small.

**Maxim Fateev:**
> We guarantee, but we can skip duplicated starts asynchronously. This assumes that the user ensures that duplicated requests for the same id are the same.

**Roey Berman:**
> That would be a new semantic. Was thinking optimistic start would cover that already.

**Maxim Fateev:**
> At CDS level dedupe requires db read which can add a lot of latency.

**Roey Berman:**
> Thought we had plans for adding a bloom filter to speed up deduping.

**Maxim Fateev:**
> Doesn't propose this for workflows yet. But might add it to standalone activity from the beginning.

<details>
<summary>Extended discussion on latency and dedup semantics</summary>

**Maxim Fateev:** [forwarded from Ben Eddy] For larger clusters, the `select_current_execution` DB read p99 is 30-50ms and `StartWorkflowExecution` API p99 is 40-80ms, so the read accounts for roughly 50-80% of total latency.

**Paul Nordstrom:** We should consider an optimistic mode for SAA that doesn't validate key uniqueness and allows duplicate instances if there IS a duplicate. Would shrink latency greatly.

**Paul Nordstrom:** Guesses majority of customers would choose this.

**Roey Berman:** Agrees we should have it, already mentioned in blueprint for post GA. But would still have dedupe on complete unless that's also bypassed.

**Maxim Fateev:** Doesn't see much value in rejecting start standalone activity synchronously. Most users will choose to drop duplicated starts for a given ID. Most such starts come from retries with the same input anyway.

**Maxim Fateev:** From latency POV, "no user provided id mode" (never rejects starts) and async rejection of duplicates are the same.

**Roey Berman:** Was thinking no ID would probably be the semantic along with a new conflict policy.

**Maxim Fateev:** We should dedupe. Just asynchronously.

**Phil Prasek:** Should we always dedupe async or would it be based on start options?

**Maxim Fateev:** Can imagine use cases when sync is needed. But async can be the default.
</details>

<details>
<summary>Coinbase batch start and eager result discussion</summary>

**Phil Prasek:** Another Coinbase question re: batch start operations and batch acknowledge.

**Chad Retz:** Suggests dropping the reasoning about why batch start isn't supported. The number of RPCs isn't what drives action cost.

**Phil Prasek:** Another idea for Coinbase callers needing result back in <90ms with eager start: can we also do eager result?

**Chad Retz:** Wouldn't be "eager result" but rather "wait for result", similar to updates where they provide a max time to wait on the same start call.

**Maxim Fateev:** Can imagine a highly optimized synchronous execution path at the history service to lower latency even more.
</details>

---

# 8. Slack: COOLCAT Design Thread

**Source:** [#coolcat-design](https://temporaltechnologies.slack.com/archives/C09U97WTWKY/p1763672408912749) (2025-11-21)

**Paul Nordstrom** (forwarding from Alex Tideman):
> Hey Paul, COOLCAT question. Would COOLCAT allow us to query running workflows by ExecutionDuration/HistorySize/HistoryLength? Right now we can't update those search attributes for running workflows

**Paul Nordstrom:**
> @Alex Tideman within the concept of my current proposal, this would be pretty easy. It's such an obvious win (given how often we hear this request from customers) I'm tempted to say "of course this would be included" but there could be hidden complexity and competing features. It would only answer your question across the scope of the collection, of course.

**Roey Berman:**
> @Paul Nordstrom can you explain how coolcat would change the query capabilities of running workflows?

**Roey Berman:**
> How are we going to support this without incurring more visibility writes per workflow execution?

**Paul Nordstrom:**
> coolcat would gather statistics about all the workflows "under its purview". We would forward statistics (e.g. workflow completion, etc.) from the workflow engine to "subscribed entities" like CC for aggregation and query access.

**Paul Nordstrom:**
> no visibility involved

**Roey Berman:**
> How does that satisfy this requirement? "Would COOLCAT allow us to query running workflows by ExecutionDuration/HistorySize/HistoryLength? Right now we can't update those"

**Paul Nordstrom:**
> Let me ask you the other way around. What question doesn't it answer?

**Roey Berman:**
> Not sure what you mean. The request was to query workflows (via visibility) based on attributes that we only update when the execution is closed. This new collection primitive doesn't touch that.

**Alex Tideman:**
> Specific example: Query running workflows with duration over 1m (now - startTime)

**Paul Nordstrom:**
> so, in coolcat, there's a "state" variable for each operation in the set ("opstate"), with room for statistics about that operation. We have to budget this carefully to include only the information about that operation we consider high-value. But say that this item meets the bar. Then we can (dumb version) iterate over all the opstate vars looking for operations that meet the criteria you just described. This isn't a database... each query type requires dedicated code.
>
> alternatively, the coolcat can export all the opstate data (in a federated way of course... but we would (auto-)partition the state for parallelism, and do a distributed query to get the answer you are looking for here.

---

# 9. Additional Slack Findings

*From broad Slack searches for "buffering batching", "COOLCAT", "buffer queue", etc.*

**Paul Nordstrom** (2025-10-17, Slack):
> I feel like I know enough to design the coolcat (CC) as a primitive and as an enabler for a batch product...my personal opinion is that we DO want to expose something like CC to customers, but that we don't bill it as our Batch product; just a useful primitive.

**Paul Nordstrom** (2025-10-03, Slack, EvenUp customer thread):
> not helpful to you, but worth noting that this use case is another argument in favor of us building COOLCAT

**Jimmy Jea** (2026-03-15, Slack):
> hello - is this the batch operation/COOLCAT? if so, i have details on Stripe's initial need for this

**Pylon bot** (2026-03-18, Slack, At Risk Accounts report):
> OpenAI "urgently needs batching/buffering capabilities for handling traffic spikes."

**Mike Nichols** (2026-03-18, Slack):
> "It kind of depends on the frequency of events being batched per customer id. Let's say that there is a buffer of 50 events per recomputation of available tokens...There are technical and cost risks over the long term taking their current approach."

---

*End of compendium. Sources not included due to access restrictions:*
- *OpenAI Gong Call (https://us-11514.app.gong.io/call?id=6120936069767025036) — restricted access*
- *Roey's "inbox pattern" doc (Notion page 2a38fc567738807a9647f19361b03aee) — referenced but not fetched*
- *Stripe Tech Day summary (https://temporaltechnologies.slack.com/archives/C03HRBUJM3M/p1772734810772619) — referenced but not fetched*
- *Design Recommendations for Large Scale Batch Campaigns (Notion page bc4b3a0f88a64877b09e54d9e075ea2e) — SA best-practices doc, tangential*
