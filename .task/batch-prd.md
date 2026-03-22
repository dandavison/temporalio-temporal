# Buffering + Batching: Handling Work Temporal Can't Immediately Process

> **Status:** 1-pager Draft
> **Driver:** Jessica Laughlin
> **Persona:** Developer
> **Company Effort (LOE):** 1: 12+ person-months (extensive effort)
> **Customer Demand:** 2: 5%+ customers requesting
> **Customer Value:** 3: Moderate value (improves customer experience noticeably)
> **Revenue Potential:** 2: $1M ARR next 3 years
> **Strategic Alignment:** 4: Strong alignment (advances multiple strategic goals)
>
> [Notion source](https://www.notion.so/31e8fc567738808c91e4d06bf989c581)

---

> **Note:** "Batch" is overloaded with Temporal's existing Batch Operations (terminate/cancel/signal on existing workflows). This 1-pager uses "batch" in the industry-standard sense: submitting a large group of Workflows for execution at once. Final naming TBD.

---

# Problem

Temporal has no way to accept work it can't immediately process.

<details><summary>💬 <b>Discussion</b> on "Temporal has no way to accept work it can't immediately process." (3 comments)</summary>

> **Dan Davison** (2026-03-13): The way I'd phrase this is that Temporal does so much when it accepts work that it can't match the write throughput of Kafka/SQS. I get what the current wording means, but it's a bit confusing because accepting work to be processed later is exactly what Temporal is designed for.
>
> I.e. in Temporal's current implementation, although StartWorkflow is basically just a persistence write and the actual processing is async, we do expensive work on acceptance: e.g. some CPU-bound work in Frontend, an RPC hop to History adding several serialization/deserialization steps, some CPU-bound work in History, 2 WAL writes, etc.
>
> In contrast, to match the write throughput of Kafka/SQS, we'd basically have to (I assume) append to WAL and not much else.

> **Yimin Chen** (2026-03-14): This is actually a good point to continue digging. Now we have TQ priority, I could imagine we leverage that feature to mark those ok-to-be-buffered workflows as low priority. Then we really just need to also treat to-be-buffered StartWorkflowExecution API calls as low priority so spike of them won't impact other normal live traffic, then we essentially have a buffer feature.

> **Paul Nordstrom** (2026-03-14): Adding an item to a task queue is a hugely more expensive thing compared to appending to the WAL. so I believe Dan's approach is much more realistic if our goal is to get anywhere near the processing cost of say Kafka (which of course it absolutely is). CHASM's very raison d'etre is to make these sorts of things efficient. Whether via implementation of a native queue CAT (e.g. OPFQ) or through capture of operations in (say) something like COOLCAT, we can outperform task queues by an order of magnitude.

</details>

Whether a user starts or signals a large burst of Workflows, hits an unexpected traffic spike, or loses connectivity during an outage, the result is the same: Workflows aren't created, Signals aren't delivered, and the burden of recovery is pushed to the application.

Today, this surfaces in three ways:

1. **Intentional bursts get rejected**. Nightly batch runs, backfills, or fan-outs from an upstream job hit `RESOURCE_EXHAUSTED` limits. 137 accounts (representing 86% of all platform actions; 61% excluding OpenAI) hit this via `StartWorkflow` in the last 30 days.

    <details><summary>💬 <b>Discussion</b> on "hit RESOURCE_EXHAUSTED limits" (1 comment)</summary>

    > **[No discussion content retrieved for this anchor]**

    </details>

2. **Traffic spikes have no graceful degradation**. When inbound volume unexpectedly exceeds APS limits, the server rejects requests immediately. The SDK retries with a backoff, but if the spike outlasts the retry window, Workflows will not be created.

    <details><summary>💬 <b>Discussion</b> on "Traffic spikes have no graceful degradation" (1 comment)</summary>

    > **Harani Mukkala** (2026-03-12): For traffic spikes, is the intended extensibility model a reactive overflow buffer (server automatically starts absorbing when capacity is exceeded, transparent to the caller), or does this also require callers to opt into a buffered submission mode? The architecture implications are quite different.

    </details>

3. **Unavailability means lost work.** When Temporal is unreachable, Workflows cannot be created. During a network outage, Block lost ~11,480 Workflow starts that had to be manually recovered.

    <details><summary>💬 <b>Discussion</b> on "During a network outage" (1 comment)</summary>

    > **Lina Jodoin** (2026-03-13): I get that we're saying, "durability of a batch of work shouldn't be coupled to our availability", but to me, this reads more as if we're intending to solve the problem of availability at *ingestion* time, which I don't believe we are (e.g., by integrating into the customer's own network, like their Kafka queueing solutions).

    </details>

Without a way to accept work it can't immediately process, Temporal pushes customers toward the same workaround: put a queue between their systems and Temporal. Stripe, OpenAI, and Coupang use Kafka. Netflix uses SQS. Meta built a MySQL workaround. 25+ named customers across all four customer segments are affected. (Full list in appendix.)

Our biggest customers are building infrastructure to work around our infrastructure. Temporal should encourage customer growth, not resist it.

<details><summary>💬 <b>Discussion</b> on "Our biggest customers are building infrastructure to work around our infrastructure." (1 comment)</summary>

> **Jessica Laughlin** (2026-03-11): Saving comment from Ethan Ruhe:
>
> "huge anti-patterns we should both eliminate the need for and make sure users know is not necessary. probably covered later, but this emphasizes the value of GTM and having a best-practices reference available to devs (and coding agents)"

</details>

### Impact Classification and Strategic Alignment

**Core Loop**: The accounts hitting this ceiling are the center of our target: high-value, high-volume use cases. This removes the workaround tax for those customers and makes the platform resilient to the conditions they already face: intentional bursts, traffic spikes, and outages.

**Revenue tailwind** ($1M+ from existing accounts, plus unlocks data orchestration market):

The 137 throttled accounts generate 86% of all platform actions. Two dynamics connect this feature to revenue:

1. **Eliminating expansion friction.** Accounts at their APS ceiling face an engineering tax to add bursty workloads. Stripe routes every bursty workload through Kafka. OpenAI built custom queueing. That tax slows adoption of new use cases and pushes some workloads to other platforms entirely. Removing this tax means accounts grow faster and fewer workloads leak.

    <details><summary>💬 <b>Discussion</b> on "workload through Kafka … OpenAI built custom queueing." (1 comment)</summary>

    > **Harani Mukkala** (2026-03-12): What percentage of the use-cases are bulk ETL style workloads vs steady stream with occasional bursts?

    </details>

2. **New workload capture.** Prospects evaluating for high-volume use cases (Lululemon designing a Kafka replacement, Finch Legal wanting 5-10x their current 500 TPS) hit burst limits during evaluation. Native burst absorption removes a blocking objection.

Conservative estimate: 2% additional actions growth across accounts responsible for ~$60M in actions ARR = $1.2M/year.

**Market expansion prerequisite**. This is also the foundation of the planned Batch primitive.

<details><summary>💬 <b>Discussion</b> on "This is also the foundation of the planned Batch primitive." (1 comment)</summary>

> **Sergey Bykov** (2026-03-11): I don't understand this statement. To me, batch and burst are two different cases. I don't think we should conflate them.

</details>

Without burst absorption and batch semantics, Temporal cannot serve data orchestration workloads because they are inherently bursty: scheduled ETL, bulk processing, periodic fan-outs. This is the market where we compete with Airflow, Prefect, and Dagster. Revenue potential of that use case category is not included in the $1M+ estimate. It's additive, and will be sized in the PRD once we have a design proposal and customer validation signal.

Overall FY27 alignment:
- Objective 2: Sharpen & Scale Use Cases — supports a subset of batch processing use cases
- Objective 4: Earn Enterprise Credibility — enterprise-scale operations without manual coordination

<details><summary>💬 <b>Discussion</b> on Revenue Potential property (2 comments)</summary>

> **Ethan Ruhe** (2026-03-10): Seems low - rough framing of revenue on order of $100k over 3 years for 12+ person months of eng. Understandably a feature where revenue attribution is hard, but over 3 years $1m is on the order of 0.1% of total revenue - that doesn't seem crazy at all, does it?

> **Jessica Laughlin** (2026-03-10): Great question! I updated the revenue potential to $1M+ ARR in next three years, and added my reasoning to the impact classification section. I didn't touch the "Company Effort" yet since we don't have an idea of the Engineering work involved. I will update once we sketch out potential implementations. but please let me know if there's a better way to do this!

</details>

---

# Solution and Requirements

This document proposes a solution to the first user problem: intentional bursts get rejected. But the implementation must be able to solve the second ("traffic spikes have no graceful degradation") and third ("unavailability means lost work") user problems without rearchitecting.

To solve intentional bursts, Temporal needs two new capabilities:

1. **Burst absorption.** The platform persists work on arrival, decoupling acceptance from execution. Work is buffered durably and drained at a sustainable rate.
2. **Batch semantics.** A way to submit, track, and manage a group of Workflows as a unit.

Together, these eliminate the need for customers to build their own in-house buffering and reconciliation.

### How it works

Users submit Workflows as a Batch. Temporal accepts them at rates far above what it can execute immediately. The target is acceptance latency competitive with Kafka/SQS (single-digit millisecond per item). Workflows then start at a platform-controlled rate.

<details><summary>💬 <b>Discussion</b> on "Users submit Workflows as a Batch" (4 comments)</summary>

> **Preeti Somal** (2026-03-10): Does this need to be explicit? IIUC in some cases the customer can't predict the burst volume…. that was certainly Snap's use case.

> **Jessica Laughlin** (2026-03-11): Good callout - Snap's case is a traffic spike, not a planned batch, so they can't pre-define what to submit.
>
> The problem statement now describes three failure modes: intentional bursts, traffic spikes, and unavailability. This phase solves the first (intentional batches with a defined submission), but the infrastructure should extend to reactive cases like Snap's without rearchitecting.
>
> Adding streaming/drip submission to Out of Scope to make this explicit.

> **Roey Berman** (2026-03-11): Agree this is different from a batch for some of the use cases. The couple of use cases I've seen require a queue to decouple Temporal frontend's rate limiting from producers.

> **Sergey Bykov** (2026-03-11): >"but the infrastructure should extend to reactive cases like Snap's without rearchitecting."
>
> I'm struggling to see this connection.

</details>

Batches must scale to the sizes customers need: 1M (SailPoint), "millions" (Roblox), 500M (OpenAI).

Supported Batch operations:

<details><summary>💬 <b>Discussion</b> on "Supported Batch operations:" (1 comment)</summary>

> **Roey Berman** (2026-03-11): What about flow control (e.g. rate limiting)?

</details>

- **Create**: open a Batch, submit Workflow start requests. Each request contains the same parameters as a `StartWorkflow` call (workflow type, workflow ID, task queue, input, timeouts).

    <details><summary>💬 <b>Discussion</b> on "Create" (1 comment)</summary>

    > **Roey Berman** (2026-03-11): Does it need to be an explicit `create`? Why not `add` and we create it if it doesn't exist?

    </details>

    <details><summary>💬 <b>Discussion</b> on "StartWorkflow" (1 comment)</summary>

    > **Roey Berman** (2026-03-11): This is just one type of action that users may want to submit. There's also `StartActivityExecution` `SignalWorkflowExecution` and many more.

    </details>

- **Query:** aggregate status (pending, started, completed, failed counts) and per-Workflow status.

    <details><summary>💬 <b>Discussion</b> on "Query" (1 comment)</summary>

    > **Roey Berman** (2026-03-11): Is there a specific retention that you are considering here for full failures or results?

    </details>

    Failed Workflows queryable with failure reasons.

    <details><summary>💬 <b>Discussion</b> on "Failed Workflows queryable with failure reasons" (1 comment)</summary>

    > **Lina Jodoin** (2026-03-13): It'd be great to support some sort of convenient retry mechanism here, either retrying as part of the existing batch request, or some sort of, "query and retry the returned results in a new batch" semantic.

    </details>

- **Cancel:** stop pending Workflows from starting, cancel running Workflows
- **Pause/Resume:** stop draining new Workflows from the buffer; running Workflows are unaffected

Workflows that fail to start (invalid workflow type, ID conflict) are marked failed with a reason.
Workflows that start successfully are tracked by their terminal state. Individual failures do not
stop the overall Batch. Aggregate and per-Workflow observability are surfaced through standard monitoring.

### Guarantees

1. **Durable acceptance.** Once acknowledged, a Batch survives crashes, restarts, and failovers.
2. **No silent loss.** Every accepted Batch and its contained Workflows are guaranteed to reach a terminal state (completed, failed, or cancelled). Workflows that cannot start are surfaced to the user. Individual failures do not stop the overall Batch.
3. **Batch tracking.** Each Batch has a client-provided ID that users can query for full status.
4. **Cancellation completeness**. Cancel stops pending Workflows from starting and cancels already-running Workflows.
5. **Idempotent submission**. Retrying with the same Batch ID doesn't recreate the Batch.

### Out of Scope, But Design for Extensibility:

- Reactive overflow buffering ("traffic spikes have no graceful degradation")
- Unavailability recovery ("unavailability means lost work")

    <details><summary>💬 <b>Discussion</b> on "Reactive overflow buffering / Unavailability recovery" (1 comment)</summary>

    > **Sergey Bykov** (2026-03-11): Why are these out of scope? Above we say, "Temporal needs two new capabilities" but we only propose one of them in the end.

    </details>

- Streaming/open-ended submission (no predefined batch)
- SignalWorkflow and standalone Activity support (design API to extend beyond StartWorkflow) - Meta already hitting this: needs 10K signals/sec for version upgrades, current Batch Operations cap at 50 RPS

    <details><summary>💬 <b>Discussion</b> on "SignalWorkflow and standalone Activity support" (1 comment)</summary>

    > **Dan Davison** (2026-03-13): I.e. a single batch could contain a heterogeneous mix of execution types, right?

    </details>

- User-configurable drain rate (Stripe has requested this)

### Out of Scope

- Mutation of Batches after submission is complete
- Recurring Batches (composition with Schedules)
- Ordering, prioritization, or dependency guarantees between Workflows

    <details><summary>💬 <b>Discussion</b> on "Ordering, prioritization, or dependency guarantees between Workflows" (3 comments)</summary>

    > **Ethan Ruhe** (2026-03-10): Even if we're not changing anything, it would be worth understanding how this interacts with priority + fairness, right? E.g., do the ergonomics allow someone to submit a batch as essentially "work on this when not pre-empted by something else". Maybe this is already implicit?

    > **Jessica Laughlin** (2026-03-10): Yes, great question! In the "Proposed Defaults" section, I'm claiming that this should be our default behavior: "work on this when not pre-empted by something else". I can make this bullet clearer.

    > **Roey Berman** (2026-03-11): 100% this should interact with priority and fairness. I don't think we can make this out of scope at this point.

    </details>

- Fire-and-forget delivery tier (OpenAI has expressed interest; revisit if tracked delivery proves architecturally costly or more customer demand arises)

---

# Open Questions

### Data Gaps

Data we don't have from customers today that we might require to inform the design:

| Data point | Why it matters | What we know |
|---|---|---|
| Burst duration | Seconds vs hours changes buffer/storage requirements | Snap: sustained minutes. OpenAI: "in seconds" implies spike. Most: unknown. |
| Burst frequency | Daily/weekly/event-driven affects capacity planning | Stripe: weekly (payday). Most: unknown. |
| Latency tolerance | "Start in seconds" vs "start within an hour" are different architectures | No customer has stated this explicitly |
| Processing rate needs | Max execution rate actually needed post-burst | Stripe: "process 100/sec". Only explicit data point. |
| Batch size distribution | Determines API design, default limits, and memory/storage design | Upper end known (SailPoint 1M, OpenAI 500M). Typical batch size unknown. |

<details><summary>💬 <b>Discussion</b> on "Latency tolerance" (1 comment)</summary>

> **Harani Mukkala** (2026-03-12): Would be useful to gather data on:
> 1. Acceptable processing submission to start execution latency tolerance
> 2. Max acceptable time before considering it a timeout style failure.

</details>

### Proposed Defaults

<details><summary>💬 <b>Discussion</b> on "Proposed Defaults" (1 comment)</summary>

> **Alex Stanfield** (2026-03-15): If we allow:
>   1. append to collection
>   2. optionally process events in order
>
> This would enable event processing where ordering is important. Example: Consume from a kafka topic. For each message kick off a collection with a single SAA. If there is an ID conflict append to the collection.
>
> This could allow events to be processed in order.
>
> In kafka ordering is guaranteed for each partition on a topic. Standard pattern is to route the messages to a partition based on the hash of the message key. For sync processing the max number of messages that can be processed at a time is the number of partitions on a topic. But consumers can use async processing to increase that. Theoretical max would be your key count.
>
> Issue is that an error that blocks 1 message from being processed can lead to the entire topic being blocked/slowed for a consumer group. (broker determines a consumer is not making progress and triggers a rebalance)
>
> Use case: Sending webhooks to external customers. You can't issue the receiver of the webhook is up / will successfully process an event.

</details>

- **Cost model:** Meter starts at execution, not submission. Buffering is platform overhead. Invalid requests (bad type, ID conflict) are not billed, consistent with current behavior.

    <details><summary>💬 <b>Discussion</b> on "Meter starts at execution, not submission." (3 comments)</summary>

    > **Yimin Chen** (2026-03-10): Currently, we only charge for successful start. Failed StartWorkflowExecution API call is not charged. We have to be careful about edge case where user submit 100M invalid start workflow requests.

    > **Jessica Laughlin** (2026-03-10): Added open business question below from this, thank you!

    > **Roey Berman** (2026-03-11): Agree with Yimin, and don't fully understand the proposal here.

    </details>

    <details><summary>💬 <b>Discussion</b> on "Buffering is platform overhead" (1 comment)</summary>

    > **Harani Mukkala** (2026-03-12): This can quickly become expensive for large ETL style workloads, why not keep the cost model on-par but a percentage of their current Kafka usage?

    </details>

- **APS consumption**: Batched Workflows consume APS at execution, not submission.

    <details><summary>💬 <b>Discussion</b> on "Rate limits" (2 comments)</summary>

    > **Yimin Chen** (2026-03-10): Will it have a separate rate limit?

    > **tlo Lorinc** (2026-03-10): Second this. Will these be standard primitives or live elsewhere (either from the product perspective or purely limit perspective)?

    </details>

- **Drain rate**: Batched Workflows are started at a platform-controlled rate, not a configurable one.

    <details><summary>💬 <b>Discussion</b> on "Drain rate" (2 comments)</summary>

    > **tlo Lorinc** (2026-03-10): How will we control this rate? Is it fixed, or variable? Will there be different iterations based on if customers want their batch workflows completed in minutes vs hours vs days?

    > **Jessica Laughlin** (2026-03-10): Good questions! I dropped into design open questions.

    </details>

- **Batch priority**: Direct (non-Batch) starts take priority over Batch draining.

    <details><summary>💬 <b>Discussion</b> on "Direct (non-Batch) starts take priority over Batch draining." (1 comment)</summary>

    > **Lina Jodoin** (2026-03-13): Should we reuse matching fairness/priority queueing for this, versus something batch-specific? If so, and I think that we should, the question becomes, "do we make that explicit to the customer", which I think we also likely would want to.

    </details>

- **Batch completion**: A Batch is "complete" when all its Workflows reach a terminal state. Workflows that use continue-as-new or spawn child Workflows are not considered complete until the full chain finishes.

    <details><summary>💬 <b>Discussion</b> on "Batch completion" (1 comment)</summary>

    > **Jessica Laughlin** (2026-03-10): @Yimin Chen — updated this — is it clearer?

    </details>

- **Partial submission**: accepted Workflows are never lost, even if the connection drops mid-submission.

### Design

1. Extensibility: What needs to be true about the architecture to extend this to traffic spikes and unavailability later?
2. Architecture tiering: Snap bursts to 3x sustained APS; OpenAI wants to submit 1000x+ execution rate. Can one design stretch across this range?
3. API shape: streaming, chunked, or repeated RPCs?
4. Submission throughput: What are the rate limits on submission into a Batch? The platform does not have unlimited buffering capacity.
5. Partial submission recovery: how does the client know which Workflows were accepted and resume from where it left off?
6. Drain rate mechanics: Fixed or variable? How does it adapt to available capacity? Does the platform need to account for completion-time expectations (minutes vs hours vs days), or is that a v2 concern?
7. Batch priority: Direct starts take priority over Batch draining, making Batches implicitly "backfill when idle." Should the design support explicit priority levels between Batches in the future?
8. Batch TTL: Do we need a default expiration for abandoned Batches?
9. Observability: Which metrics and UI surfaces? Requirement is aggregate + per-Workflow status.

### Business

1. Pricing model: is there a billing event for Batch submission/buffering, or is this absorbed into existing action pricing?

    <details><summary>💬 <b>Discussion</b> on "Pricing model" (2 comments)</summary>

    > **tlo Lorinc** (2026-03-10): Once we have the shape of the solution we will need to evaluate. The ideal goal would be to reduce the cost of batch workflows by running them more efficiently. This can either come from more efficient queue/architecture or by allowing us to spread out rate limit spikes that force the use of TRUs.

    > **Jessica Laughlin** (2026-03-10): Okay great, sounds good to me.

    </details>

2. Submission abuse: If buffering is free, what prevents unbounded invalid submissions? Options include: validation at submission time, a per-submission fee, or submission rate limits. Current behavior (failed starts are free) may not scale to batch sizes.

    <details><summary>💬 <b>Discussion</b> on "Submission abuse" (1 comment)</summary>

    > **Jessica Laughlin** (2026-03-10): @Yimin Chen — added this question per your comment above, thank you!

    </details>

---

# Next Steps

1. **Engineering design exploration.** Assess feasibility, propose architecture(s) that fit the above constraints. Output: design proposal with recommended approach.
2. **Customer validation.** Once we have a design proposal, we can bring a crisper definition to a handful of accounts (OpenAI, Stripe, Snap, Roblox, and more).
3. **PRD.** Once we have signal on Batch shape and semantics.

---

# Appendix: Customer Evidence

### Tier 1: Explicitly asking for this feature

| Customer | Evidence | Source |
|---|---|---|
| OpenAI | Kafka buffer, wants native batch start, 1M+ "in seconds" | Slack, Gong |
| Stripe | Kafka buffer for all bursty workloads, "accept 1000/sec, process 100/sec" | Slack |
| Netflix | SQS in front of signals for backpressure | Gong |

### Tier 2: Hitting the wall (incidents, capacity requests, workarounds)

| Customer | Evidence | Source |
|---|---|---|
| Snap | 3x APS spikes, repeated RESOURCE_EXHAUSTED | Slack |
| Coupang | 12K APS coordinated events, Kafka buffer | Slack |
| Rippling | 6x APS spikes, Redis holding queues | Slack, Pylon #26540 |
| Meta | 100Ks of signals, MySQL workaround to avoid 50/sec batch limit | Gong |
| Roblox | Millions of workflows to signal in bulk | Gong |
| Airbnb | Throttled on StartWorkflow | Slack |
| Block | Kafka consumer signalWithStart, events to DLQ. During a network outage, lost ~11,480 Workflow starts that had to be manually recovered. | Pylon #26481, Slack |
| SailPoint | P1 from starting 1M workflows | Slack |
| Lovable | Billing backfill maxed 12K APS | Pylon #26539 |
| Intuit | Bursty starts, RESOURCE_EXHAUSTED | Community Slack |
| Redo | Rate limited on starts, production impact | Slack |

### Tier 3: Adjacent pain (batch observability, failure handling, rate pacing)

| Customer | Evidence | Source |
|---|---|---|
| Relativity | Context-aware rate limiting, batch completion events | Gong |
| Brex | Migrated from Kafka to Temporal | Slack |
| Q2 | Debugging/tracking failures in batch workflows | Gong |
| Neon | Batch workflow abstraction, completion visibility | Gong |
| Hebbia | Scheduling/prioritization feedback | Gong |
| Rivian | Batch workflow results in UI | Gong (Miro only) |
| Harvey | Per-fairness-key rate limits | Gong |
| Twilio | Bursty APS, DynamoDB throttling during batch | Gong |
| Zapier | Per-customer queues for isolation | Gong |
| Finch Legal | Hitting 500 TPS, wants 5-10x | Pylon #26491 |
| Lululemon | Prospect designing Kafka replacement | Slack |

### Community (OSS users, same pattern)

| User | Scale | Source |
|---|---|---|
| Vaibhav Gupta | 20K workflows batch, OOM | Community Slack |
| Aaron Redmond | 100M signalWithStart, wants batch API | Community Slack |
| Ivan Bautrukevich | 60M workflows/day | Community Slack |
| Andriy Lupa | Kafka consumer → 1M workflows/day via signalWithStart | Community Slack |
