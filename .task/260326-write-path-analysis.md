## Buffering & Batching: Write-Path Analysis

### The Core Problem

Temporal's sync write path for `StartWorkflowExecution` performs substantial work per accepted request:

1. **Frontend**: interceptor chain (rate limiting, auth, namespace validation, etc.)
2. **Frontend→History RPC**: shard ID computation (`FarmHash32("{nsID}_{wfID}") % shards + 1`), membership lookup, gRPC hop
3. **History**: shard controller ownership check, execution cache lookup/miss, workflow context lock acquisition
4. **History**: mutable state creation, history event generation, transfer/timer/visibility task generation
5. **Persistence (Cloud)**: three-phase WAL write sequence (LPWAL → HEWAL → MSWAL) to BookKeeper/BOSS, each involving serialization, `ReliableWriter.Write()`, and future-wait with timeout
6. **Post-write notifications**: notify queue processors of new tasks (async from here — task dispatch to Matching is not on the sync write path)

The cost is dominated by persistence IO:
- **WAL writes**: at minimum 2 WAL writes (HEWAL + MSWAL), each a BookKeeper append with durability guarantee
- **Dedup DB read**: for executions with user-provided IDs, `select_current_execution` adds 30-50ms at p99, accounting for 50-80% of total `StartWorkflowExecution` latency on larger clusters

By contrast, Kafka/SQS achieve high write throughput by doing essentially one thing: append to a durable log. To match that throughput, Temporal would need to reduce its acceptance path to something comparably cheap.

### The Capacity Problem

Temporal Cloud cells are provisioned with ~50% headroom for normal customer traffic spikes. The buffering/batching use case demands absorbing 10x-1000x normal traffic in seconds (e.g., OpenAI: 1M starts "in seconds"). Routing this volume through the existing cell infrastructure — Frontend, History, CDS WAL, shard locks — would destabilize the cell for all tenants.

The fundamental tension: **Temporal's write path is optimized for correctness and durability of individual executions, not for bulk ingestion throughput.** Every start creates a full execution with mutable state, history events, and internal tasks — even when the actual work won't execute for minutes or hours.

---

### Technical Directions Proposed

#### Direction 1: External Buffer in Front of Cells (Tushar)

Tushar argues that buffering must happen *outside* the cellular infrastructure entirely. The cell's WAL, History service, and Frontend lack the spare capacity to absorb burst spikes without disrupting other tenants.

His proposed architecture: an external, multi-tenant, elastically scalable queue (candidates: S3, Kinesis, or a purpose-built WAL with different characteristics than the cell WAL) sits in front of the cell. Requests are durably captured there, then drained into the cell at a pace matching available capacity.

> *"We need to design for the future where we have somewhat infinitely scalable and elastic queue in front of our cells, which can absorb these loads and we can process them slowly"* — Tushar

> *"My WAL in the cell run with just enough capacity so we can absorb some spikes from the customer. It is not designed to absorb 10x-20x capacity in matter of seconds."* — Tushar

> *"S3 today is practically unlimited capacity. Not because they are very elastic. Only because they are multi tenant and no single customer spike can hit them fast enough to overwhelm their system."* — Tushar

**Write-path implication**: This direction introduces a new persistence layer *before* the Frontend. The sync write path from the client's perspective becomes: write to external buffer → return success. The existing write path (Frontend → History → CDS) becomes the *drain* path, invoked asynchronously at controlled rate. This is architecturally the cleanest separation but requires building and operating a new storage tier.

#### Direction 2: Flow Control on Ingestion (Max)

Max is skeptical of adding another persistent queue. He proposes flow control mechanisms that let Temporal *pace* consumption from the customer's existing data source (Kafka, DB, filesystem) rather than accepting and buffering everything.

> *"I'm not convinced that adding another persistent queue is the right answer. Temporal is already scalable and pretty efficient."* — Max

> *"We can just introduce a rate limiter on ingestion. Not rate limiter, but flow controller."* — Max

Concretely, Max proposes:
- A flow-control API that slows down `StartWorkflow` calls (backpressure rather than rejection)
- An iterator/consumer pattern where Temporal pulls from the customer's data source at a controlled rate

**Write-path implication**: The existing write path is unchanged. The bottleneck is managed by controlling the *rate* at which requests enter the path, not by making the path cheaper. This is the lowest-engineering-cost option but doesn't address customers who want fire-and-forget semantics ("just take my million starts and deal with them").

#### Direction 3: Deferred Execution — Start Without Workflow Task (Max)

Max proposes a "start that doesn't create a workflow task" — the workflow is durably created in History/CDS but no transfer task is generated to push work to Matching. A separate controller then initiates execution at a controlled rate.

> *"I think we should add start that doesn't create a workflow task. Then have a controller that initiates execution of workflows in a rate or parallelism controlled manner."* — Max

**Write-path implication**: This still traverses the full write path (Frontend → History → CDS WAL) for each start, but reduces post-write cost (no transfer task dispatch). The critical question is whether the write path itself has sufficient throughput. Max argues it does:

> *"Putting start records to the WAL should be pretty efficient"* — Max
> *"If you have 200k actions capacity then you can process more than 200k starts per second"* — Max

Tushar disagrees sharply:

> *"There is not a single cell which has 200K actions spare capacity... Our spare capacity is designed for normal spikes from customers. Not for 100x-1000x spikes from single namespace."* — Tushar

This is an empirical question about cell headroom, but Tushar's operational knowledge of actual cell capacity carries significant weight here.

Roey adds that the Matching backlog already serves as a queue:

> *"But workflow tasks go to matching backlog, that's already a pretty good queue. And you get priority and fairness out of the box."* — Roey

#### Direction 4: Priority-Based Buffering via Task Queue Priority (Yimin)

Yimin proposes leveraging the existing TQ priority feature: mark buffered workflows as low priority so their starts and task dispatch don't compete with live traffic.

> *"Now we have TQ priority, I could imagine we leverage that feature to mark those ok-to-be-buffered workflows as low priority. Then we really just need to also treat to-be-buffered StartWorkflowExecution API calls as low priority so spike of them won't impact other normal live traffic, then we essentially has a buffer feature."* — Yimin

**Write-path implication**: The write path itself is unchanged — every start still does the full Frontend→History→CDS sequence. The "buffering" is achieved by deprioritizing these starts at the namespace rate-limit layer (so they yield to normal traffic) and at the Matching layer (so their tasks are dispatched last). This could handle moderate spikes using unused namespace capacity but, as Yimin acknowledges:

> *"This may be good enough to solve that occasional spikes (as long as they are not too crazy), but won't be enough to solve the large backfill case."* — Yimin

#### Direction 5: Async Dedup for Standalone Activities (Max, Roey)

For the SAA (Standalone Activity) path specifically, Max proposes eliminating the synchronous dedup read — the `select_current_execution` DB call that accounts for 30-50ms p99 on large clusters.

> *"We could also eliminate a db read if we can dedupe them asynchronously."* — Max

The idea: accept the start optimistically (write to WAL without checking for ID conflicts), then detect and resolve duplicates asynchronously. This assumes duplicate requests carry the same payload — which Max argues is the common case (retries with same input).

> *"I don't see much value in rejecting start standalone activity synchronously. Most users will choose to drop duplicated starts for a given ID. Most such starts come from retries with the same input anyway."* — Max

Roey notes this would be a new semantic:

> *"That would be a new semantic. Was thinking optimistic start would cover that already."* — Roey

**Write-path implication**: This directly attacks the most expensive read in the write path. For SAA on the CDS path, the sync write sequence would become: WAL writes only (no DB read for dedup). The dedup check moves to the flusher or a background process. This could reduce SAA start latency by 50-80% on large clusters. However, it changes the API contract: `StartActivityExecution` would no longer synchronously reject duplicates.

#### Direction 6: COOLCAT / Collection Primitive (Paul)

Paul proposes a CHASM archetype ("COOLCAT") that manages collections of operations. Users would submit operations into a collection, which handles tracking, throttling, and execution.

**Write-path implication**: If operations are submitted as items within a COOLCAT collection, the write path per item depends on how the collection is persisted. Paul envisions this as a CHASM component, which means each item addition would go through the CHASM write path (History → CDS WAL). Tushar challenges whether this is sufficiently cheaper than full `StartWorkflow`:

> *"CHASM is not 10%. It is basically almost a cost of full action."* — Tushar

Yimin had hoped for 5-10% cost:

> *"I was hoping we need 10% or 5% capacity to buffer the request compare to fully process them."* — Yimin

Additionally, Roey identifies a practical constraint:

> *"The collection idea is subject to the 4mb grpc limit. If your workflow inputs are large, you might not be able to submit more than a handful at a time."* — Roey

This direction is more relevant to the *batch management* problem (tracking groups of operations as a unit) than to the *write-path throughput* problem.

---

### Analysis: Mapping Directions to the Write Path

The write-path architecture reveals a key structural constraint: **every operation that enters the History service acquires a shard-level IO semaphore and write lock** (`context_impl.go:628-688`). Even if individual WAL writes are fast, the shard lock serializes writes within a shard, and the IO semaphore bounds concurrent persistence operations per shard. Bulk ingestion for a single namespace will hash across shards (since shard = `hash(nsID_wfID) % N`), providing some parallelism, but the per-shard serialization remains a throughput ceiling.

The CDS write sequence compounds this: `runOperation` (`execution_runner.go:98-249`) acquires the Element's IO lock, then performs up to 3 sequential WAL writes (LPWAL → HEWAL → MSWAL), each waiting on a BookKeeper durability future. The total per-operation latency floor is the sum of these WAL write latencies.

For a "cheap accept" that approaches queue-like throughput, the minimum viable write path would need to:
1. Skip or defer the Frontend→History RPC hop (or batch multiple operations into a single RPC)
2. Skip or defer mutable state creation and history event generation
3. Perform a single WAL append (not the current 2-3 WAL sequence)
4. Skip or defer dedup reads

This is essentially Tushar's "external buffer" direction — the only way to match queue throughput is to bypass the execution write path entirely for the acceptance phase.

The intermediate directions (Max's deferred execution, Yimin's priority-based buffering) don't change the per-operation write-path cost but manage the *rate* at which operations enter it. These are viable for moderate burst absorption but structurally cannot reach queue-like ingestion throughput.

Async dedup (Direction 5) is orthogonal and valuable regardless of which buffering direction is chosen — it reduces the per-operation write-path cost for SAA, benefiting both buffered and non-buffered starts.

---

### Summary

**Problems:**
- Temporal's sync write path (Frontend → History → CDS 3-WAL sequence) is too expensive per-operation to match queue throughput; the cost is dominated by persistence IO: 2-3 durable WAL writes plus a dedup DB read
- The dedup read (`select_current_execution`) alone adds 30-50ms p99, representing 50-80% of start latency on large clusters
- Cloud cells lack spare capacity to absorb 10x-1000x burst spikes without impacting other tenants; cells are provisioned with ~50% headroom for normal spikes
- No mechanism exists to accept work durably without immediately paying the full execution-creation cost
- Rate limiting rejects rather than buffers, forcing customers to build external queues (Kafka, SQS, Redis)

**Proposed directions:**
- **External buffer before cells** (Tushar, Paul): durable queue (S3, Kinesis, or dedicated WAL) outside cell infrastructure; drain into cell at controlled rate. *"We need to design for the future where we have somewhat infinitely scalable and elastic queue in front of our cells"* — Tushar
- **Flow control / backpressure on ingestion** (Max): pace consumption from customer's data source rather than buffering; *"We can just introduce a [flow controller] on ingestion"* — Max. Lowest engineering cost but doesn't address fire-and-forget use case
- **Start without workflow task + controlled drain** (Max): create execution in History but defer task dispatch; controller initiates execution at controlled rate. Disputed: Max believes cell WAL has capacity; Tushar says it does not
- **Priority-based buffering** (Yimin): use TQ priority to mark buffered starts as low priority; use namespace's unused capacity. *"Good enough for occasional spikes but won't be enough for the large backfill case"* — Yimin
- **Async dedup for SAA** (Max, Roey): skip synchronous `select_current_execution` read; dedupe asynchronously. Could cut SAA start latency by 50-80%. *"Most users will choose to drop duplicated starts for a given ID"* — Max. Roey: *"That would be a new semantic"*
- **COOLCAT collection primitive** (Paul): CHASM archetype for bulk operation management. Tushar and Yimin challenge whether CHASM write cost is sufficiently cheaper than full start. More relevant to batch management than write-path throughput
- **Matching backlog as implicit buffer** (Roey): *"Workflow tasks go to matching backlog, that's already a pretty good queue. And you get priority and fairness out of the box."* — relevant if starts can be made cheap enough to reach Matching