## StartActivityExecution: Current Semantics, Async Dedup Variations, and Where the Latency Lives

### Current semantics: every response is definitive

Today, when `StartActivityExecution` returns, the caller knows exactly what happened. There are no ambiguous outcomes:

| Response | What the caller knows |
|----------|----------------------|
| ✅ New RunID returned | A new activity was created. This RunID is valid and will execute. |
| ✅ Existing RunID returned (request-ID match) | This is a retry of a previous successful request. The RunID from the original attempt is returned. Nothing new was created. |
| ✅ Existing RunID returned (USE_EXISTING) | An activity with this ID was already running. The caller got a handle to it. |
| ❌ Already running (FAIL policy) | A different request started an activity with this ID and it's still running. The caller's start was rejected. |
| ❌ Already completed (reuse rejected) | This Activity ID was used before and the reuse policy forbids recycling it. |

The key property: **every response is a commitment.** A returned RunID identifies an execution that exists and will not be discarded. An error means the start definitively did not happen. The caller can act immediately — describe, get result, cancel — with confidence.

### Where the latency lives in the flowchart

The first diamond — **"Does an activity with this activityID already exist?"** — is the expensive operation. This is the `select_current_execution` DB read (30-50ms p99 on large clusters, 50-80% of total start latency). **Every path through the entire flowchart must pass through this check**, including the happy path where no existing activity is found.

For a truly new activity (the common case for burst starts), this read is particularly wasteful: the answer is almost always "no," but the server must do a DB round-trip to confirm it, because it cannot distinguish "not in cache because it doesn't exist" from "not in cache because it was evicted."

Everything *after* that diamond — checking request ID match, checking running status, evaluating conflict/reuse policy — is in-memory work against already-loaded state. It's cheap.

The other expensive operations are the WAL writes at the terminal nodes (creating the activity), but those are the *write* cost. Async dedup targets the *read* cost.

### What async dedup changes

The proposal: skip the `select_current_execution` read entirely. Write to WAL immediately. Return a RunID. Resolve conflicts asynchronously (at flush time, or when the Element is later loaded from DB).

This breaks the "every response is a commitment" property. Specifically:

**Mechanism 1 (request-ID dedup) degrades.** If the client's first attempt succeeded but the response was lost, the retry would create a *second* execution with the same Activity ID and a *different* RunID (since there was no read to discover the first one). Both get written to WAL. Async dedup would later resolve this, but the client now holds a RunID from the retry that might be the one that gets discarded.

**Mechanism 2 (conflict policy) becomes unenforceable synchronously.** With FAIL policy, the server can't reject — it doesn't know anything is already running. With USE_EXISTING, the server can't return the existing RunID — it doesn't know it exists. The only possible synchronous response is "accepted, here's a RunID."

**Mechanism 3 (reuse policy) becomes unenforceable synchronously.** The server can't check whether a previous run completed successfully or not. REJECT_DUPLICATE and ALLOW_DUPLICATE_FAILED_ONLY cannot be evaluated at accept time.

In summary: under async dedup, `StartActivityExecution` can only say **"accepted"** — never "rejected" and never "here's the existing one." The caller gets a RunID that is provisional, not definitive.

### Variations and their trade-offs

**Variation A: Async dedup as a new conflict policy.** Add something like `ConflictPolicy.ASYNC` (or a start option `async_dedup: true`). The caller explicitly opts in to provisional semantics. All other conflict/reuse policy combinations continue to do the synchronous read. This preserves the clean contract as the default.

*Pro:* Backwards compatible. Callers who need definitive answers keep them.
*Con:* Two code paths to maintain. Callers who opt in still need a way to learn the outcome later.

**Variation B: New API (`EnqueueActivity` / `LazyStartActivity`).** A separate RPC with a different return type — perhaps an "acceptance token" rather than a RunID, making the provisional nature explicit in the type system. This could later extend to workflows (`EnqueueWorkflow`).

*Pro:* Cleanest separation. No ambiguity about what contract the caller is getting. The different return type prevents callers from accidentally using a provisional handle as if it were definitive.
*Con:* New API surface to design, document, and support in every SDK.

**Variation C: Deferred error delivery.** The start always returns a RunID. If async dedup later discards the execution, subsequent RPCs (Describe, GetResult) on that RunID surface the error (e.g., `EXECUTION_DISCARDED` or `EXECUTION_ALREADY_STARTED`). The existing execution's RunID could be included in the error.

*Pro:* The caller eventually gets the same information as today, just at a later point.
*Con:* Requires contract changes to Describe/GetResult. The window between start and error discovery is a period of false confidence. Roey's concern: *"the experience there is going to be pretty janky."*

### Why activities are more tolerant of this than workflows

Max's argument for restricting async dedup to activities (at least initially):

1. **Activities must be idempotent.** If your start is deduped in favor of an existing execution with the same Activity ID and identical input, the outcome is the same as if yours ran. You don't care which physical execution "won."

2. **Duplicates are expected.** Activity retries (from timeouts, worker crashes) are a normal part of the programming model. Callers already handle the case where "my attempt" and "the real execution" are different things.

3. **USE_EXISTING is the natural fit.** Most burst-start callers don't care which specific execution they get — they care that the logical work identified by the Activity ID happens exactly once. Under async dedup with USE_EXISTING semantics, the system guarantees exactly that; it just can't tell you *which* RunID won at accept time.

For workflows, none of these hold as strongly. Workflows are not required to be idempotent, their first workflow task may have side effects that depend on RunID or timing, and callers more often need a definitive handle to a specific execution.