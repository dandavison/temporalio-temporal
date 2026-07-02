# From the Durable Data doc

Group 1 — Bulk control over existing Temporal executions

Need: act on large cohorts of workflows they already run (terminate, cancel, signal/unpause, reset) with per-object outcomes, failed-unit retry, and audit. The "side effect" is on Temporal's own objects, not primarily downstream business systems. No new work is created.

- SailPoint, Lovable (mass terminate)
- Rippling, Attentive (mass cancel)
- Intuit, Meta, Coupang (mass signal / unpause)
- Block/Afterpay, SEI, Samsara (batch reset / failed-unit retry)
- Stripe (compliance-grade bulk ops — named operation, acted-on set, evidence)

This is the V1 cluster and the clearest current pull. The defining trait is fleet operations, not data.

Group 2 — High-consequence side-effecting jobs with downstream writes

Need: run consequential work per business item (payment, claim, remit, ERP export) where each unit writes to an external system, blind reruns are unsafe, and they need idempotency, selected retry, repair, and provable evidence. This is your "each task has side effects calling downstream services" example — but specifically the high-value variant that justifies durable cost.

- Candid Health (remit processing / billing rules)
- Humana (failed claims + human repair, PHI/PII, audit)
- Brex (month-end ERP export, finance close)
- SoFi/Galileo (payment + reconciliation files, nested recovery)
- Instacart (payments/reconciliation at scale)
- Affirm (ingestion + reconciliation across Luigi/Airflow/Celery)
- Shopify (manual DB migrations, at-least-once concerns, repair)

This is the V3 strategic core. Distinguishing sub-trait: reconciliation/finance (SoFi, Instacart, Affirm, Brex, Candid) vs. regulated human-in-loop repair (Humana, Candid).

Group 3 — Per-item enrichment / transform pipelines (transform plus downstream effect)

Need: push many documents/files/records through a transform operation (OCR, chunk, embed, LLM-enrich) with per-item idempotency and retry of only the failed units. Closest to your "pure data transforms" example, but every one of these also has a side effect (writing to a vector store, calling a model), and several are flagged as economics warnings because per-item value is too low.

- Qualtrics (LLM document enrichment, millions of docs — V2 candidate)
- Rapidflare (ingest → chunk → embed → vector)
- Hebbia (data-room OCR + embed; unit = file)
- Snap (embeddings refresh / enrichment, model calls + downstream writes)
- Planet (imagery backfills; Dataflow/BigQuery keep the actual compute)
- ZoomInfo (high-volume enrichment generation — prices out, warning case)

Key tension the doc names: this group only fits when Temporal owns outcomes/retry/refs/evidence and explicitly does not become the compute engine or vector store, and only when per-item value clears the cost bar.

Group 4 — Bulk start / admission of new work from an input set

Need: start many executions of a known workflow type from a bounded input set, with duplicate-start prevention, per-start outcomes, and (for bursty cases) buffering/backpressure before durable execution is admitted.

- Vita (bulk start — the one unlocked V2 case)
- OpenAI (bulk workflow admission, burst-start backpressure — extension, not core)
- Apollo (simple delayed-data / API-call retry jobs — narrow customer-defined candidate)

Group 5 — Execution scheduling / fairness / capacity (infrastructure, not data)

Need: replace a custom scheduler that orders Temporal work by deadline, priority, and fairness keys with hierarchical concurrency limits and queued-work visibility. Distinct because it's about which work runs next, not repair/evidence of work.

- Rippling (ETA scheduler: deadline-priority, pool-level controls, task-slot visibility) — drives the V4+ "Deadline-Aware Execution Scheduling" module, not V3 repair.

Group 6 — Connector / multi-tenant scheduled syncs

Need: many schedules across tenants, per-tenant status, and safe retry. Sits between Group 1 and Group 2; worth separating because the unit is a scheduled sync run, not a payment or a document.

- Fivetran (connector syncs, schedule volume, multi-tenant status)

Group 7 — Economics warnings: cheap high-volume work that should stay elsewhere

Need (anti-pattern): very high volume, very low value-per-unit, safely rerunnable. The doc explicitly uses these to mark the boundary.

- Stripe webhooks, Dropbox, Daylight, Netflix-style action volume, ZoomInfo (cost), OpenAI (capacity/burst)

Note Stripe appears in two groups: its compliance bulk ops fit Group 1, but its webhook volume is a cost warning here.

---
Cross-cutting reads

- Deutsche Bank doesn't fit a workload group — it's an exec/architecture validation signal (audit-trail + history support), so I'd hold it as a credibility datapoint, not a need cluster.
- The two axes that actually separate these groups: (a) act-on-existing vs. create-new, and (b) consequence-per-unit. Groups 1/6 are act-on-existing; 2/3/4 create new. Groups 2 (high consequence) and 7 (negligible consequence) are the two poles the whole strategy is built to discriminate between.