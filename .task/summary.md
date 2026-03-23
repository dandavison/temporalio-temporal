## Analysis of TemporalFS Proposal Documents

### The Problem

AI agent workloads on Temporal generate and depend on filesystem state (code repos, build artifacts, datasets, configs), but Temporal has no native primitive for durable files. Today's workarounds are all bad:

1. **Ephemeral scratch** — files on worker-local disk, lost on failure or worker migration
2. **External storage (S3/GCS)** — no consistency with workflow state, no replay determinism, manual sync code
3. **Serialize into payloads** — explodes payload sizes, no random access, impractical for real workspaces

The market research doc ("State of Agent Sandboxes") validates this thoroughly: the sandbox ecosystem (E2B, Daytona, Fly.io Sprites, Modal, etc.) has solved isolation and compute, but **application-level durability — crash recovery with execution continuity, replay, multi-session coordination — remains an open gap**. This is exactly Temporal's sweet spot.

### How Many Distinct Solutions Have Been Proposed?

**Two fundamentally different architectures** are on the table:

#### 1. TemporalFS — Server-Side CHASM Archetype (1-pager, PRD, Design doc)

A new first-class CHASM archetype (`temporalfs`) where **filesystem state lives server-side**. Every file operation (read, write, mkdir, etc.) is a gRPC call from the SDK to the Temporal server, which applies it to an inode-based storage engine (PebbleStore for OSS, WalkerStore with S3 tiering for Cloud). The FUSE mount on the worker translates POSIX syscalls into these RPCs.

Key properties:
- Independent lifecycle (outlives any single workflow)
- Multi-workflow sharing (P2)
- Every mutation is a CHASM transition with MVCC versioning
- Replay determinism via transition-pinned snapshots
- Pluggable storage: `FSStoreProvider` interface (Pebble local / Walker+S3 for Cloud)
- Close-to-open consistency (writes buffered locally, flushed on `close()`)
- Synced directory fallback for environments without FUSE

#### 2. Durable Workspace — SDK-Side Overlay FS (Durable Workspace Design doc)

A **workflow-scoped** filesystem that lives entirely on the worker's local disk during activity execution, with **diffs captured as chunked tar archives and stored via External Payload Storage** (S3, GCS, local). No new server-side storage engine — workspace metadata (version counter + diff claims) is stored in `ExecutionInfo` alongside existing workflow state.

Key properties:
- All file I/O is local (FUSE overlay: lower layer = accumulated state, upper layer = current changes)
- Diffs uploaded only on activity completion (atomic with `ActivityTaskCompleted` event)
- Lazy loading: only manifests downloaded upfront; file chunks fetched on demand via FUSE interception
- Chunked tar format compatible with client-side encryption (`PayloadCodec`)
- Suspend/resume of FUSE mounts between activities
- Multiple SnapshotFS backends: FuseOverlayFS (recommended), TarDiffFS, ZfsSnapshotFS, BtrfsSnapshotFS
- **Already implemented** with working code across `api`, `api-go`, `temporal`, `sdk-go`, `cli`, and `samples-go` repos

### Is There Consensus?

**Partial consensus on the product vision, but no clear resolution on the architecture.**

**Agreed points** (from comment threads on the 1-pager):
- P1 should be single-workflow (one workflow + its activities), multi-workflow sharing deferred to P2 (Paul, Johann, Moe all aligned)
- FUSE mount is the right primary access mode, not a programmatic API (Roey's feedback, accepted)
- Synced directory fallback needed for Lambda/unprivileged containers (Sergey's feedback, accepted)
- Close-to-open consistency is the right model (not per-operation linearization) — Johann's NFS analogy was decisive
- Encryption and compression must be designed in from day 1 (Johann's feedback)
- Direct-to-S3 for large chunks to avoid double-egress (Johann's feedback from large payload project)
- Fork/branch model is a compelling approach for P2 concurrency (Johann's proposal, captured but not finalized)

**Unresolved tension**: the 1-pager/PRD/Design doc describe a server-side architecture, but the Durable Workspace doc describes an SDK-side architecture that is **already built and working**. The documents don't explicitly compare these two approaches or explain the relationship between them. There's no document that says "we evaluated both and chose X because Y."

The latest comment (2026-03-23) on the PRD is about compression before encryption — a detail-level discussion, suggesting the PRD is being actively reviewed but the architectural choice is still being iterated on.

### Are They on the Right Path?

**The problem identification is excellent.** The market analysis is thorough and well-sourced. The "durability gap" framing — sandbox vendors solve isolation but not application-level durability — is precisely the right way to position this. Temporal genuinely has a unique advantage here.

**The product vision is sound.** The developer experience (`Create()` + `Mount()`, unmodified programs work, files survive failures) is the right target. The phasing (P1 = single workflow, P2 = multi-workflow) is appropriately scoped.

**However, I have significant concerns about the TemporalFS server-side architecture:**

**1. Per-operation gRPC latency is the elephant in the room.** In the CHASM archetype design, every `open()`, `read()`, `write()`, `close()`, `stat()`, `readdir()` from any program running in the agent's workspace becomes a gRPC round-trip to the Temporal server. Even with write buffering (close-to-open), reads are still RPCs. A `git clone` of a modest repo involves tens of thousands of syscalls. The PRD targets P95 read latency of <10ms (cached) — but that's per-chunk, and a single `ls -la` in a directory with 100 files requires 100+ `Getattr` calls. The write-buffering helps writes, but read-heavy workloads (which are extremely common — builds, tests, linters all read far more than they write) will suffer badly.

The Durable Workspace approach avoids this entirely: all reads are local disk I/O (~microseconds), and the only network cost is at activity boundaries. This is an enormous practical advantage.

**2. The CHASM archetype approach conflates two concerns.** TemporalFS as described is both a *storage engine* (inodes, chunks, LSM compaction, bloom filters, GC) and a *durability/versioning layer* (transitions, replay, snapshots). The Durable Workspace approach separates these: local filesystem for the storage engine (let the OS do what it's good at), external payload storage for durability. This separation of concerns is cleaner and leverages existing, battle-tested infrastructure.

**3. History shard hot-spotting is acknowledged but not resolved.** The Design doc's own "Open Questions" section flags this: all operations for one FS execution hit the same history shard. For an AI agent doing a `git clone` + build + test cycle, this could be thousands of operations per second hitting a single shard. The proposed mitigations (larger shard count, batching) are hand-wavy. The Durable Workspace approach has no such bottleneck — it only touches the server on activity completion.

**4. The Durable Workspace is already built; the CHASM archetype is still on paper.** The Durable Workspace doc shows working code across six repos with tested lazy loading, suspend/resume, chunked encryption-compatible storage, and end-to-end demos. The CHASM archetype design is detailed but unimplemented (beyond the standalone `temporal-fs` library). Shipping matters.

**5. The server-side approach introduces significant operational complexity.** A dedicated PebbleDB instance (or Walker shardspace) for FS data, WAL recovery, flush intervals, manifest compaction tasks, chunk GC tasks, quota enforcement tasks, data cleanup tasks — this is a substantial new subsystem to operate. The Durable Workspace approach reuses the existing External Payload Storage driver and adds zero new server-side infrastructure.

### What's Being Overlooked?

**1. No head-to-head comparison of the two architectures.** This is the most critical gap. The team has two very different approaches and no document that evaluates them against each other on the dimensions that matter: latency, complexity, operational burden, shipping timeline, and path to multi-workflow sharing. The 1-pager/PRD/Design are written as if the CHASM archetype is the only option; the Durable Workspace doc is written independently. Someone needs to write the comparison.

**2. Benchmarks with realistic workloads are absent.** The PRD lists latency targets but no measurements. Before committing to either architecture, the team needs numbers: `git clone` of a 10K-file repo, `npm install`, a Python test suite — measured against both architectures. The per-operation gRPC cost of the server-side approach might be a dealbreaker, or it might be fine with caching. Nobody knows yet.

**3. The "synced directory" mode is underspecified.** The 1-pager describes it as an `fsnotify`-based fallback, but `fsnotify` has known limitations: it doesn't catch all filesystem events reliably across platforms, it can miss rapid sequences of events, and it doesn't work at all inside some container runtimes. If this is the fallback for Lambda/serverless (a major deployment target for AI agents), it needs much more design attention.

**4. The path from Durable Workspace to multi-workflow sharing isn't explored.** A common argument for the server-side approach is that it naturally supports multi-workflow sharing. But has anyone analyzed what it would take to extend the Durable Workspace approach? For example: a lightweight coordination service that manages workspace leases + a shared blob store could provide multi-workflow access without the per-operation RPC overhead. The assumption that "we need server-side storage for sharing" deserves scrutiny.

**5. Cost modeling is missing.** The PRD mentions billing dimensions (GB-month storage, per-1K ops) but doesn't model actual costs. An AI coding agent doing a build+test cycle might generate millions of FS operations. At what per-op price does this become cost-prohibitive relative to alternatives? The Durable Workspace approach, which charges only for payload storage and activity completions, might be dramatically cheaper.

**6. No user research is cited.** The 1-pager claims "25%+ customers requesting" (the highest demand rating) but no specific customer names, quotes, or use cases are provided. One commenter explicitly asked for this. The "State of Agent Sandboxes" doc provides market context but not Temporal-specific customer evidence.

### What Implementation Work Is Required?

For the **CHASM Archetype (TemporalFS)** approach, as described in the Design doc:

1. **Proto definitions** — New service protos, state protos, task protos (api, api-go, temporal repos)
2. **CHASM archetype** — Filesystem component, state machine, library registration, FX wiring
3. **FSStoreProvider + PebbleStore** — Pluggable interface + OSS implementation with FNV-1a partitioning
4. **FS operations API** — History service handler for all POSIX-mapped gRPC ops (Lookup, Read, Write, Mkdir, etc.)
5. **Frontend routing** — Validate + route FS RPCs to correct history shard
6. **Go SDK** — FUSE-to-gRPC bridge, synced directory mode, chunk cache, replay integration
7. **GC/compaction tasks** — Chunk GC, manifest compaction, quota enforcement, owner lifecycle, data cleanup
8. **SaaS integration** — CDSStoreProvider (Walker), WAL engine, flusher, ShardspaceTemporalFS
9. **Walker S3 tiering** — Prerequisite for Cloud deployment (separate project)
10. **Integration tests** — Replay correctness, failure scenarios, cross-worker reconstruction

Timeline target: P1 in Q3 2026, P2 (multi-workflow + Python/TS SDKs) in Q4 2026.

For the **Durable Workspace** approach, remaining work from the design doc:

1. FUSE mount lifecycle management (startup cleanup for stale mounts)
2. Pre-activity rollback (handle timeout dirty state)
3. Sticky worker affinity
4. Diff compaction (consolidate accumulated diffs)
5. Workspace cleanup on workflow close
6. Sequential access enforcement
7. S3/GCS StorageDriver implementations

The Durable Workspace has substantially less remaining work since the core is already implemented.

### Bottom Line

The team has correctly identified a real and important problem. The product vision and phasing are sound. The market analysis is thorough. But they are carrying two architectures without a clear decision framework. The server-side CHASM approach is more ambitious and enables multi-workflow sharing more naturally, but introduces per-operation network latency, operational complexity, and a longer shipping timeline. The SDK-side Durable Workspace is simpler, already works, and provides better single-activity performance, but has a less obvious path to multi-workflow sharing.

My recommendation: **ship the Durable Workspace as P1** (it's built, it works, it solves the core problem), then evaluate whether the CHASM archetype is needed for P2 multi-workflow sharing, or whether a lighter coordination layer on top of the workspace model could achieve the same goal. The worst outcome would be spending another quarter building the server-side approach from scratch while the working SDK-side implementation sits on a branch.
