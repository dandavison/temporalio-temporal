
How do kafka and sqs achieve high write throughput?

8:57 AM
Kafka and SQS take quite different architectural approaches to achieving high write throughput.

Kafka achieves it primarily through sequential I/O and partitioning. Messages are appended to an immutable, append-only log on disk, which turns random writes into sequential writes — something modern disks (especially SSDs) handle extremely fast. Throughput scales horizontally by splitting a topic into partitions, each of which can live on a different broker, so producers can write in parallel across the cluster. Kafka also batches messages on the producer side before sending them over the network, amortizing the cost of individual sends. Optional compression (gzip, snappy, lz4, zstd) further reduces network and disk overhead. Producers can also tune durability vs. speed with the acks setting — acks=0 or acks=1 trades some durability for lower latency per write.

SQS takes a managed, distributed approach. AWS runs SQS across many redundant hosts behind the scenes, so it can absorb a very high volume of SendMessage calls without any user-side scaling. There's no concept of partitions to manage; throughput scales automatically. For even higher throughput, SQS offers SendMessageBatch, which lets you send up to 10 messages in a single API call, reducing round trips. FIFO queues have a per-message-group throughput limit (up to 3,000 messages/sec with batching when high-throughput mode is enabled), while Standard queues have virtually unlimited throughput because ordering and exactly-once delivery aren't guaranteed — this relaxation of constraints is itself a throughput optimization.

In short, Kafka leans on sequential disk I/O, partitioning, and producer-side batching, while SQS leans on AWS's opaque distributed infrastructure, relaxed ordering guarantees (for Standard queues), and API-level batching. Kafka gives you more control over the trade-offs; SQS abstracts them away.

Tell me more about this part:

Kafka also batches messages on the producer side before sending them over the network, amortizing the cost of individual sends.

9:01 AM
Sure. Kafka's producer doesn't send each message immediately when send() is called. Instead, it accumulates messages in an in-memory buffer, grouped by target partition, and flushes them as a batch in a single network request. This is controlled by two main settings:

linger.ms — how long the producer waits before sending a batch. If set to 0 (the default), it sends as soon as a network thread is available, which in practice still allows some natural batching if messages arrive faster than they're sent. Setting it to, say, 5–10ms tells the producer to deliberately wait a bit and collect more messages before flushing.

batch.size — the maximum size (in bytes) of a single batch for a given partition. If a batch fills up before linger.ms expires, it's sent immediately. The default is 16KB.

So the actual send trigger is whichever comes first: the linger timer expires, or the batch reaches batch.size.

This matters for throughput because a single network round-trip carrying 500 messages is far cheaper than 500 individual round-trips. Each request has fixed overhead — TCP framing, request headers, broker-side processing, and the fsync or replication cost on the broker. Batching amortizes all of that across many messages.

There's a complementary setting, buffer.memory, which controls the total memory the producer can use for buffering across all partitions (default 32MB). If the buffer fills up — because the broker is slow or the network is congested — send() will block (or throw an exception after max.block.ms), which acts as natural backpressure.

When compression is enabled, it operates at the batch level, not the individual message level. This means a bigger batch compresses more efficiently, since the compression algorithm has more data to find redundancies in. So linger.ms and batch.size interact with compression in a positive way: bigger batches → better compression ratios → less network and disk I/O.

The trade-off is latency. A linger.ms of 0 gives you the lowest per-message latency but smaller batches. Increasing it adds a small delay to each message but can dramatically increase throughput. In practice, even a 5ms linger is often an excellent trade-off for high-throughput workloads.

Consider this message:

Netflix are thinking of adopting SQS in front of their workflow signals to avoid signaling a workflow too quickly. I've already given the advice
There's no inherent way to avoid accumulation of signals on a workflow @SignalMethod. Any backpressure would come from another mechanism e.g. SQS queueing. Or as mentioned here, sharding to lower the amount of signals any one workflow receives (though this still risks hitting limits in any one shard).
They're wondering if we're ever going to deliver a mechanism to rate limit / introduce backpressure on signals so they don't have to introduce a component / change strategy to support their high signal volume

Explain the concept of backpressure from first principles.

10:51 AM
Backpressure is a concept that comes up whenever you have a producer generating work faster than a consumer can process it. The fundamental question is: what happens to the excess?

There are only a few possible answers. You can drop the excess (lossy). You can buffer it somewhere (which just delays the problem unless the burst is temporary). Or you can slow the producer down so it doesn't outpace the consumer in the first place. That last option is backpressure — a signal that flows backwards, from consumer to producer, saying "slow down."

The term comes from fluid dynamics. If you have water flowing through a pipe into a container that's draining slowly, pressure builds up at the outlet and propagates back through the pipe, naturally resisting further flow. In software, there's no physical force doing this, so you have to build the mechanism explicitly.

The simplest example is a bounded queue. A producer puts items in, a consumer takes them out. When the queue is full, the producer's "put" call blocks — it physically cannot proceed until the consumer frees up space. The consumer's processing speed directly governs the producer's sending speed. That blocking is the backpressure.

In the Netflix/Temporal context you're describing, the problem is that there's no bounded queue between the signal senders and the workflow. Signals arrive at a workflow's @SignalMethod and accumulate without limit. Nothing tells the upstream callers "you're sending too fast." The workflow just keeps receiving signals, and if it can't process them fast enough, they pile up — potentially hitting platform limits.

Putting SQS in front of the workflow is essentially inserting that missing bounded queue (or at least a buffering layer with controllable drain rate). SQS doesn't give you true backpressure against the original producers — they can still enqueue freely — but it decouples the ingestion rate from the processing rate, letting a consumer pull from SQS at whatever pace the workflow can handle. It converts an unbounded push into a bounded pull.

The deeper architectural point is that any system where a producer can push unbounded work at a consumer without feedback is fragile at scale. Backpressure is the general principle that systems need a feedback loop from consumers to producers. Without it, your only options are unbounded buffering (eventually runs out of memory or hits limits) or dropping work — neither of which is usually acceptable.

OK, I get the concept of a blocking enqueue. I think asyncio queue is an  example? await queut.put(item)

10:53 AM
Yes, exactly. asyncio.Queue with a maxsize is a textbook example. When you call await queue.put(item) on a full queue, the coroutine yields control and doesn't resume until another coroutine calls queue.get() and frees a slot. The await is where the backpressure lives — it's the point where the producer is forced to wait.

One small note: if you create an asyncio.Queue() without specifying maxsize (or with maxsize=0), it's unbounded — put never blocks, and you have no backpressure. You'd just accumulate items in memory indefinitely, which is analogous to the Temporal signal problem. You need asyncio.Queue(maxsize=N) to get the blocking behavior.

And the typo — queut — I'm guessing you meant queue. Just mentioning it in case it's from code you're working on.

What about much more high scale use cases? I work for Temporal and we're discussing how to accept work from some of the highest throughput tech companies in the world. How might we use  "backpressure" to tell them to slow down? Do Kafka / SQS do something like this? If not, why not?

10:56 AM
This is a great question, and it gets at an interesting tension in distributed systems design.

At the scale you're talking about — where the producer is a remote client sending requests over the network — the mechanics of backpressure change significantly compared to an in-process bounded queue. You can't just block a coroutine. The producer is on a different machine, potentially in a different data center. So the "slow down" signal has to cross the network.

There are really two broad strategies: reject and retry or flow control at the protocol level.

Reject and retry is what most managed services do, including both SQS and Kafka (at the broker level). When you exceed a rate limit or the broker is overloaded, you get an error back — HTTP 429 in SQS's case, or a throttling error from Kafka's broker quotas. The producer is expected to back off and retry, typically with exponential backoff. This is "backpressure" in a loose sense: the consumer (the service) is telling the producer to slow down. But it's coarse — the producer already did the work of serializing, sending, and waiting for a response, only to be told "no." It's reactive, not preventive.

Kafka's broker-side quotas are a bit more nuanced. You can set byte-rate or request-rate limits per client ID or user. When a client exceeds its quota, the broker doesn't reject the request — it delays the response, effectively throttling the client by making it wait longer before it can send the next batch. This is cleverer because it naturally slows the producer down without triggering error-handling paths. But it's still fundamentally the broker absorbing the request and then choosing when to acknowledge it.

Protocol-level flow control is what you see in things like TCP's sliding window, HTTP/2 flow control, or gRPC flow control (which sits on top of HTTP/2). Here the receiver advertises how much data it's willing to accept, and the sender is physically unable to transmit more until the window opens up. This is true preventive backpressure — the producer is throttled before it sends, not after. But it operates at the transport layer, not the application layer. It tells you "stop sending bytes" — it doesn't understand anything about your messages, priorities, or business logic.

The reason Kafka and SQS don't do deep protocol-level backpressure is partly philosophical and partly practical. They're designed as buffers themselves — their whole job is to absorb bursts and decouple producers from consumers. If Kafka told producers to slow down aggressively, it would defeat the purpose of having Kafka in the first place. Producers are supposed to fire-and-forget into Kafka, and consumers pull at their own pace. The backpressure in Kafka's architecture lives between the consumer and the log, not between the producer and the broker.

For Temporal's case, the question is which role you want to play. If Temporal is the execution engine (not a buffer), then telling clients to slow down is legitimate and probably necessary. A few options on the spectrum:

You could do admission control with explicit rate feedback — reject with a 429 or equivalent, but include metadata like a retry-after header or a recommended rate. This gives clients actionable information rather than just "try again later."

You could do server-side queuing with drain control — accept the signals into a durable queue internally, but process them at a controlled rate per workflow. This is essentially building the SQS-like buffer into Temporal itself, which is what Netflix is asking for.

Or you could do cooperative flow control — something like a credit-based system where a workflow or namespace advertises how many signals it can accept per time window, and the SDK enforces that client-side. This is closer to the TCP window approach but at the application layer.

The honest reality is that most systems at this scale end up with the reject-and-retry pattern, because it's simple, stateless on the server side, and pushes the complexity to the client (where SDKs can handle it transparently). The fancier approaches are better in theory but add significant protocol complexity. The question for Temporal is probably whether the SDK can absorb that complexity so users don't have to think about it.






Claude is AI and can make mistakes. Please double-check responses.
