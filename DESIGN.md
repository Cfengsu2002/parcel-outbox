# Parcel Outbox

A durable webhook delivery service for applications that need to publish events without losing the connection between a business write and its outgoing notification.

## Current implementation

The local prototype implements caller-owned atomic business/event transactions, SQLite-backed event insertion, idempotent event keys, exclusive claims, expiring leases and generation-fenced acknowledgements. SQLite serializes writers with `BEGIN IMMEDIATE`; the concurrency test uses eight independent connections. Explicit timestamps make expiry boundaries deterministic in tests.

Delivery will be at least once. A generation token prevents a stale worker from changing local state after another worker reclaims a lease; it cannot retract an HTTP request already sent. Consumers must deduplicate using the event key.

## Implemented scope

The library, worker, CLI, metadata inspection, signature verifier and local HTTP receiver example are implemented. The receiver example rejects duplicate security headers, oversized or truncated bodies, unsupported transfer encoding and invalid content types. Tests exercise actual local HTTP and spawned sender processes. CI runs the suite on Python 3.11–3.14.

Management is local CLI only; no remote management API is exposed. Event cancellation, automatic receipt retention and multi-destination routing are outside this version. No service has been deployed, and no throughput or production reliability claims have been measured.

## Transport boundary

Endpoints are trusted operator configuration, not arbitrary customer input. HTTPS is required; an explicit development option allows numeric loopback HTTP addresses. Credentials, fragments and control characters are rejected. This is not an SSRF sandbox: DNS, private-network destinations and egress policy must be constrained by deployment before supporting untrusted endpoint registration.

Receivers verify `Parcel-Signature: v1=<hex>` as HMAC-SHA256 over the exact `Parcel-Timestamp` ASCII value, a dot, and the raw request body. They must enforce a timestamp tolerance and deduplicate the event key. The JSON envelope contains `event_key` and `payload`; `Idempotency-Key` is the SHA-256 hex digest of the UTF-8 event key. The receiver helper supports overlapping verification keys for rotation, enforces timestamp tolerance, rejects malformed envelopes and duplicate JSON fields, and records receipts atomically with business updates in SQLite.

The transport reports 2xx success; 408, 425, 429, 5xx and network/protocol failures are retryable. Other statuses, including redirects, are terminal for the worker. It performs one request only; scheduling and durable attempt budgets belong to the worker. The configured timeout bounds individual blocking socket operations, not total wall-clock delivery time or DNS resolution. The CLI wraps this sender in process-isolated `DeadlineDelivery`, described below. Payload size is checked before network I/O, but JSON serialization currently allocates the encoded body before the size check.

## Worker lifecycle

A worker claims one event at a time, sends once, then commits success or schedules a retry. It passes one attempt budget to both claim and failure operations; all workers sharing a queue must use the same budget. Crashes consume attempts because the receiver may have accepted a request before the sender stopped. Exhausted expired claims are dead-lettered transactionally before another event is selected. The worker polls without claiming new work after its stop event; in-flight delivery is allowed to finish. A stop waits for the current process-isolated delivery deadline rather than interrupting an accepted request. No lease heartbeat exists yet, so deployments must account for the documented transport deadline limitations before relying on a lease duration.

## Receiver transaction boundary

`Receiver.receive` verifies the exact raw bytes before opening a write transaction. It serializes concurrent receipts with `BEGIN IMMEDIATE`, compares canonical payloads for repeated event keys, and calls the application only for a new event. Its callback receives the same database connection and must perform only transactional SQL writes on that connection. It must not commit, roll back, open an independent business transaction, or perform network side effects. A callback exception rolls back both the receipt and its business writes. A callback that commits violates this trusted API contract; the post-callback check detects the violation but cannot undo an already committed transaction.

Deduplication is scoped to one receiver database and event-key namespace. Use separate databases or globally unique event keys for different producers. Receipts currently have no retention policy: deleting one permits a later replay to apply again. HTTP adapters must enforce the request-body limit while reading, before handing the full byte string to the verifier. Reject duplicate signature/timestamp headers at the HTTP boundary. Clock synchronization and an appropriate tolerance are operational requirements.

## Deadline isolation

`DeadlineDelivery(HttpDelivery(...), deadline_seconds=10)` runs each send in a fresh spawned process and waits against a monotonic deadline. DNS resolution and socket/header reads happen in that process. On deadline or completion, the parent reaps the process; terminate and kill fallback each have a 0.2-second join allowance. A child crash is retryable. Input rejection returns a fixed message without forwarding the original exception. Secrets travel only through local process startup state, not command-line arguments or logs.

The worker requires its lease to exceed this deadline by more than one second for cleanup and database acknowledgement. OS scheduling, process startup/serialization and SQLite writer contention can still exceed this margin; generation fencing remains the correctness boundary, not a hard real-time guarantee. Killing the local sender cannot retract a request already accepted remotely, so receiver deduplication is mandatory. This approach trades per-request process startup cost for isolation without leaking a pool of blocked DNS threads. The application entry point must use the standard `if __name__ == '__main__'` guard for spawn. The CLI always uses this wrapper.

## Command line

Run from the project directory with Python 3.11 or newer:

```sh
python -m parcel_outbox --db ./events.db enqueue --key order:123 < event.json
python -m parcel_outbox --db ./events.db status
python -m parcel_outbox --db ./events.db work --endpoint https://receiver.example/hooks --once
python -m parcel_outbox --db ./events.db replay 1
```

The enqueue input is the JSON payload, without the transport envelope. Set `PARCEL_SIGNING_KEY_HEX` through the deployment secret store or shell environment to a hex-encoded key of at least 32 random bytes. No secret CLI argument is accepted. A worker without `--once` polls until SIGINT/SIGTERM; it stops claiming and lets the current bounded delivery finish. Worker configuration is checked before opening the queue. Status opens SQLite read-only and returns aggregate counts, never payloads or event keys. Errors intentionally omit input and low-level exception details. `--allow-loopback-http` is for local receiver development only.

The CLI is single-process/single-worker. Multiple invocations can share a SQLite queue using the same delivery attempt budget. Sender routing is one configured endpoint per worker; running workers for different destinations against the same database is unsupported. Package installation, a local receiver example and metadata inspection are included.

## Queue inspection

`python -m parcel_outbox --db events.db inspect --state dead --limit 100` returns only local IDs, attempts, generations, scheduling timestamps and derived state. Continue with `--after <next_after> --through <through>` until `next_after` is null. The first page captures a maximum ID so newly enqueued events cannot extend the traversal indefinitely. Each page is internally consistent, but this is not a multi-page snapshot: deliveries, retries and replay can change state between pages. A filtered traversal may therefore omit an event that moves into the filter behind the cursor. Rerun inspection to refresh operational state.

Limits are capped at 500 records. State labels are delivered, dead, leased, waiting and ready. An expired lease is ready if its retry timestamp has arrived; an exhausted attempt budget is finalized by the next claim operation, not by read-only inspection. The command intentionally exposes neither payloads nor raw event keys nor lease owners. Replay of a missing database now fails without creating an empty queue.
