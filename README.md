# Parcel Outbox

Keep an application database write and its outgoing webhook in one SQLite transaction. A separate worker claims events, signs requests, retries transient failures and moves exhausted deliveries to a dead-letter queue.

This is a small single-host implementation for understanding and using the transactional outbox pattern. It has no runtime dependencies beyond Python 3.11+ and SQLite. It has not been deployed or benchmarked in production.

## Run it

```sh
python -m pip install .
python -m unittest discover -s tests -v
```

Generate a local development signing key and keep it out of source control:

```sh
python -c 'import secrets; print(secrets.token_hex(32))'
```

Set `PARCEL_SIGNING_KEY_HEX` to that value in both terminals using your shell or secret manager. In the first terminal, start the local demonstration receiver from the repository:

```sh
python -m examples.receiver --db receiver.db --port 8765
```

In the second terminal:

```sh
printf '%s' '{"order_id":123}' | parcel-outbox --db events.db enqueue --key order:123
parcel-outbox --db events.db work --endpoint http://127.0.0.1:8765/hook --allow-loopback-http --once
parcel-outbox --db events.db status
parcel-outbox --db events.db inspect --state dead --limit 100
parcel-outbox --db events.db replay 1
```

Replay accepts only dead events. Omit `--once` to keep polling; SIGINT/SIGTERM stops new claims and allows the current delivery to finish. HTTP is allowed only for explicitly enabled numeric loopback destinations. Use HTTPS for other endpoints. The example receiver is a local development server, not an internet-facing service.

## Share the business transaction

```python
from parcel_outbox import Outbox

outbox = Outbox('application.db')
with outbox.transaction() as db:
    db.execute('INSERT INTO orders(id, amount) VALUES (?, ?)', ('123', 500))
    outbox.enqueue_in(db, 'order:123', {'order_id': '123', 'amount': 500})
```

Create the application table separately. `enqueue_in` never commits the caller's transaction. Reusing an event key with the same payload returns the existing ID; a different payload raises an error.

## Delivery and recovery

SQLite serializes claims with `BEGIN IMMEDIATE`. Each claim gets a generation token and an expiry. A worker that loses its lease cannot acknowledge or reschedule the newer claim. Crash-only attempts consume the same retry budget as explicit failures. All workers sharing a queue must use the same budget and destination.

Delivery is **at least once**. A receiver may accept a request before the sender dies or loses its lease. The receiver helper verifies the timestamped HMAC-SHA256 signature and atomically deduplicates the event key alongside application SQL writes. Callbacks must use the supplied database connection and must not commit or perform external side effects. Overlapping verification keys support rotation.

The sender retries network errors, 408, 425, 429 and 5xx responses with bounded full jitter. Other responses are terminal. It does not follow redirects or consume response bodies. Each send runs in a spawned child process so the parent can terminate blocked DNS or I/O after its deadline. This adds process startup overhead. Cleanup, OS scheduling and SQLite contention still prevent a hard real-time guarantee; generation fencing handles late completion.

Status and inspection output omit payloads, raw event keys and worker owners. Inspection uses keyset pagination with an ID watermark; state can change between pages. The SQLite files themselves contain application data and must be protected and backed up accordingly.

## Boundaries

- One trusted, operator-configured destination per queue. No arbitrary endpoint registration or SSRF sandbox.
- Local SQLite storage; no multi-host coordination or multi-region replication.
- No ordering guarantee across workers, receipt retention policy, scheduling priority or automatic dead-letter replay.
- The transport limits encoded payload size after serialization; callers must bound input objects. The CLI and receiver example also bound incoming bytes.
- The receiver example rejects duplicate security headers and chunked requests. Production deployments need their own hardened HTTP server, TLS and egress controls.

See [DESIGN.md](DESIGN.md) for protocol details and tradeoffs. Tests cover concurrent claims and receipts, transaction rollback, migrations, restart/retry boundaries, stale leases, process deadlines, real HTTP delivery and subprocess CLI shutdown.
