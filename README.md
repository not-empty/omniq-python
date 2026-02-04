# OmniQ (Python)

**OmniQ** is a Redis + Lua, language-agnostic job queue. This package is the **Python client** for OmniQ v1.

Key ideas:

- **Hybrid lanes**: ungrouped jobs by default, optional **grouped** jobs (FIFO per group + per-group concurrency).
- **Lease-based execution**: workers reserve a job with a time-limited lease.
- **Token-gated ACK/heartbeat**: `reserve()` returns a `lease_token` that must be used by `heartbeat()` and `ack_*()`.
- **Pause / resume (flag-only)**: pausing a queue prevents *new reserves*; it does **not** move jobs or stop running jobs.

Core project / docs: https://github.com/not-empty/omniq

---

## Install

```bash
pip install omniq
```

---

## Quick start

### Publish

```python
from omniq.client import OmniqClient

uq = OmniqClient(
    host="omniq-redis",
    port=6379,
)

job_id = uq.publish(
    queue="demo",
    payload={"hello": "world"},
    timeout_ms=30_000,
)

print("OK", job_id)
```

### Consume

```python
import time
from omniq.client import OmniqClient

def handler(ctx):
    print("Waiting 2 seconds")
    time.sleep(2)
    print("Done")

uq = OmniqClient(
    host="omniq-redis",
    port=6379,
)

uq.consume(
    queue="demo",
    handler=handler,
    verbose=True,
    drain=False,
)
```

---

## Client initialization

You can connect using a **redis URL** or the standard host/port fields.

```python
from omniq.client import OmniqClient

# Option A: host/port
uq = OmniqClient(host="localhost", port=6379, db=0)

# Option B: Redis URL (recommended for TLS / auth)
uq = OmniqClient(redis_url="redis://:password@localhost:6379/0")
```

> The client automatically loads the Lua scripts on first use (or during init).

---

## Main API

All methods below are on `OmniqClient`.

### publish()

Enqueue a job.

```python
job_id = uq.publish(
    queue="demo",
    payload={"hello": "world"},     # must be dict or list
    job_id=None,                   # optional ULID (generated if omitted)
    max_attempts=3,
    timeout_ms=60_000,
    backoff_ms=5_000,
    due_ms=0,                      # schedule in the future (ms since epoch)
    gid=None,                      # optional group id (string)
    group_limit=0,                 # lazily initialize per-group limit (>0)
)
```

Notes:

- `payload` **must be** a `dict` or `list` (structured JSON). Passing a raw string is an error.
- If `gid` is provided, the job goes into that group’s FIFO lane. Group concurrency is controlled by `group_limit` (first writer wins).

---

### pause(), resume(), is_paused()

Pause/resume is a **queue-level flag**.

```python
uq.pause(queue="demo")
print(uq.is_paused(queue="demo"))  # True
uq.resume(queue="demo")
```

Behavior:

- Pause does **not** move jobs.
- Pause does **not** affect running jobs.
- Pause only blocks **new reserves** (reserve returns `PAUSED`).

---

## consume() helper

`consume()` is a convenience loop that:

- periodically runs `promote_delayed` + `reap_expired`
- calls `reserve()`
- runs your `handler(ctx)`
- heartbeats while the handler runs
- ACKs success / fail using the job’s `lease_token`

```python
uq.consume(
    queue="demo",
    handler=handler,              # handler(ctx)
    poll_interval_s=0.05,
    promote_interval_s=1.0,
    promote_batch=1000,
    reap_interval_s=1.0,
    reap_batch=1000,
    heartbeat_interval_s=None,    # None => derived from timeout_ms/2 (clamped)
    verbose=False,
    drain=True,                   # drain=True => finish current job on Ctrl+C then exit
)
```

### Handler context

Your handler receives a `ctx` object with:

- `queue`
- `job_id`
- `payload_raw` (JSON string)
- `payload` (parsed dict/list)
- `attempt`
- `lock_until_ms`
- `lease_token`
- `gid`

---

## Grouped jobs (FIFO + concurrency)

If you pass a `gid` when publishing, jobs are routed to that group.

```python
uq.publish(queue="demo", payload={"i": 1}, gid="company:acme", group_limit=2)
uq.publish(queue="demo", payload={"i": 2}, gid="company:acme")
```

- FIFO ordering is preserved **within** each group.
- Groups can run concurrently with each other.
- Concurrency **within** a group is limited by `group_limit` (or the queue default set in the core config).

---

## Queue configuration (v1 defaults)

OmniQ v1 supports per-job overrides and queue defaults such as:

- `timeout_ms` (lease duration)
- `max_attempts`
- `backoff_ms`
- `completed_keep` (retention size)

See the core docs for the full contract and configuration details:
https://github.com/not-empty/omniq

---

## License

See the repository license.
