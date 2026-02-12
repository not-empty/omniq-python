# OmniQ (Python)

**OmniQ** is a Redis + Lua, language-agnostic job queue. This package is
the **Python client** for OmniQ v1.

Key ideas:

-   **Hybrid lanes**: ungrouped jobs by default, optional **grouped**
    jobs (FIFO per group + per-group concurrency).
-   **Lease-based execution**: workers reserve a job with a time-limited
    lease.
-   **Token-gated ACK/heartbeat**: `reserve()` returns a `lease_token`
    that must be used by `heartbeat()` and `ack_*()`.
-   **Pause / resume (flag-only)**: pausing a queue prevents *new
    reserves*; it does **not** move jobs or stop running jobs.
-   **Admin-safe operations**: strict `remove`, `remove_batch`, `retry`,
    and `retry_batch` operations.
-   **Handler-driven completion primitive**: `check_completion` for
    parent/child workflows.

Core project / docs: https://github.com/not-empty/omniq

------------------------------------------------------------------------

## Install

``` bash
pip install omniq
```

------------------------------------------------------------------------

## Quick start

### Publish

``` python
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

``` python
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

------------------------------------------------------------------------

## Client initialization

``` python
from omniq.client import OmniqClient

# Option A: host/port
uq = OmniqClient(host="localhost", port=6379, db=0)

# Option B: Redis URL
uq = OmniqClient(redis_url="redis://:password@localhost:6379/0")
```

------------------------------------------------------------------------

# Administrative Operations

These operations are **strict and atomic (Lua-backed)**.

## retry_failed()

Retry a single failed job (resets `attempt=0` and moves back to
waiting).

``` python
uq.retry_failed(queue="demo", job_id="01ABC...")
```

-   Only works if job `state == "failed"`.
-   Safe under grouping rules.

------------------------------------------------------------------------

## retry_failed_batch()

Retry up to **100 failed jobs** atomically.

``` python
results = uq.retry_failed_batch(
    queue="demo",
    job_ids=["01A...", "01B...", "01C..."]
)

for job_id, status, reason in results:
    print(job_id, status, reason)
```

-   Max 100 jobs per call.
-   Returns per-job result.
-   Jobs not in `failed` state return `ERR NOT_FAILED`.

------------------------------------------------------------------------

## remove_job()

Remove a single non-active job from a specific lane.

``` python
uq.remove_job(
    queue="demo",
    job_id="01ABC...",
    lane="failed",   # wait | delayed | failed | completed | gwait
)
```

Rules:

-   Cannot remove `active` jobs.
-   Lane must match job state.
-   Group safety is preserved.

------------------------------------------------------------------------

## remove_jobs_batch()

Remove up to **100 jobs** from a specific lane.

``` python
results = uq.remove_jobs_batch(
    queue="demo",
    lane="failed",
    job_ids=["01A...", "01B...", "01C..."]
)

for job_id, status, reason in results:
    print(job_id, status, reason)
```

-   Strict lane validation.
-   Atomic per batch.
-   Safe for grouped jobs.

------------------------------------------------------------------------

# Handler Context

Inside `handler(ctx)` you receive:

-   `queue`
-   `job_id`
-   `payload_raw`
-   `payload`
-   `attempt`
-   `lock_until_ms`
-   `lease_token`
-   `gid`
-   `check_completion`

------------------------------------------------------------------------

# check_completion (Parent / Child workflows)

A **handler-driven primitive** for parallel fan-out workflows.

No TTL. Cleanup occurs only when the counter reaches zero.

## Parent Example

``` python
def parent_handler(ctx):
    document_id = ctx.payload["document_id"]
    pages = ctx.payload["pages"]

    key = f"document:{document_id}"

    ctx.check_completion.InitJobCounter(key, pages)

    for p in range(1, pages + 1):
        uq.publish(
            queue="pages",
            payload={
                "document_id": document_id,
                "page": p,
                "completion_key": key,
            },
        )
```

## Child Example

``` python
def page_handler(ctx):
    key = ctx.payload["completion_key"]

    # do work...
    remaining = ctx.check_completion.JobDecrement(key)

    if remaining == 0:
        print("Last page finished.")
```

Properties:

-   Idempotent decrement (safe under retries).
-   No accidental completion.
-   Cross-queue safe.
-   Fully user-controlled business logic.

------------------------------------------------------------------------

## Grouped jobs (FIFO + concurrency)

``` python
uq.publish(queue="demo", payload={"i": 1}, gid="company:acme", group_limit=2)
uq.publish(queue="demo", payload={"i": 2}, gid="company:acme")
```

-   FIFO inside group
-   Groups run in parallel
-   Concurrency limited per group

------------------------------------------------------------------------

## License

See the repository license.
