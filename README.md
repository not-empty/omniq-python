# OmniQ (Python)

**OmniQ** is a Redis + Lua, language-agnostic job queue.\
This package is the **Python client** for OmniQ v1.

Core project / docs: https://github.com/not-empty/omniq

------------------------------------------------------------------------

## Key Ideas

-   **Hybrid lanes**
    -   Ungrouped jobs by default
    -   Optional grouped jobs (FIFO per group + per-group concurrency)
-   **Lease-based execution**
    -   Workers reserve a job with a time-limited lease
-   **Token-gated ACK / heartbeat**
    -   `reserve()` returns a `lease_token`
    -   `heartbeat()` and `ack_*()` must include the same token
-   **Pause / resume (flag-only)**
    -   Pausing prevents *new reserves*
    -   Running jobs are not interrupted
    -   Jobs are not moved
-   **Admin-safe operations**
    -   Strict `retry`, `retry_batch`, `remove`, `remove_batch`
-   **Handler-driven completion primitive**
    -   `check_completion` for parent/child workflows

------------------------------------------------------------------------

## Install

``` bash
pip install omniq
```

------------------------------------------------------------------------

## Quick Start

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

------------------------------------------------------------------------

### Consume

``` python
import time
from omniq.client import OmniqClient

def handler(ctx):
    print("Waiting 2 seconds")
    time.sleep(2)
    print("Done")

uq = OmniqClient(host="omniq-redis", port=6379)

uq.consume(
    queue="demo",
    handler=handler,
    verbose=True,
    drain=False,
)
```

------------------------------------------------------------------------

## Client Initialization

``` python
# Option A: host/port
uq = OmniqClient(host="localhost", port=6379, db=0)

# Option B: Redis URL
uq = OmniqClient(redis_url="redis://:password@localhost:6379/0")
```

------------------------------------------------------------------------

# Administrative Operations

All admin operations are **Lua-backed and atomic**.

## retry_failed()

``` python
uq.retry_failed(queue="demo", job_id="01ABC...")
```

-   Works only if job state is `failed`
-   Resets attempt counter
-   Respects grouping rules

------------------------------------------------------------------------

## retry_failed_batch()

``` python
results = uq.retry_failed_batch(
    queue="demo",
    job_ids=["01A...", "01B...", "01C..."]
)

for job_id, status, reason in results:
    print(job_id, status, reason)
```

-   Max 100 jobs per call
-   Atomic batch
-   Per-job result returned

------------------------------------------------------------------------

## remove_job()

``` python
uq.remove_job(
    queue="demo",
    job_id="01ABC...",
    lane="failed",  # wait | delayed | failed | completed | gwait
)
```

Rules:

-   Cannot remove active jobs
-   Lane must match job state
-   Group safety preserved

------------------------------------------------------------------------

## remove_jobs_batch()

``` python
results = uq.remove_jobs_batch(
    queue="demo",
    lane="failed",
    job_ids=["01A...", "01B...", "01C..."]
)
```

-   Max 100 per call
-   Strict lane validation
-   Atomic per batch

------------------------------------------------------------------------

# Handler Context

Inside `handler(ctx)`:

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

# check_completion (Parent / Child Workflows)

A handler-driven primitive for fan-out workflows.

No TTL. Cleanup happens only when counter reaches zero.

## Parent Example

``` python
def parent_handler(ctx):
    document_id = ctx.payload["document_id"]
    pages = ctx.payload["pages"]

    key = f"document:{document_id}"

    ctx.check_completion.init_job_counter(key, pages)

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

    remaining = ctx.check_completion.job_decrement(key)

    if remaining == 0:
        print("Last page finished.")
```

Properties:

-   Idempotent decrement
-   Safe under retries
-   Cross-queue safe
-   Fully business-logic driven

------------------------------------------------------------------------

## Grouped Jobs

``` python
uq.publish(queue="demo", payload={"i": 1}, gid="company:acme", group_limit=2)
uq.publish(queue="demo", payload={"i": 2}, gid="company:acme")
```

-   FIFO inside group
-   Groups execute in parallel
-   Concurrency limited per group

------------------------------------------------------------------------

## License

See the repository license.
