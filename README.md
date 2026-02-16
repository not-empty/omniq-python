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
-   **Handler-driven execution layer**
    -   `ctx.exec` exposes internal OmniQ operations safely inside handlers

------------------------------------------------------------------------

## Install

``` bash
pip install omniq
```

------------------------------------------------------------------------

## Quick Start

### Publish

``` python
# importing the lib
from omniq.client import OmniqClient

# creating OmniQ passing redis information
omniq = OmniqClient(
    host="omniq-redis",
    port=6379,
)

# publishing the job
job_id = omniq.publish(
    queue="demo",
    payload={"hello": "world"},
    timeout_ms=30_000
)

print("OK", job_id)

```

------------------------------------------------------------------------

### Consume

``` python
import time

# importing the lib
from omniq.client import OmniqClient

# creating your handler (ctx will have all the job information and actions)
def my_actions(ctx):
    print("Waiting 2 seconds")
    time.sleep(2)
    print("Done")

# creating OmniQ passing redis information
omniq = OmniqClient(
    host="omniq-redis",
    port=6379,
)

# creating the consumer that will listen and execute the actions in your handler
omniq.consume(
    queue="demo",
    handler=my_actions,
    verbose=True,
    drain=False,
)

```

------------------------------------------------------------------------

## Handler Context

Inside `handler(ctx)`:

-   `queue`
-   `job_id`
-   `payload_raw`
-   `payload`
-   `attempt`
-   `lock_until_ms`
-   `lease_token`
-   `gid`
-   `exec` → execution layer (`ctx.exec`)

------------------------------------------------------------------------

# Administrative Operations

All admin operations are **Lua-backed and atomic**.

## retry_failed()

``` python
omniq.retry_failed(queue="demo", job_id="01ABC...")
```

-   Works only if job state is `failed`
-   Resets attempt counter
-   Respects grouping rules

------------------------------------------------------------------------

## retry_failed_batch()

``` python
results = omniq.retry_failed_batch(
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
omniq.remove_job(
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
results = omniq.remove_jobs_batch(
    queue="demo",
    lane="failed",
    job_ids=["01A...", "01B...", "01C..."]
)
```

-   Max 100 per call
-   Strict lane validation
-   Atomic per batch

------------------------------------------------------------------------

## pause()

``` python
pause_result = omniq.pause(
    queue="demo",
)

resume_result = omniq.resume(
    queue="demo",
)

is_paused = omniq.is_paused(
    queue="demo",
)
```
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
-   `exec`

------------------------------------------------------------------------

# Child Ack Control (Parent / Child Workflows)

A handler-driven primitive for fan-out workflows.

No TTL. Cleanup happens only when counter reaches zero.

## Parent Example

The first queue will receive a document with 5 pages
``` python
# importing the lib
from omniq.client import OmniqClient

# creating OmniQ passing redis information
omniq = OmniqClient(
    host="omniq-redis",
    port=6379,
)

# publishing the job
job_id = omniq.publish(
    queue="documents",
    payload={
        "document_id": "doc-123", # this will be our unique key to initiate childs and tracking then until completion
        "pages": 5, # each page must be completed before something happen
    },
)
print("OK", job_id)
```

The first consumer will publish a job for each page passing the unique key for childs tracking
``` python
# importing the lib
from omniq.client import OmniqClient

# creating OmniQ passing redis information
omniq = OmniqClient(
    host="omniq-redis",
    port=6379,
)

# publishing the job
job_id = omniq.publish(
    queue="documents",
    payload={
        "document_id": "doc-123", # this will be our unique key to initiate childs and tracking then until completion
        "pages": 5, # each page must be completed before something happen
    },
)
print("OK", job_id)
```

## Child Example

The second consumer will deal with each page and ack each child (alerting when the last page was processed)

``` python
import time

# importing the lib
from omniq.client import OmniqClient

# creating your handler (ctx will have all the job information and actions)
def page_worker(ctx):

    page = ctx.payload["page"]
    # getting the unique key to track the childs
    completion_key = ctx.payload["completion_key"]

    print(f"[page_worker] Processing page {page} (job_id={ctx.job_id})")
    time.sleep(1.5)

    # acking itself as a child the number of remaining jobs are returned so we can say when the last job was executed
    remaining = ctx.exec.child_ack(completion_key)

    print(f"[page_worker] Page {page} done. Remaining={remaining}")
    

    # remaining will be 0 ONLY when this is the last job
    # will return > 0 when are still jobs to process
    # and -1 if something goes wrong with the counter
    if remaining == 0:
        print("[page_worker] Last page finished.")

# creating OmniQ passing redis information
omniq = OmniqClient(
    host="omniq-redis",
    port=6379,
)

# creating the consumer that will listen and execute the actions in your handler
omniq.consume(
    queue="pages",
    handler=page_worker,
    verbose=True,
    drain=False,
)
```

Properties:

-   Idempotent decrement
-   Safe under retries
-   Cross-queue safe
-   Fully business-logic driven

------------------------------------------------------------------------

## Grouped Jobs

``` python
# if you provide a gid (group_id) you can limit the parallel execution for jobs in the same group
omniq.publish(queue="demo", payload={"i": 1}, gid="company:acme", group_limit=1)

# you can also publis ungrouped jobs that will also be executed (fairness by round-robin algorithm)
omniq.publish(queue="demo", payload={"i": 2})
```

-   FIFO inside group
-   Groups execute in parallel
-   Concurrency limited per group

------------------------------------------------------------------------

## Pause and Resume inside the consumer

You publish your job as usual
``` python
# importing the lib
from omniq.client import OmniqClient

# creating OmniQ passing redis information
uq = OmniqClient(
    host="omniq-redis",
    port=6379,
)

# publishing the job
job_id = uq.publish(
    queue="test",
    payload={"hello": "world"},
    timeout_ms=30_000
)
print("OK", job_id)
```

Inside your consumer you can pause/resume your queue (or another one)
``` python
import time

# importing the lib
from omniq.client import OmniqClient

# creating your handler (ctx will have all the job information and actions)
def pause_unpause_example(ctx):
    print("Waiting 2 seconds")

    # checking if this queue it is paused (spoiler: it's not)
    is_paused = ctx.exec.is_paused(
        "test"
    )
    print("Is paused", is_paused)
    time.sleep(2)


    print("Pausing")

    # pausing this queue (this job it's and others active jobs will be not affected but not new job will be start until queue is resumed)
    ctx.exec.pause(
        "test"
    )

    # checking again now is suposed to be paused
    is_paused = ctx.exec.is_paused(
        "test"
    )
    print("Is paused", is_paused)
    time.sleep(2)

    print("Resuming")

    # resuming this queue (all other workers can process jobs again)
    ctx.exec.resume(
        "test"
    )

    # checking again and is suposed to be resumed
    is_paused = ctx.exec.is_paused(
        "test"
    )
    print("Is paused", is_paused)
    time.sleep(2)

    print("Done")

# creating OmniQ passing redis information
omniq = OmniqClient(
    host="omniq-redis",
    port=6379,
)

# creating the consumer that will listen and execute the actions in your handler
omniq.consume(
    queue="test",
    handler=pause_unpause_example,
    verbose=True,
    drain=False,
)
```

## Examples

All examples can be found in the `./examples` folder.

------------------------------------------------------------------------

## License

See the repository license.
