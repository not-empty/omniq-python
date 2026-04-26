# OmniQ (Python)

Python client for **OmniQ**, a Redis-based distributed job queue designed 
for deterministic **consumer-driven job execution and coordination**.

OmniQ provides primitives for **job reservation, execution, and coordination**
**directly inside Redis**, allowing multiple **consumers** to safely process 
jobs in a distributed system.

Unlike traditional queues that treat jobs as transient messages, OmniQ 
maintains **explicit execution and state structures**, enabling predictable 
control over concurrency, ordering, and failure recovery.

The system is **language-agnostic**, allowing producers and consumers 
implemented in different runtimes to share the same execution model.

Core project:

[https://github.com/not-empty/omniq](https://github.com/not-empty/omniq)

------------------------------------------------------------------------

## Installation

```bash
pip install omniq
```

------------------------------------------------------------------------

## Features

- **Redis-native execution model -**
  - Job reservation, execution, and coordination happen atomically inside Redis
- **Consumer- driven processing -**
  - Workers control execution lifecycle instead of passive message delivery
- **Deterministic job state -**
  -  Explicit lanes for `wait`, `delayed`, `active`, `failed`, and `completed`
-  **Grouped jobs with concurrency limits -**
   -  FIFO within groups with parallel execution across groups
- **Atomic administrative operations -**
  - Retry, removal, pause, and batch operations backed by Lua Scripts
- **Parent/Child workflow primitive -**
  - Fan-out processing with idempotent completion tracking
- **Structured payload support -**
  - Publish typed dataclasses as JSON
- **Language-agnostic architecture -**
  - Producers and consumers can run in different runtimes.    


------------------------------------------------------------------------

## Quick Start

### Publish

```python
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

### Publish Structured JSON

```python
from dataclasses import dataclass
from typing import List, Optional

# importing the lib
from omniq.client import OmniqClient


# Nested structure
@dataclass
class Customer:
    id: str
    email: str
    vip: bool


# Main payload
@dataclass
class OrderCreated:
    order_id: str
    customer: Customer
    amount: int
    currency: str
    items: List[str]
    processed: bool
    retry_count: int
    tags: Optional[List[str]] = None


# creating OmniQ passing redis information
omniq = OmniqClient(
    host="omniq-redis",
    port=6379,
)

# creating structured payload
payload = OrderCreated(
    order_id="ORD-2026-0001",
    customer=Customer(
        id="CUST-99",
        email="leo@example.com",
        vip=True,
    ),
    amount=1500,
    currency="USD",
    items=["keyboard", "mouse"],
    processed=False,
    retry_count=0,
    tags=["priority", "online"],
)

# publish using publish_json
job_id = omniq.publish_json(
    queue="deno",
    payload=payload,
    max_attempts=5,
    timeout_ms=60_000,
)

print("OK", job_id)
```
------------------------------------------------------------------------

### Consume

```python
import time

# importing the lib
from omniq.client import OmniqClient
from omniq.types import JobCtx

# creating your handler (ctx will have all the job information and actions)
def my_actions(ctx: JobCtx):
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

Inside `handler(ctx: JobCtx)`:
- `queue`
- `job_id`
- `payload_raw`
- `payload`
- `attempt`
- `max_attempts`
- `lock_until_ms`
- `lease_token`
- `gid`
- `exec` - execution layer (`ctx.exec`)

Example:

```python
from omniq.types import JobCtx

def my_actions(ctx: JobCtx):
    is_last_attempt = ctx.attempt >= ctx.max_attempts
    print("Last attempt?", is_last_attempt)
```

------------------------------------------------------------------------

## Admistrative OPerations

All admin operations are **Lua-backend and atomic**

### Retry_failed()

```bash
omniq.retry_failed(queue="demo", job_id="01ABC...")
```
- Works only if job state is `failed`
- Resets attempt counter
- Respects grouping rule

------------------------------------------------------------------------

### Retry_failed_batch()

```bash
results = omniq.retry_failed_batch(
    queue="demo",
    job_ids=["01A...", "01B...", "01C..."]
)

for job_id, status, reason in results:
    print(job_id, status, reason)
```
- Max 100 jobs per call
- Atomic batch
- Per-job result returned

------------------------------------------------------------------------

### Remove_job()

```bash
omniq.remove_job(
    queue="demo",
    job_id="01ABC...",
    lane="failed",  # wait | delayed | failed | completed | gwait
)
```
**Rules:**
- Cannot remove active jobs
- Lane must match job state
- Group safety preserved

------------------------------------------------------------------------

### Remove_job_batch()

```bash
results = omniq.remove_jobs_batch(
    queue="demo",
    lane="failed",
    job_ids=["01A...", "01B...", "01C..."]
)
```
**Rules:**
- Max 100 per call
- Strict lane validation
- Atomic per batch

------------------------------------------------------------------------

### Pause()

```bash
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
**Rules:**
- Max 100 per call
- Strict lane validation
- Atomic per batch

------------------------------------------------------------------------

## Child ACK Control (Parent/Child Workflows)

This primitive enables **fan-out workflows**, where a parent job spawns
multiple child jobs that can run in parallel across one or more queues.

If you want to learn more about the internal execution model and architecture,
see the core project: **[OmniQ](https://github.com/not-empty/omniq)**.

Each child job acknowledges its completion using a **shared completion key**.
OmniQ maintains an **atomic counter in Redis** that tracks how many child jobs
are still pending.

When a child finishes, it calls `child_ack()`, which decrements the counter
and returns the number of remaining jobs. When the counter reaches `0`,
it indicates that **all child jobs have completed**.

The mechanism is **idempotent and safe under retries**, ensuring that
duplicate executions do not corrupt the completion tracking.

No TTL is used, the counter is automatically cleaned up when the value
reaches zero.

------------------------------------------------------------------------

### Parent Example

The first queue will receive a document with 5 pages

```python
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

The first consumer will publish a job for each page passing the unique key
for childs tracking.

```python
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

------------------------------------------------------------------------

### Child Example

The second consumer will deal with each page and ack each (alerting whe the last
page was processed).

```python
import time

# importing the lib
from omniq.client import OmniqClient
from omniq.types import JobCtx

# creating your handler (ctx will have all the job information and actions)
def page_worker(ctx: JobCtx):

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

**Propeties:**
- Idempotent decrement
- Safe under retries
- Cross-queue safe
- Fully business-logic driven

------------------------------------------------------------------------

## Grouped Jobs

```python
# if you provide a gid (group_id) you can limit the parallel execution for jobs in the same group
omniq.publish(queue="demo", payload={"i": 1}, gid="company:acme", group_limit=1)

# you can also publis ungrouped jobs that will also be executed (fairness by round-robin algorithm)
omniq.publish(queue="demo", payload={"i": 2})
```
- FIFO inside group
- Groups execute in parallel
- Concurrency limited per group

------------------------------------------------------------------------

## Pause and Resume inside the consumer

You publish your as usual

```python
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

```python
import time

# importing the lib
from omniq.client import OmniqClient
from omniq.types import JobCtx

# creating your handler (ctx will have all the job information and actions)
def pause_unpause_example(ctx: JobCtx):
    print("Waiting 2 seconds")

    # checking if this queue it is paused (spoiler: it's not)
    is_paused = ctx.exec.is_paused(
        queue="test"
    )
    print("Is paused", is_paused)
    time.sleep(2)


    print("Pausing")

    # pausing this queue (this job it's and others active jobs will be not affected but not new job will be start until queue is resumed)
    ctx.exec.pause(
        queue="test"
    )

    # checking again now is suposed to be paused
    is_paused = ctx.exec.is_paused(
        queue="test"
    )
    print("Is paused", is_paused)
    time.sleep(2)

    print("Resuming")

    # resuming this queue (all other workers can process jobs again)
    ctx.exec.resume(
        queue="test"
    )

    # checking again and is suposed to be resumed
    is_paused = ctx.exec.is_paused(
        queue="test"
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

------------------------------------------------------------------------

## Examples

Additional usage examples demonstrating common patterns can be found
in the `/examples` folder.

------------------------------------------------------------------------

## License

See the repository license.
