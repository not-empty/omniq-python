# OmniQ (Python)

OmniQ Python is the **Python client** for **OmniQ**, a Redis + Lua-based task
queue designed for distributed processing with strict control over execution
concurrency, and state. The library provides a direct interface for publishing,
consuming, and managing jobs while maintaining operational safety guarantees and
consistency, even in concurrent and distributed environments.

Core project / docs: https://github.com/not-empty/omniq

------------------------------------------------------------------------

## Why OmniQ

Redis-based queues typically sacrifice predictability under concurrency.

**OmniQ was designed to provide:**
-   Atomic state transitions (with Lua)
-   Lease-based execution (no double processing)
-   Group isolation with FIFO ordering
-   Deterministic control over pause, retry, and concurrency

Focused on operational safety in distributed environments.

------------------------------------------------------------------------

## Execution
The development environment can be started in two different ways, depending on your setup and infrastructure preferences:

-   With Docker — recommended for isolation, reproducibility, and consistent Redis configuration.

-   Without Docker — using a locally installed and properly configured Redis instance.

Both approaches allow you to run the examples and validate queue behavior in a local environment.

------------------------------------------------------------------------

## Installation

### Option 1 - Using Docker

**1. Create Project**

``` bash
mkdir omniq-sample
cd omniq-sample
```
**2. Create docker-compose.yml**

``` python
services:
  omniq-redis:
    image: redis:7.4-alpine
    container_name: omniq-redis
    ports:
      - "6379:6379"
    command: ["redis-server", "--appendonly", "yes"]
    volumes:
      - redis_data:/data

  omniq-valkey:
    image: valkey/valkey:9-alpine
    container_name: omniq-valkey
    ports:
      - "6380:6379"
    command: ["valkey-server", "--appendonly", "yes"]
    volumes:
      - valkey_data:/data

  omniq-python:
    image: python:3.12-slim
    container_name: omniq-python
    working_dir: /app
    environment:
      PYTHONUNBUFFERED: "1"
    volumes:
      - ./:/app
    depends_on:
      - omniq-redis
    command: ["bash", "-lc", "tail -f /dev/null"]
```
choose either Redis or Valkey to run this project.

**3. Start environment**

``` bash
docker compose up -d
```
**4. Access Python container**

``` bash
docker exec -it omniq-python bash
pip install omniq
```

**5. Create Simple example**

``` bash
mkdir simple
cd simple
```

**publish.py**
``` python
from omniq.client import OmniqClient

omniq = OmniqClient(host="omniq-redis", port=6379)

job_id = omniq.publish(
    queue="demo",
    payload={"hello": "world"},
    timeout_ms=30_000
)

print("Published:", job_id)
```

**consumer.py**
``` python
import time
from omniq.client import OmniqClient

def handler(ctx):
    print("Processing:", ctx.job_id)
    time.sleep(2)
    print("Done")

omniq = OmniqClient(host="omniq-redis", port=6379)

omniq.consume(
    queue="demo",
    handler=handler,
    verbose=True
)
```
**Run:**
``` bash
python publish.py
python consumer.py
```

------------------------------------------------------------------------

### Option 2 - Running Locally Without Docker

**1. Start Redis**
``` bash
redis-server
```

**2. Create Project**
``` bash
mkdir omniq-sample
cd omniq-sample
pip install omniq
```
Then create publish.py and consumer.py as shown above

------------------------------------------------------------------------

## Publishing

### Publish Simple Job

``` python
from omniq.client import OmniqClient

omniq = OmniqClient(host="omniq-redis", port=6379)

job_id = omniq.publish(
    queue="demo",
    payload={"hello": "world"},
    timeout_ms=30_000
)
```
The publish method enqueues a job with a JSON-serializable payload.

**Key behavior:**
-   The job receives a unique job_id
-   timeout_ms defines the lease duration once reserved
-   The payload is stored atomically
This is the simplest way to enqueue work for asynchronous processing.

------------------------------------------------------------------------

### Publish Structured Payload

``` python
from dataclasses import dataclass
from typing import Optional, List
from omniq.client import OmniqClient

@dataclass
class OrderCreated:
    order_id: str
    amount: int
    currency: str
    tags: Optional[List[str]] = None

omniq = OmniqClient(host="omniq-redis", port=6379)

job_id = omniq.publish_json(
    queue="orders",
    payload=OrderCreated(
        order_id="ORD-1",
        amount=1000,
        currency="USD",
        tags=["priority"]
    ),
    max_attempts=5,
    timeout_ms=60_000
)
```
The publish_json method allows you to send structured objects (such as dataclasses) that are automatically serialized to JSON before being stored in the queue.

**Advantages:**
-   Safe and standardized serialization
-   Typed and predictable payload structure
-   Explicit max_attempts control
-   Better organization for event-driven systems
-   Recommended for production environments where payload contracts must remain stable.

------------------------------------------------------------------------

## Consumption

``` python
omniq.consume(
    queue="demo",
    handler=handler,
    verbose=True,
    drain=False
)
```
The consume method starts a worker loop that continuously reserves and processes jobs from the specified queue.

**Parameters:**
-   queue — Target queue name
-   handler — Function responsible for processing each job
-   verbose=True — Enables execution logs (reservations, ACKs, retries, errors)
-   drain=False — Keeps the consumer running, waiting for new jobs
  
With drain=False, the worker behaves as a long-running consumer suitable for services and background processors.

------------------------------------------------------------------------

## Drain Mode

``` python
omniq.consume(
    queue="demo",
    handler=handler,
    drain=True
)
```
**When drain=True, the consumer:**
-   Processes all currently available jobs
-   Does not wait for new jobs
-   Automatically exits once the queue is empty
  
**Best suited for:**
-   Batch processing
-   Maintenance scripts
-   Controlled execution pipelines

------------------------------------------------------------------------

## Heartbeat

For long-running jobs:
``` python
ctx.exec.heartbeat()
```
The heartbeat call renews the lease of the currently running job.

**Behavior**
-   Uses the same lease_token issued at reservation
-   Extends lock_until_ms safely
-   Prevents the job from returning to the queue while still being processed
-   Essential for long or unpredictable execution times.
  
------------------------------------------------------------------------

## Handler Context

### Inside handler(ctx):

| Field          | Description                                |
|----------------|--------------------------------------------|
| queue          | Queue name                                 |
| job_id         | Unique identifier                          |
| payload        | Deserialized payload                       |
| payload_raw    | Raw JSON                                   |
| attempt        | Current attempt number                     |
| lock_until_ms  | Lease expiration timestamp                 |
| lease_token    | Required token for ACK/heartbeat           |
| gid            | Group identifier                           |
| exec           | Secure layer for administrative operations |

------------------------------------------------------------------------

## Grouped Queues

``` python
omniq.publish(
    queue="payments",
    payload={"invoice": 1},
    gid="company:acme",
    group_limit=1
)
```
By defining a gid, the job becomes part of a logical group.

**Guarantees:**
-   FIFO ordering within the same group
-   Parallel execution across different groups
-   Round-robin scheduling between active groups
-   Concurrency control via group_limit

This allows isolation of tenants, customers, or entities without blocking the entire system.

------------------------------------------------------------------------

## Retry

### Retry Failed Job

``` python
omniq.retry_failed(
    queue="demo",
    job_id="01ABC..."
)
```
Reactivates a job currently in the failed lane.

**Rules:**
-   Only works if the job is in failed state
-   Resets the attempt counter
-   Preserves group and concurrency rules

------------------------------------------------------------------------

### Batch Retry (up to 100)

``` python
results = omniq.retry_failed_batch(
    queue="demo",
    job_ids=["01A...", "01B..."]
)
```
Allows retrying multiple failed jobs at once.

**Characteristics:**
-   Atomic operation (Lua-backed)
-   Up to 100 jobs per call
-   Individual result per job (status + reason)

------------------------------------------------------------------------

## Removal

``` python
omniq.remove_jobs_batch(
    queue="demo",
    lane="failed",
    job_ids=["01A...", "01B..."]
)
```
Removes jobs from a specific lane.

**Important rules:**
-   Active jobs cannot be removed
-   The provided lane must match the actual job state
-   Group integrity is preserved
-   Used for administrative cleanup and manual state control.

------------------------------------------------------------------------

## Pause and Resume

``` python
omniq.pause(queue="demo")
omniq.resume(queue="demo")
omniq.is_paused(queue="demo")
```
Controls queue execution flow.

**Guarantees:**
- Prevents new reservations while paused
- Active jobs continue running normally
- No job is lost or moved unexpectedly
- Immediate and consistent resume behavior

Can be used externally or inside a handler via ctx.exec.

------------------------------------------------------------------------

## Parent/Child Flow

``` python
remaining = ctx.exec.child_ack(completion_key)

if remaining == 0:
    print("Last child completed")
```
**Returns:**
- Pending children -> `> 0`
- Last Child -> `0`
- Counter error -> `1`

**Properties:**
- Idempotent
- Retry-safe
- Isolated across queues

------------------------------------------------------------------------

## Administrative Operations
All administrative operations are `executed atomically via Lua`, ensuring 
consistency even under high concurrency.

------------------------------------------------------------------------

## Examples

For a better understanding of queue behavior and its main features, see
the examples available in the `./examples` folder.

**You will find scenarios such as:**
- Basic job publishing and consumption
- Using the `ctx.exec` layer inside handlers
- Parent/child workflows with `childs_init` and `child_ack`
- Coordination between multiple queues (documents -> pages)
- Execution control using pause and resume inside handlers

The examples are recommended to understand the full execution flow in
real-world environments.

------------------------------------------------------------------------

## License
GPL-3.0
Refer to the main repository for details
