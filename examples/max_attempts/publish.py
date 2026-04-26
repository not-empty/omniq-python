# importing the lib
from omniq.client import OmniqClient

# creating OmniQ passing redis information
omniq = OmniqClient(
    host="omniq-redis",
    port=6379,
)

# publishing the job
job_id = omniq.publish(
    queue="max-attempts",
    payload={"hello": "world"},
    max_attempts=3,
    backoff_ms=1_000,
    timeout_ms=30_000,
)

print("OK", job_id)
