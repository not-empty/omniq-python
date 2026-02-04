from src.omniq.client import OmniqClient

uq = OmniqClient(
    host="omniq-redis",
    port=6379,
)
job_id = uq.publish(
    queue="demo",
    payload={"hello": "world"},
    timeout_ms=30_000
)
print("OK", job_id)
