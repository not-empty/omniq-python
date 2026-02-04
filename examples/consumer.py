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
