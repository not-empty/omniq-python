import time
from omniq.client import OmniqClient

def handler(ctx):
    time.sleep(2)
    print(1)
    time.sleep(2)
    print(2)
    time.sleep(2)
    print("done")

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
