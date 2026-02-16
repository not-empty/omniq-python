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
