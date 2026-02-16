import time

# importing the lib
from omniq.client import OmniqClient

# creating your handler (ctx will have all the job information and actions)
def pause_unpause_example(ctx):
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
