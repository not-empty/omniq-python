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
