# importing the lib
from omniq.client import OmniqClient

# creating your handler (ctx will have all the job information and actions)
def document_worker(ctx):
    
    # getting the data from payload
    document_id = ctx.payload["document_id"]
    pages = ctx.payload["pages"]

    # creating the unique key to track the childs
    completion_key = f"document:{document_id}"

    print(f"[document_worker] Initializing completion for {pages} pages")

    # calling the childs initialization
    ctx.exec.childs_init(completion_key, pages)

    # publishing 5 jobs on the pages queue
    for page in range(1, pages + 1):
        # ctx.exec also have the publisher ready to use
        ctx.exec.publish(
            queue="pages",
            payload={
                "document_id": document_id,
                "page": page,
                "completion_key": completion_key,
            },
        )

    print("[document_worker] All page jobs published.")

# creating OmniQ passing redis information
omniq = OmniqClient(
    host="omniq-redis",
    port=6379,
)

# creating the consumer that will listen and execute the actions in your handler
omniq.consume(
    queue="documents",
    handler=document_worker,
    verbose=True,
    drain=False,
)
