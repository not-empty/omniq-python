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
