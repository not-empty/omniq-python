import time

# importing the lib
from omniq.client import OmniqClient
from omniq.types import JobCtx


# creating your handler (ctx will have all the job information and actions)
def fail_until_last_attempt(ctx: JobCtx):
    is_last_attempt = ctx.attempt >= ctx.max_attempts

    print(
        f"[max_attempts] job_id={ctx.job_id} "
        f"attempt={ctx.attempt}/{ctx.max_attempts} "
        f"last_attempt={is_last_attempt}"
    )

    if not is_last_attempt:
        print("[max_attempts] Failing on purpose to force a retry.")
        raise RuntimeError("Intentional failure before the last attempt")

    print("[max_attempts] Last attempt reached. Finishing successfully.")
    time.sleep(1)


# creating OmniQ passing redis information
omniq = OmniqClient(
    host="omniq-redis",
    port=6379,
)

# creating the consumer that will listen and execute the actions in your handler
omniq.consume(
    queue="max-attempts",
    handler=fail_until_last_attempt,
    verbose=True,
    drain=False,
)
