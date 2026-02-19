import os
import time
import random
import argparse
import subprocess
import sys

from omniq.client import OmniqClient


def run_publish_churn(host: str, port: int, queue: str, n: int, sleep_ms: int) -> None:
    for i in range(n):
        # create client -> publish -> close (this is the churn case)
        uq = OmniqClient(host=host, port=port, client_name=f"omniq-chaos-pub:{os.getpid()}:{i}")
        try:
            uq.publish(queue=queue, payload={"i": i, "hello": "world"})
        finally:
            uq.close()

        if sleep_ms > 0:
            time.sleep(sleep_ms / 1000.0)


def run_shared_client_publish(host: str, port: int, queue: str, n: int, sleep_ms: int) -> None:
    uq = OmniqClient(host=host, port=port, client_name=f"omniq-chaos-shared:{os.getpid()}")
    try:
        for i in range(n):
            uq.publish(queue=queue, payload={"i": i, "mode": "shared"})
            if sleep_ms > 0:
                time.sleep(sleep_ms / 1000.0)
    finally:
        uq.close()


def run_consumer_bounce(host: str, port: int, queue: str, runs: int, alive_s: float) -> None:
    """
    Starts a consumer in a subprocess and kills it.
    - SIGTERM should exit cleanly (your consumer closes client in finally)
    - SIGKILL will not run finally (expected), but connections should disappear after OS/Redis detects it
    """
    code = f"""
import time
from omniq.client import OmniqClient

def handler(ctx):
    time.sleep(0.2)

uq = OmniqClient(host={host!r}, port={port}, client_name="omniq-chaos-consumer-subproc")
uq.consume(queue={queue!r}, handler=handler, verbose=False, drain=True)
"""
    for i in range(runs):
        p = subprocess.Popen([sys.executable, "-c", code])
        time.sleep(alive_s)

        # mostly SIGTERM; sometimes SIGKILL
        if random.random() < 0.85:
            p.terminate()
        else:
            p.kill()

        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait()

        time.sleep(0.25)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default=os.getenv("REDIS_HOST", "omniq-redis"))
    ap.add_argument("--port", type=int, default=int(os.getenv("REDIS_PORT", "6379")))
    ap.add_argument("--queue", default=os.getenv("OMNIQ_QUEUE", "chaos"))
    ap.add_argument("--n", type=int, default=2000)
    ap.add_argument("--sleep-ms", type=int, default=0)
    ap.add_argument("--mode", choices=["churn", "shared", "consumer-bounce", "all"], default="all")
    args = ap.parse_args()

    print(f"[chaos] host={args.host} port={args.port} queue={args.queue} mode={args.mode}")

    if args.mode in ("churn", "all"):
        print("[chaos] publish churn: create->publish->close ...")
        run_publish_churn(args.host, args.port, args.queue, args.n, args.sleep_ms)
        print("[chaos] publish churn done")

    if args.mode in ("shared", "all"):
        print("[chaos] shared client publish ...")
        run_shared_client_publish(args.host, args.port, args.queue, args.n, args.sleep_ms)
        print("[chaos] shared publish done")

    if args.mode in ("consumer-bounce", "all"):
        print("[chaos] consumer bounce (subprocess) ...")
        run_consumer_bounce(args.host, args.port, args.queue, runs=30, alive_s=0.8)
        print("[chaos] consumer bounce done")

    print("[chaos] done")


if __name__ == "__main__":
    main()
