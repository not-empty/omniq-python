import argparse
import csv
import os
import signal
import subprocess
import sys
import time
from typing import Optional

import redis


def parse_connected_clients(info_clients: str) -> Optional[int]:
    # INFO clients output includes a line like: connected_clients:12
    for line in info_clients.splitlines():
        if line.startswith("connected_clients:"):
            try:
                return int(line.split(":", 1)[1].strip())
            except Exception:
                return None
    return None


def count_clients_by_name(r: redis.Redis, name_prefix: str) -> int:
    try:
        raw = r.execute_command("CLIENT", "LIST")
    except Exception:
        return -1

    # redis-py decode_responses=True -> str
    s = raw if isinstance(raw, str) else raw.decode("utf-8", "replace")

    # CLIENT LIST entries have: name=...
    # We count any line containing name=<prefix>
    needle = f"name={name_prefix}"
    return sum(1 for line in s.splitlines() if needle in line)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis-host", default=os.getenv("REDIS_HOST", "127.0.0.1"))
    ap.add_argument("--redis-port", type=int, default=int(os.getenv("REDIS_PORT", "6379")))
    ap.add_argument("--redis-user", default=os.getenv("REDIS_USERNAME", None))
    ap.add_argument("--redis-pass", default=os.getenv("REDIS_PASSWORD", None))
    ap.add_argument("--redis-db", type=int, default=int(os.getenv("REDIS_DB", "0")))
    ap.add_argument("--interval-ms", type=int, default=200)
    ap.add_argument("--out", default="connections.csv")

    # Optional: count only matching client names (best for OmniQ-only signal)
    ap.add_argument("--name-prefix", default="omniq-chaos")

    # Run chaos command as a subprocess; everything after `--` is the command
    ap.add_argument("cmd", nargs=argparse.REMAINDER)

    args = ap.parse_args()
    if args.cmd and args.cmd[0] == "--":
        args.cmd = args.cmd[1:]

    r = redis.Redis(
        host=args.redis_host,
        port=args.redis_port,
        db=args.redis_db,
        username=args.redis_user,
        password=args.redis_pass,
        decode_responses=True,
    )

    # Open CSV
    interval_s = max(0.01, args.interval_ms / 1000.0)
    start = time.time()

    proc = None
    stop = False

    def _stop(_sig, _frame):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["t_s", "connected_clients", "omniq_named_clients", "chaos_alive"])

        if args.cmd:
            proc = subprocess.Popen(args.cmd)
            print(f"[logger] started chaos pid={proc.pid} cmd={' '.join(args.cmd)}")
        else:
            print("[logger] no chaos command provided; logging only (Ctrl+C to stop)")

        try:
            while not stop:
                # INFO clients
                try:
                    info = r.info(section="clients")  # dict
                    cc = int(info.get("connected_clients", -1))
                except Exception:
                    cc = -1

                # CLIENT LIST name-prefix count
                named = count_clients_by_name(r, args.name_prefix) if args.name_prefix else -1

                chaos_alive = 0
                if proc is not None:
                    chaos_alive = 1 if (proc.poll() is None) else 0

                t_s = time.time() - start
                w.writerow([f"{t_s:.3f}", cc, named, chaos_alive])
                f.flush()

                if proc is not None and proc.poll() is not None:
                    # chaos finished; keep a short cooldown window for settling
                    # (so you can see it return to baseline)
                    cooldown_s = 10.0
                    end_at = time.time() + cooldown_s
                    while time.time() < end_at and not stop:
                        try:
                            info = r.info(section="clients")
                            cc = int(info.get("connected_clients", -1))
                        except Exception:
                            cc = -1
                        named = count_clients_by_name(r, args.name_prefix) if args.name_prefix else -1
                        t_s = time.time() - start
                        w.writerow([f"{t_s:.3f}", cc, named, 0])
                        f.flush()
                        time.sleep(interval_s)
                    break

                time.sleep(interval_s)

        finally:
            if proc is not None and proc.poll() is None:
                try:
                    proc.terminate()
                    proc.wait(timeout=3)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass

    print(f"[logger] wrote {args.out}")


if __name__ == "__main__":
    main()
