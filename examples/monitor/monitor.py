#!/usr/bin/env python3
"""
OmniQ observer

Polls OmniQ monitoring keys and optional transactional keys to help validate
behavior under stress.

Examples:
  python omniq_observer.py --redis-url redis://omniq-redis:6379/0
  python omniq_observer.py --redis-url redis://omniq-redis:6379/0 --interval 0.5 --csv omniq_observer.csv
  python omniq_observer.py --redis-url redis://omniq-redis:6379/0 --queues emails,pdfs --raw-verify
"""

from __future__ import annotations

import argparse
import csv
import os
import signal
import sys
import time
from dataclasses import dataclass
from typing import Iterable

import redis


@dataclass
class QueueSnapshot:
    ts_ms: int
    queue: str
    paused: int
    waiting: int
    group_waiting: int
    waiting_total: int
    active: int
    delayed: int
    failed: int
    completed_kept: int
    groups_ready: int
    last_activity_ms: int
    last_enqueue_ms: int
    last_reserve_ms: int
    last_finish_ms: int
    raw_waiting: int | None = None
    raw_group_waiting: int | None = None
    raw_waiting_total: int | None = None
    raw_active: int | None = None
    raw_delayed: int | None = None
    raw_failed: int | None = None
    raw_completed_kept: int | None = None
    raw_groups_ready: int | None = None
    ok_waiting: int | None = None
    ok_group_waiting: int | None = None
    ok_waiting_total: int | None = None
    ok_active: int | None = None
    ok_delayed: int | None = None
    ok_failed: int | None = None
    ok_completed_kept: int | None = None
    ok_groups_ready: int | None = None


class Observer:
    def __init__(
        self,
        redis_client: redis.Redis,
        queues: list[str] | None,
        raw_verify: bool,
        csv_path: str | None,
        console_every: int,
    ) -> None:
        self.r = redis_client
        self.explicit_queues = queues
        self.raw_verify = raw_verify
        self.csv_path = csv_path
        self.console_every = max(1, console_every)
        self._stop = False
        self._loop_n = 0
        self._csv_file = None
        self._csv_writer = None

    def stop(self, *_args) -> None:
        self._stop = True

    def install_signal_handlers(self) -> None:
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)

    @staticmethod
    def _to_i(value: object) -> int:
        if value is None:
            return 0
        if isinstance(value, bytes):
            value = value.decode("utf-8", errors="replace")
        if value == "":
            return 0
        try:
            return int(float(value))
        except Exception:
            return 0

    def _discover_queues(self) -> list[str]:
        if self.explicit_queues is not None:
            return self.explicit_queues
        names = sorted(
            q.decode("utf-8", errors="replace") if isinstance(q, bytes) else str(q)
            for q in self.r.smembers("omniq:queues")
        )
        return names

    def _read_stats(self, queue: str) -> dict[str, int]:
        stats = self.r.hgetall(f"{queue}:stats")
        decoded: dict[str, int] = {}
        for k, v in stats.items():
            key = k.decode("utf-8", errors="replace") if isinstance(k, bytes) else str(k)
            decoded[key] = self._to_i(v)
        return decoded

    def _scan_group_waiting(self, queue: str) -> int:
        total = 0
        cursor = 0
        pattern = f"{queue}:g:*:wait"
        while True:
            cursor, keys = self.r.scan(cursor=cursor, match=pattern, count=200)
            if keys:
                pipe = self.r.pipeline(transaction=False)
                for key in keys:
                    pipe.llen(key)
                lengths = pipe.execute()
                total += sum(self._to_i(x) for x in lengths)
            if cursor == 0:
                break
        return total

    def _read_raw(self, queue: str) -> dict[str, int]:
        raw = {
            "waiting": self._to_i(self.r.llen(f"{queue}:wait")),
            "active": self._to_i(self.r.zcard(f"{queue}:active")),
            "delayed": self._to_i(self.r.zcard(f"{queue}:delayed")),
            "failed": self._to_i(self.r.llen(f"{queue}:failed")),
            "completed_kept": self._to_i(self.r.llen(f"{queue}:completed")),
            "groups_ready": self._to_i(self.r.zcard(f"{queue}:groups:ready")),
        }
        raw["group_waiting"] = self._scan_group_waiting(queue)
        raw["waiting_total"] = raw["waiting"] + raw["group_waiting"]
        return raw

    def snapshot_queue(self, queue: str) -> QueueSnapshot:
        now_ms = int(time.time() * 1000)
        stats = self._read_stats(queue)
        paused = 1 if self.r.exists(f"{queue}:paused") else 0

        snap = QueueSnapshot(
            ts_ms=now_ms,
            queue=queue,
            paused=paused,
            waiting=stats.get("waiting", 0),
            group_waiting=stats.get("group_waiting", 0),
            waiting_total=stats.get("waiting_total", 0),
            active=stats.get("active", 0),
            delayed=stats.get("delayed", 0),
            failed=stats.get("failed", 0),
            completed_kept=stats.get("completed_kept", 0),
            groups_ready=stats.get("groups_ready", 0),
            last_activity_ms=stats.get("last_activity_ms", 0),
            last_enqueue_ms=stats.get("last_enqueue_ms", 0),
            last_reserve_ms=stats.get("last_reserve_ms", 0),
            last_finish_ms=stats.get("last_finish_ms", 0),
        )

        if self.raw_verify:
            raw = self._read_raw(queue)
            snap.raw_waiting = raw["waiting"]
            snap.raw_group_waiting = raw["group_waiting"]
            snap.raw_waiting_total = raw["waiting_total"]
            snap.raw_active = raw["active"]
            snap.raw_delayed = raw["delayed"]
            snap.raw_failed = raw["failed"]
            snap.raw_completed_kept = raw["completed_kept"]
            snap.raw_groups_ready = raw["groups_ready"]

            snap.ok_waiting = int(snap.waiting == snap.raw_waiting)
            snap.ok_group_waiting = int(snap.group_waiting == snap.raw_group_waiting)
            snap.ok_waiting_total = int(snap.waiting_total == snap.raw_waiting_total)
            snap.ok_active = int(snap.active == snap.raw_active)
            snap.ok_delayed = int(snap.delayed == snap.raw_delayed)
            snap.ok_failed = int(snap.failed == snap.raw_failed)
            snap.ok_completed_kept = int(snap.completed_kept == snap.raw_completed_kept)
            snap.ok_groups_ready = int(snap.groups_ready == snap.raw_groups_ready)

        return snap

    def _ensure_csv(self) -> None:
        if not self.csv_path or self._csv_writer is not None:
            return
        path = Path(self.csv_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        exists = path.exists() and path.stat().st_size > 0
        self._csv_file = path.open("a", newline="", encoding="utf-8")
        self._csv_writer = csv.DictWriter(self._csv_file, fieldnames=list(QueueSnapshot.__dataclass_fields__.keys()))
        if not exists:
            self._csv_writer.writeheader()
            self._csv_file.flush()

    def write_csv(self, snaps: Iterable[QueueSnapshot]) -> None:
        if not self.csv_path:
            return
        self._ensure_csv()
        assert self._csv_writer is not None
        for snap in snaps:
            self._csv_writer.writerow(snap.__dict__)
        assert self._csv_file is not None
        self._csv_file.flush()

    def print_console(self, snaps: list[QueueSnapshot]) -> None:
        if not snaps:
            print(f"[{int(time.time())}] no queues discovered")
            return

        headers = ["queue", "paused", "wait", "gwait", "wtotal", "active", "delayed", "failed", "done", "gready"]
        rows = []
        for s in snaps:
            rows.append([
                s.queue,
                str(s.paused),
                str(s.waiting),
                str(s.group_waiting),
                str(s.waiting_total),
                str(s.active),
                str(s.delayed),
                str(s.failed),
                str(s.completed_kept),
                str(s.groups_ready),
            ])

        widths = [len(h) for h in headers]
        for row in rows:
            for i, cell in enumerate(row):
                widths[i] = max(widths[i], len(cell))

        def fmt(row: list[str]) -> str:
            return "  ".join(cell.ljust(widths[i]) for i, cell in enumerate(row))

        print()
        print(fmt(headers))
        print(fmt(["-" * w for w in widths]))
        for row in rows:
            print(fmt(row))

        if self.raw_verify:
            mismatches = []
            for s in snaps:
                bad = []
                if s.ok_waiting == 0:
                    bad.append("waiting")
                if s.ok_group_waiting == 0:
                    bad.append("group_waiting")
                if s.ok_waiting_total == 0:
                    bad.append("waiting_total")
                if s.ok_active == 0:
                    bad.append("active")
                if s.ok_delayed == 0:
                    bad.append("delayed")
                if s.ok_failed == 0:
                    bad.append("failed")
                if s.ok_completed_kept == 0:
                    bad.append("completed_kept")
                if s.ok_groups_ready == 0:
                    bad.append("groups_ready")
                if bad:
                    mismatches.append(f"{s.queue}: {', '.join(bad)}")
            if mismatches:
                print("verify:", " | ".join(mismatches))
            else:
                print("verify: all stats match raw keys")

    def run(self, interval_s: float, duration_s: float | None, once: bool) -> int:
        self.install_signal_handlers()
        started = time.monotonic()

        while not self._stop:
            queues = self._discover_queues()
            snaps = [self.snapshot_queue(q) for q in queues]
            self.write_csv(snaps)

            if self._loop_n % self.console_every == 0:
                self.print_console(snaps)

            self._loop_n += 1

            if once:
                break
            if duration_s is not None and (time.monotonic() - started) >= duration_s:
                break

            time.sleep(interval_s)

        if self._csv_file is not None:
            self._csv_file.close()
        return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Observe OmniQ queue stats and optional raw Redis validation.")
    p.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://omniq-redis:6379/0"))
    p.add_argument("--queues", default="", help="Comma-separated queue names. Empty means discover from omniq:queues.")
    p.add_argument("--interval", type=float, default=1.0, help="Polling interval in seconds.")
    p.add_argument("--duration", type=float, default=None, help="Optional total run duration in seconds.")
    p.add_argument("--csv", default="", help="Optional CSV output path.")
    p.add_argument("--raw-verify", action="store_true", help="Compare monitoring stats against transactional keys.")
    p.add_argument("--console-every", type=int, default=1, help="Print every N loops.")
    p.add_argument("--once", action="store_true", help="Run a single snapshot and exit.")
    return p


def main() -> int:
    args = build_parser().parse_args()
    try:
        client = redis.Redis.from_url(args.redis_url, decode_responses=False)
        client.ping()
    except Exception as exc:
        print(f"Redis connection failed: {exc}", file=sys.stderr)
        return 2

    queues = [q.strip() for q in args.queues.split(",") if q.strip()] or None

    observer = Observer(
        redis_client=client,
        queues=queues,
        raw_verify=args.raw_verify,
        csv_path=args.csv or None,
        console_every=args.console_every,
    )
    return observer.run(
        interval_s=max(0.05, args.interval),
        duration_s=args.duration,
        once=args.once,
    )


if __name__ == "__main__":
    raise SystemExit(main())
