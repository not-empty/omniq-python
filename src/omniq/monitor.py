from dataclasses import dataclass
from typing import List, Optional

from .helper import as_str, queue_base

@dataclass(frozen=True)
class QueueCounts:
    paused: bool
    waiting: int
    active: int
    delayed: int
    completed: int
    failed: int

@dataclass(frozen=True)
class GroupStatus:
    gid: str
    inflight: int
    limit: int

@dataclass(frozen=True)
class ActiveSample:
    job_id: str
    gid: str
    lock_until_ms: int
    attempt: int

@dataclass(frozen=True)
class DelayedSample:
    job_id: str
    gid: str
    due_ms: int
    attempt: int

@dataclass(frozen=True)
class FailedSample:
    job_id: str
    gid: str
    attempt: int
    max_attempts: int
    failed_at_ms: int
    last_error: str

@dataclass(frozen=True)
class JobInfo:
    job_id: str
    state: str
    gid: str
    attempt: int
    max_attempts: int
    timeout_ms: int
    backoff_ms: int
    lease_token: str
    lock_until_ms: int
    due_ms: int
    payload: str
    last_error: str
    updated_ms: int

class QueueMonitor:
    def __init__(self, uq):
        self._uq = uq
        self._r = (
            getattr(uq, "r", None)
            or getattr(getattr(uq, "ops", None), "r", None)
            or getattr(getattr(uq, "_ops", None), "r", None)
        )
        if self._r is None:
            raise ValueError("QueueMonitor needs redis access (inject from server, do not expose to UI callers).")

    def _base(self, queue: str) -> str:
        return queue_base(queue)

    def counts(self, queue: str) -> QueueCounts:
        base = self._base(queue)
        r = self._r

        paused = r.exists(f"{base}:paused") == 1

        waiting = int(r.llen(f"{base}:wait") or 0)
        active = int(r.zcard(f"{base}:active") or 0)
        delayed = int(r.zcard(f"{base}:delayed") or 0)
        completed = int(r.llen(f"{base}:completed") or 0)
        failed = int(r.llen(f"{base}:failed") or 0)

        return QueueCounts(
            paused=paused,
            waiting=waiting,
            active=active,
            delayed=delayed,
            completed=completed,
            failed=failed,
        )

    def groups_ready(self, queue: str, limit: int = 200) -> List[str]:
        base = self._base(queue)
        r = self._r
        limit = max(1, min(int(limit), 2000))
        try:
            gids = r.zrange(f"{base}:groups:ready", 0, limit - 1)
            return [as_str(g) for g in gids if g]
        except Exception:
            return []

    def group_status(self, queue: str, gids: List[str], default_limit: int = 1) -> List[GroupStatus]:
        base = self._base(queue)
        r = self._r

        out: List[GroupStatus] = []
        for gid in gids:
            gid_s = as_str(gid)

            inflight = int(as_str(r.get(f"{base}:g:{gid_s}:inflight")) or "0")

            raw = r.get(f"{base}:g:{gid_s}:limit")
            gl = int(as_str(raw) or "0")
            limit = gl if gl > 0 else int(default_limit)

            out.append(GroupStatus(gid=gid_s, inflight=inflight, limit=limit))
        return out

    def sample_active(self, queue: str, limit: int = 50) -> List[ActiveSample]:
        base = self._base(queue)
        r = self._r
        limit = max(1, min(int(limit), 500))

        job_ids = r.zrange(f"{base}:active", 0, limit - 1)
        out: List[ActiveSample] = []

        for jid in job_ids:
            jid_s = as_str(jid)
            k_job = f"{base}:job:{jid_s}"
            gid, attempt = r.hmget(k_job, "gid", "attempt")
            score = r.zscore(f"{base}:active", jid) or 0

            out.append(
                ActiveSample(
                    job_id=jid_s,
                    gid=as_str(gid),
                    lock_until_ms=int(score),
                    attempt=int(as_str(attempt) or "0"),
                )
            )
        return out

    def sample_delayed(self, queue: str, limit: int = 50) -> List[DelayedSample]:
        base = self._base(queue)
        r = self._r
        limit = max(1, min(int(limit), 500))

        job_ids = r.zrange(f"{base}:delayed", 0, limit - 1)
        out: List[DelayedSample] = []

        for jid in job_ids:
            jid_s = as_str(jid)
            k_job = f"{base}:job:{jid_s}"
            gid, attempt = r.hmget(k_job, "gid", "attempt")
            due = r.zscore(f"{base}:delayed", jid) or 0

            out.append(
                DelayedSample(
                    job_id=jid_s,
                    gid=as_str(gid),
                    due_ms=int(due),
                    attempt=int(as_str(attempt) or "0"),
                )
            )
        return out

    def sample_failed(self, queue: str, limit: int = 50) -> List[FailedSample]:
        base = self._base(queue)
        r = self._r
        limit = max(1, min(int(limit), 500))

        try:
            job_ids = r.lrange(f"{base}:failed", 0, limit - 1)
        except Exception:
            return []

        out: List[FailedSample] = []

        for jid in job_ids:
            jid_s = as_str(jid)
            k_job = f"{base}:job:{jid_s}"

            try:
                gid, attempt, max_attempts, last_error, last_error_ms, updated_ms = r.hmget(
                    k_job,
                    "gid",
                    "attempt",
                    "max_attempts",
                    "last_error",
                    "last_error_ms",
                    "updated_ms",
                )
            except Exception:
                gid = attempt = max_attempts = last_error = last_error_ms = updated_ms = None

            fam = int(as_str(last_error_ms) or "0")
            if fam <= 0:
                fam = int(as_str(updated_ms) or "0")

            out.append(
                FailedSample(
                    job_id=jid_s,
                    gid=as_str(gid),
                    attempt=int(as_str(attempt) or "0"),
                    max_attempts=int(as_str(max_attempts) or "0"),
                    failed_at_ms=fam,
                    last_error=as_str(last_error),
                )
            )

        return out

    def get_job(self, queue: str, job_id: str) -> Optional[JobInfo]:
        base = self._base(queue)
        r = self._r
        jid_s = as_str(job_id)
        k_job = f"{base}:job:{jid_s}"

        if r.exists(k_job) != 1:
            return None

        fields = [
            "state", "gid", "attempt", "max_attempts", "timeout_ms", "backoff_ms",
            "lease_token", "lock_until_ms", "due_ms", "payload", "last_error", "updated_ms",
        ]

        try:
            vals = r.hmget(k_job, *fields)
        except Exception:
            return None

        m = {fields[i]: vals[i] for i in range(len(fields))}

        return JobInfo(
            job_id=jid_s,
            state=as_str(m["state"]),
            gid=as_str(m["gid"]),
            attempt=int(as_str(m["attempt"]) or "0"),
            max_attempts=int(as_str(m["max_attempts"]) or "0"),
            timeout_ms=int(as_str(m["timeout_ms"]) or "0"),
            backoff_ms=int(as_str(m["backoff_ms"]) or "0"),
            lease_token=as_str(m["lease_token"]),
            lock_until_ms=int(as_str(m["lock_until_ms"]) or "0"),
            due_ms=int(as_str(m["due_ms"]) or "0"),
            payload=as_str(m["payload"]),
            last_error=as_str(m["last_error"]),
            updated_ms=int(as_str(m["updated_ms"]) or "0"),
        )