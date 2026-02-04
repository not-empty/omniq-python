import json
from dataclasses import dataclass
from typing import Optional, Any

from .clock import now_ms
from .ids import new_ulid
from .types import PayloadT, ReservePaused, ReserveJob, ReserveResult, AckFailResult
from .transport import RedisLike
from .scripts import OmniqScripts

@dataclass
class OmniqOps:
    r: RedisLike
    scripts: OmniqScripts

    @staticmethod
    def queue_base(queue_name: str) -> str:
        if "{" in queue_name and "}" in queue_name:
            return queue_name
        return "{" + queue_name + "}"

    def publish(
        self,
        *,
        queue: str,
        payload: Any,
        job_id: Optional[str] = None,
        max_attempts: int = 3,
        timeout_ms: int = 30_000,
        backoff_ms: int = 5_000,
        due_ms: int = 0,
        now_ms_override: int = 0,
        gid: Optional[str] = None,
        group_limit: int = 0,
    ) -> str:
        if not isinstance(payload, (dict, list)):
            raise TypeError(
                "publish(payload=...) must be a dict or list (structured JSON). "
                "Wrap strings as {'text': '...'} or {'value': '...'}."
            )

        base = self.queue_base(queue)
        nms = now_ms_override or now_ms()

        jid = job_id or new_ulid()

        payload_s = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)

        gid_s = (gid or "").strip()
        glimit_s = str(int(group_limit)) if group_limit and group_limit > 0 else "0"

        argv = [
            base,
            jid,
            payload_s,
            str(int(max_attempts)),
            str(int(timeout_ms)),
            str(int(backoff_ms)),
            str(int(nms)),
            str(int(due_ms)),
            gid_s,
            glimit_s,
        ]

        res = self.r.evalsha(self.scripts.enqueue_sha, 0, *argv)

        if not isinstance(res, list) or len(res) < 2:
            raise RuntimeError(f"Unexpected ENQUEUE response: {res}")

        status = str(res[0])
        out_id = str(res[1])

        if status != "OK":
            raise RuntimeError(f"ENQUEUE failed: {status}")

        return out_id

    def pause(self, *, queue: str) -> str:
        base = self.queue_base(queue)
        res = self.r.evalsha(self.scripts.pause_sha, 0, base)
        return str(res)

    def resume(self, *, queue: str) -> int:
        base = self.queue_base(queue)
        res = self.r.evalsha(self.scripts.resume_sha, 0, base)
        try:
            return int(res)
        except Exception:
            return 0

    def is_paused(self, *, queue: str) -> bool:
        base = self.queue_base(queue)
        return self.r.exists(base + ":paused") == 1

    def reserve(self, *, queue: str, now_ms_override: int = 0) -> ReserveResult:
        base = self.queue_base(queue)
        nms = now_ms_override or now_ms()

        res = self.r.evalsha(self.scripts.reserve_sha, 0, base, str(int(nms)))

        if not isinstance(res, list) or len(res) < 1:
            raise RuntimeError(f"Unexpected RESERVE response: {res}")

        if res[0] == "EMPTY":
            return None

        if res[0] == "PAUSED":
            return ReservePaused()

        if res[0] != "JOB" or len(res) < 7:
            raise RuntimeError(f"Unexpected RESERVE response: {res}")

        return ReserveJob(
            status="JOB",
            job_id=str(res[1]),
            payload=str(res[2]),
            lock_until_ms=int(res[3]),
            attempt=int(res[4]),
            gid=str(res[5] or ""),
            lease_token=str(res[6] or ""),
        )

    def heartbeat(self, *, queue: str, job_id: str, lease_token: str, now_ms_override: int = 0) -> int:
        base = self.queue_base(queue)
        nms = now_ms_override or now_ms()

        res = self.r.evalsha(self.scripts.heartbeat_sha, 0, base, job_id, str(int(nms)), lease_token)

        if not isinstance(res, list) or len(res) < 1:
            raise RuntimeError(f"Unexpected HEARTBEAT response: {res}")

        if res[0] == "OK":
            return int(res[1])

        if res[0] == "ERR":
            reason = str(res[1]) if len(res) > 1 else "UNKNOWN"
            raise RuntimeError(f"HEARTBEAT failed: {reason}")

        raise RuntimeError(f"Unexpected HEARTBEAT response: {res}")

    def ack_success(self, *, queue: str, job_id: str, lease_token: str, now_ms_override: int = 0) -> None:
        base = self.queue_base(queue)
        nms = now_ms_override or now_ms()

        res = self.r.evalsha(self.scripts.ack_success_sha, 0, base, job_id, str(int(nms)), lease_token)

        if not isinstance(res, list) or len(res) < 1:
            raise RuntimeError(f"Unexpected ACK_SUCCESS response: {res}")

        if res[0] == "OK":
            return

        if res[0] == "ERR":
            reason = str(res[1]) if len(res) > 1 else "UNKNOWN"
            raise RuntimeError(f"ACK_SUCCESS failed: {reason}")

        raise RuntimeError(f"Unexpected ACK_SUCCESS response: {res}")

    def ack_fail(
        self,
        *,
        queue: str,
        job_id: str,
        lease_token: str,
        error: Optional[str] = None,
        now_ms_override: int = 0,
    ) -> AckFailResult:
        base = self.queue_base(queue)
        nms = now_ms_override or now_ms()

        if error is None or str(error).strip() == "":
            res = self.r.evalsha(self.scripts.ack_fail_sha, 0, base, job_id, str(int(nms)), lease_token)
        else:
            err_s = str(error)
            res = self.r.evalsha(self.scripts.ack_fail_sha, 0, base, job_id, str(int(nms)), lease_token, err_s)

        if not isinstance(res, list) or len(res) < 1:
            raise RuntimeError(f"Unexpected ACK_FAIL response: {res}")

        if res[0] == "RETRY":
            return ("RETRY", int(res[1]))

        if res[0] == "FAILED":
            return ("FAILED", None)

        if res[0] == "ERR":
            reason = str(res[1]) if len(res) > 1 else "UNKNOWN"
            raise RuntimeError(f"ACK_FAIL failed: {reason}")

        raise RuntimeError(f"Unexpected ACK_FAIL response: {res}")

    def promote_delayed(self, *, queue: str, max_promote: int = 1000, now_ms_override: int = 0) -> int:
        base = self.queue_base(queue)
        nms = now_ms_override or now_ms()

        res = self.r.evalsha(self.scripts.promote_delayed_sha, 0, base, str(int(nms)), str(int(max_promote)))

        if not isinstance(res, list) or len(res) < 2 or res[0] != "OK":
            raise RuntimeError(f"Unexpected PROMOTE_DELAYED response: {res}")

        return int(res[1])

    def reap_expired(self, *, queue: str, max_reap: int = 1000, now_ms_override: int = 0) -> int:
        base = self.queue_base(queue)
        nms = now_ms_override or now_ms()

        res = self.r.evalsha(self.scripts.reap_expired_sha, 0, base, str(int(nms)), str(int(max_reap)))

        if not isinstance(res, list) or len(res) < 2 or res[0] != "OK":
            raise RuntimeError(f"Unexpected REAP_EXPIRED response: {res}")

        return int(res[1])

    def job_timeout_ms(self, *, queue: str, job_id: str, default_ms: int = 60_000) -> int:
        base = self.queue_base(queue)
        k_job = base + ":job:" + job_id
        v = self.r.hget(k_job, "timeout_ms")
        try:
            n = int(v) if v is not None and v != "" else 0
        except Exception:
            n = 0
        return n if n > 0 else int(default_ms)

    @staticmethod
    def paused_backoff_s(poll_interval_s: float) -> float:
        return max(0.25, float(poll_interval_s) * 10.0)

    @staticmethod
    def derive_heartbeat_interval_s(timeout_ms: int) -> float:
        half = max(1.0, (float(timeout_ms) / 1000.0) / 2.0)
        return max(1.0, min(10.0, half))
