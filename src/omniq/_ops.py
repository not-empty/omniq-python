import json
import redis

from dataclasses import dataclass
from typing import Optional, Any, List, ClassVar
from threading import Lock

from .clock import now_ms
from .ids import new_ulid
from .types import ReservePaused, ReserveJob, ReserveResult, AckFailResult, BatchRemoveResult, BatchRetryFailedResult
from .transport import RedisLike
from .scripts import OmniqScripts
from .helper import queue_base, queue_anchor, childs_anchor

@dataclass
class OmniqOps:
    _script_lock: ClassVar[Lock] = Lock()
    r: RedisLike
    scripts: OmniqScripts

    def _evalsha_with_noscript_fallback(
        self,
        sha: str,
        src: str,
        numkeys: int,
        *keys_and_args: Any,
    ):
        try:
            return self.r.evalsha(sha, numkeys, *keys_and_args)
        except redis.exceptions.NoScriptError:
            with self._script_lock:
                try:
                    return self.r.evalsha(sha, numkeys, *keys_and_args)
                except redis.exceptions.NoScriptError:
                    new_sha = self.r.script_load(src)
                    return self.r.evalsha(new_sha, numkeys, *keys_and_args)

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

        anchor = queue_anchor(queue)
        nms = now_ms_override or now_ms()

        jid = job_id or new_ulid()

        payload_s = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)

        gid_s = (gid or "").strip()
        glimit_s = str(int(group_limit)) if group_limit and group_limit > 0 else "0"

        argv = [
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

        res = self._evalsha_with_noscript_fallback(
            self.scripts.enqueue.sha,
            self.scripts.enqueue.src,
            1,
            anchor,
            *argv,
        )
        
        if not isinstance(res, list) or len(res) < 2:
            raise RuntimeError(f"Unexpected ENQUEUE response: {res}")

        status = str(res[0])
        out_id = str(res[1])

        if status != "OK":
            raise RuntimeError(f"ENQUEUE failed: {status}")

        return out_id

    def pause(self, *, queue: str) -> str:
        anchor = queue_anchor(queue)
        res = self._evalsha_with_noscript_fallback(
            self.scripts.pause.sha,
            self.scripts.pause.src,
            1,
            anchor
        )
        return str(res)

    def resume(self, *, queue: str) -> int:
        anchor = queue_anchor(queue)
        res = self._evalsha_with_noscript_fallback(
            self.scripts.resume.sha,
            self.scripts.resume.src,
            1,
            anchor
        )
        try:
            return int(res)
        except Exception:
            return 0

    def is_paused(self, *, queue: str) -> bool:
        base = queue_base(queue)
        return self.r.exists(base + ":paused") == 1

    def reserve(self, *, queue: str, now_ms_override: int = 0) -> ReserveResult:
        anchor = queue_anchor(queue)
        nms = now_ms_override or now_ms()

        res = self._evalsha_with_noscript_fallback(
            self.scripts.reserve.sha,
            self.scripts.reserve.src,
            1,
            anchor,
            str(int(nms)),
        )

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
        anchor = queue_anchor(queue)
        nms = now_ms_override or now_ms()

        res = self._evalsha_with_noscript_fallback(
            self.scripts.heartbeat.sha,
            self.scripts.heartbeat.src,
            1,
            anchor,
            job_id,
            str(int(nms)),
            lease_token
        )

        if not isinstance(res, list) or len(res) < 1:
            raise RuntimeError(f"Unexpected HEARTBEAT response: {res}")

        if res[0] == "OK":
            return int(res[1])

        if res[0] == "ERR":
            reason = str(res[1]) if len(res) > 1 else "UNKNOWN"
            raise RuntimeError(f"HEARTBEAT failed: {reason}")

        raise RuntimeError(f"Unexpected HEARTBEAT response: {res}")

    def ack_success(self, *, queue: str, job_id: str, lease_token: str, now_ms_override: int = 0) -> None:
        anchor = queue_anchor(queue)
        nms = now_ms_override or now_ms()

        res = self._evalsha_with_noscript_fallback(
            self.scripts.ack_success.sha,
            self.scripts.ack_success.src,
            1,
            anchor,
            job_id,
            str(int(nms)),
            lease_token
        )

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
        anchor = queue_anchor(queue)
        nms = now_ms_override or now_ms()

        if error is None or str(error).strip() == "":
            res = self._evalsha_with_noscript_fallback(
                self.scripts.ack_fail.sha,
                self.scripts.ack_fail.src,
                1,
                anchor,
                job_id,
                str(int(nms)),
                lease_token
            )
        else:
            err_s = str(error)
            res = self._evalsha_with_noscript_fallback(
                self.scripts.ack_fail.sha,
                self.scripts.ack_fail.src,
                1,
                anchor,
                job_id,
                str(int(nms)),
                lease_token,
                err_s
            )

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
        anchor = queue_anchor(queue)
        nms = now_ms_override or now_ms()

        res = self._evalsha_with_noscript_fallback(
            self.scripts.promote_delayed.sha,
            self.scripts.promote_delayed.src,
            1,
            anchor,
            str(int(nms)),
            str(int(max_promote))
        )

        if not isinstance(res, list) or len(res) < 2 or res[0] != "OK":
            raise RuntimeError(f"Unexpected PROMOTE_DELAYED response: {res}")

        return int(res[1])

    def reap_expired(self, *, queue: str, max_reap: int = 1000, now_ms_override: int = 0) -> int:
        anchor = queue_anchor(queue)
        nms = now_ms_override or now_ms()

        res = self._evalsha_with_noscript_fallback(
            self.scripts.reap_expired.sha,
            self.scripts.reap_expired.src,
            1,
            anchor,
            str(int(nms)),
            str(int(max_reap))
        )

        if not isinstance(res, list) or len(res) < 2 or res[0] != "OK":
            raise RuntimeError(f"Unexpected REAP_EXPIRED response: {res}")

        return int(res[1])

    def job_timeout_ms(self, *, queue: str, job_id: str, default_ms: int = 60_000) -> int:
        base = queue_base(queue)
        k_job = base + ":job:" + job_id
        v = self.r.hget(k_job, "timeout_ms")
        try:
            n = int(v) if v is not None and v != "" else 0
        except Exception:
            n = 0
        return n if n > 0 else int(default_ms)
    
    def retry_failed(self, *, queue: str, job_id: str, now_ms_override: int = 0) -> None:
        anchor = queue_anchor(queue)
        nms = now_ms_override or now_ms()

        res = self._evalsha_with_noscript_fallback(
            self.scripts.retry_failed.sha,
            self.scripts.retry_failed.src,
            1,
            anchor,
            job_id,
            str(int(nms)),
        )

        if not isinstance(res, list) or len(res) < 1:
            raise RuntimeError(f"Unexpected RETRY_FAILED response: {res}")

        if res[0] == "OK":
            return

        if res[0] == "ERR":
            reason = str(res[1]) if len(res) > 1 else "UNKNOWN"
            raise RuntimeError(f"RETRY_FAILED failed: {reason}")

        raise RuntimeError(f"Unexpected RETRY_FAILED response: {res}")

    def retry_failed_batch(
        self,
        *,
        queue: str,
        job_ids: List[str],
        now_ms_override: int = 0,
    ) -> BatchRetryFailedResult:
        if len(job_ids) > 100:
            raise ValueError("retry_failed_batch max is 100 job_ids per call")

        anchor = queue_anchor(queue)
        nms = now_ms_override or now_ms()

        argv: list[str] = [str(int(nms)), str(len(job_ids))]
        argv.extend([str(j) for j in job_ids])

        res = self._evalsha_with_noscript_fallback(
            self.scripts.retry_failed_batch.sha,
            self.scripts.retry_failed_batch.src,
            1,
            anchor,
            *argv,
        )

        if not isinstance(res, list):
            raise RuntimeError(f"Unexpected RETRY_FAILED_BATCH response: {res}")

        if len(res) >= 2 and str(res[0]) == "ERR":
            reason = str(res[1])
            extra = str(res[2]) if len(res) > 2 else ""
            raise RuntimeError(f"RETRY_FAILED_BATCH failed: {reason} {extra}".strip())

        out: BatchRetryFailedResult = []
        i = 0
        while i < len(res):
            job_id = str(res[i] or "")
            status = str(res[i + 1] or "")
            reason: Optional[str] = None
            if status == "ERR":
                reason = str(res[i + 2] or "UNKNOWN")
                i += 3
            else:
                i += 2
            out.append((job_id, status, reason))
        return out

    def remove_job(self, *, queue: str, job_id: str, lane: str) -> str:
        anchor = queue_anchor(queue)

        res = self._evalsha_with_noscript_fallback(
            self.scripts.remove_job.sha,
            self.scripts.remove_job.src,
            1,
            anchor,
            job_id,
            lane,
        )

        if not isinstance(res, list) or len(res) < 1:
            raise RuntimeError(f"Unexpected REMOVE_JOB response: {res}")

        if res[0] == "OK":
            return str(res[0] or "")

        if res[0] == "ERR":
            reason = str(res[1]) if len(res) > 1 else "UNKNOWN"
            raise RuntimeError(f"REMOVE_JOB failed: {reason}")

        raise RuntimeError(f"Unexpected REMOVE_JOB response: {res}")

    def remove_jobs_batch(
        self,
        *,
        queue: str,
        lane: str,
        job_ids: List[str],
    ) -> BatchRemoveResult:
        if len(job_ids) > 100:
            raise ValueError("remove_jobs_batch max is 100 job_ids per call")

        anchor = queue_anchor(queue)

        argv: list[str] = [str(lane), str(len(job_ids))]
        argv.extend([str(j) for j in job_ids])

        res = self._evalsha_with_noscript_fallback(
            self.scripts.remove_jobs_batch.sha,
            self.scripts.remove_jobs_batch.src,
            1,
            anchor,
            *argv,
        )

        if not isinstance(res, list):
            raise RuntimeError(f"Unexpected REMOVE_JOBS_BATCH response: {res}")

        if len(res) >= 2 and str(res[0]) == "ERR":
            reason = str(res[1])
            extra = str(res[2]) if len(res) > 2 else ""
            raise RuntimeError(f"REMOVE_JOBS_BATCH failed: {reason} {extra}".strip())

        out: BatchRemoveResult = []
        i = 0
        while i < len(res):
            job_id = str(res[i] or "")
            status = str(res[i + 1] or "")
            reason: Optional[str] = None
            if status == "ERR":
                reason = str(res[i + 2] or "UNKNOWN")
                i += 3
            else:
                i += 2
            out.append((job_id, status, reason))
        return out
    
    def childs_init(self, *, key: str, expected: int) -> None:
        anchor = childs_anchor(key)

        res = self._evalsha_with_noscript_fallback(
            self.scripts.childs_init.sha,
            self.scripts.childs_init.src,
            1,
            anchor,
            str(int(expected)),
        )

        if not isinstance(res, list) or len(res) < 1:
            raise RuntimeError(f"Unexpected CHILDS_INIT response: {res}")

        if res[0] == "OK":
            return

        if res[0] == "ERR":
            reason = str(res[1]) if len(res) > 1 else "UNKNOWN"
            raise RuntimeError(f"CHILDS_INIT failed: {reason}")

        raise RuntimeError(f"Unexpected CHILDS_INIT response: {res}")

    def child_ack(self, *, key: str, child_id: str) -> int:
        anchor = childs_anchor(key)

        cid = (str(child_id) if child_id is not None else "").strip()
        if not cid:
            raise ValueError("child_ack child_id is required")

        try:
            res = self._evalsha_with_noscript_fallback(
                self.scripts.child_ack.sha,
                self.scripts.child_ack.src,
                1,
                anchor,
                cid,
            )
        except Exception:
            return -1
        try:
            if not isinstance(res, list) or len(res) < 1:
                return -1

            if res[0] == "OK":
                if len(res) < 2:
                    return -1
                try:
                    return int(res[1])
                except Exception:
                    return -1

            if res[0] == "ERR":
                return -1

            return -1
        except Exception:
            return -1

    @staticmethod
    def paused_backoff_s(poll_interval_s: float) -> float:
        return max(0.25, float(poll_interval_s) * 10.0)

    @staticmethod
    def derive_heartbeat_interval_s(timeout_ms: int) -> float:
        half = max(1.0, (float(timeout_ms) / 1000.0) / 2.0)
        return max(1.0, min(10.0, half))
