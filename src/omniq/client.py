from dataclasses import dataclass, is_dataclass, asdict
from typing import Callable, Optional, Any

from ._ops import OmniqOps
from .scripts import load_scripts, default_scripts_dir
from .transport import RedisConnOpts, build_redis_client, RedisLike
from .types import ReserveResult, AckFailResult
from .helper import queue_base

def _safe_close_redis(r: Any) -> None:
    if r is None:
        return
    try:
        r.close()
        return
    except Exception:
        pass
    try:
        r.connection_pool.disconnect()
    except Exception:
        pass

@dataclass
class OmniqClient:
    _ops: OmniqOps

    def __init__(
        self,
        *,
        redis: Optional[RedisLike] = None,
        redis_url: Optional[str] = None,
        host: Optional[str] = None,
        port: int = 6379,
        db: int = 0,
        username: Optional[str] = None,
        password: Optional[str] = None,
        ssl: bool = False,
        scripts_dir: Optional[str] = None,
        client_name: Optional[str] = None,
    ):
        self._owns_redis = redis is None

        if redis is not None:
            r = redis
        else:
            r = build_redis_client(
                RedisConnOpts(
                    redis_url=redis_url,
                    host=host,
                    port=port,
                    db=db,
                    username=username,
                    password=password,
                    ssl=ssl,
                )
            )
        
        if client_name:
            try:
                r.client_setname(str(client_name))
            except Exception:
                pass

        if scripts_dir is None:
            scripts_dir = default_scripts_dir()
        scripts = load_scripts(r, scripts_dir)

        self._ops = OmniqOps(r=r, scripts=scripts)

    def close(self) -> None:
        if not getattr(self, "_owns_redis", False):
            return
        r = getattr(self._ops, "r", None)
        _safe_close_redis(r)

    def __enter__(self) -> "OmniqClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    @staticmethod
    def queue_base(queue_name: str) -> str:
        return queue_base(queue_name)

    def publish(
        self,
        *,
        queue: str,
        payload: Any,
        job_id: Optional[str] = None,
        max_attempts: int = 3,
        timeout_ms: int = 60_000,
        backoff_ms: int = 5_000,
        due_ms: int = 0,
        gid: Optional[str] = None,
        group_limit: int = 0,
    ) -> str:
        return self._ops.publish(
            queue=queue,
            payload=payload,
            job_id=job_id,
            max_attempts=max_attempts,
            timeout_ms=timeout_ms,
            backoff_ms=backoff_ms,
            due_ms=due_ms,
            gid=gid,
            group_limit=group_limit,
        )

    def publish_json(
        self,
        *,
        queue: str,
        payload: Any,
        job_id: Optional[str] = None,
        max_attempts: int = 3,
        timeout_ms: int = 60_000,
        backoff_ms: int = 5_000,
        due_ms: int = 0,
        gid: Optional[str] = None,
        group_limit: int = 0,
    ) -> str:
        if isinstance(payload, (dict, list)):
            structured = payload
        else:
            if is_dataclass(payload):
                structured = asdict(payload)

            elif hasattr(payload, "model_dump"):
                structured = payload.model_dump()

            elif hasattr(payload, "dict"):
                structured = payload.dict()

            else:
                raise TypeError(
                    "publish_json(payload=...) must be dict/list or a dataclass/pydantic model "
                    "that converts to structured JSON."
                )

        if not isinstance(structured, (dict, list)):
            raise TypeError(
                "publish_json(payload=...) must convert to dict or list (structured JSON)."
            )

        return self._ops.publish(
            queue=queue,
            payload=structured,
            job_id=job_id,
            max_attempts=max_attempts,
            timeout_ms=timeout_ms,
            backoff_ms=backoff_ms,
            due_ms=due_ms,
            gid=gid,
            group_limit=group_limit,
        )

    def reserve(self, *, queue: str, now_ms_override: int = 0) -> ReserveResult:
        return self._ops.reserve(queue=queue, now_ms_override=now_ms_override)

    def heartbeat(self, *, queue: str, job_id: str, lease_token: str, now_ms_override: int = 0) -> int:
        return self._ops.heartbeat(queue=queue, job_id=job_id, lease_token=lease_token, now_ms_override=now_ms_override)

    def ack_success(self, *, queue: str, job_id: str, lease_token: str, now_ms_override: int = 0) -> None:
        return self._ops.ack_success(queue=queue, job_id=job_id, lease_token=lease_token, now_ms_override=now_ms_override)

    def ack_fail(self, *, queue: str, job_id: str, lease_token: str, error: Optional[str] = None, now_ms_override: int = 0) -> AckFailResult:
        return self._ops.ack_fail(queue=queue, job_id=job_id, lease_token=lease_token, error=error, now_ms_override=now_ms_override)

    def promote_delayed(self, *, queue: str, max_promote: int = 1000, now_ms_override: int = 0) -> int:
        return self._ops.promote_delayed(queue=queue, max_promote=max_promote, now_ms_override=now_ms_override)

    def reap_expired(self, *, queue: str, max_reap: int = 1000, now_ms_override: int = 0) -> int:
        return self._ops.reap_expired(queue=queue, max_reap=max_reap, now_ms_override=now_ms_override)

    def pause(self, *, queue: str) -> str:
        return self._ops.pause(queue=queue)

    def resume(self, *, queue: str) -> int:
        return self._ops.resume(queue=queue)

    def is_paused(self, *, queue: str) -> bool:
        return self._ops.is_paused(queue=queue)

    def retry_failed(self, *, queue: str, job_id: str, now_ms_override: int = 0) -> None:
        return self._ops.retry_failed(queue=queue, job_id=job_id, now_ms_override=now_ms_override)

    def retry_failed_batch(self, *, queue: str, job_ids: list[str], now_ms_override: int = 0) -> None:
        return self._ops.retry_failed_batch(queue=queue, job_ids=job_ids, now_ms_override=now_ms_override)

    def remove_job(self, *, queue: str, job_id: str, lane: str) -> None:
        return self._ops.remove_job(queue=queue, job_id=job_id, lane=lane)
    
    def remove_jobs_batch(self, *, queue: str, lane: str, job_ids: list[str]):
        return self._ops.remove_jobs_batch(queue=queue, lane=lane, job_ids=job_ids)

    def childs_init(self, *, key: str, expected: int) -> None:
        return self._ops.childs_init(key=key, expected=expected)

    def child_ack(self, *, key: str, child_id: str) -> int:
        return self._ops.child_ack(key=key, child_id=child_id)

    def consume(
        self,
        *,
        queue: str,
        handler: Callable[[Any], None],
        poll_interval_s: float = 0.05,
        promote_interval_s: float = 1.0,
        promote_batch: int = 1000,
        reap_interval_s: float = 1.0,
        reap_batch: int = 1000,
        heartbeat_interval_s: Optional[float] = None,
        verbose: bool = False,
        logger: Callable[[str], None] = print,
        drain: bool = True,
    ) -> None:
        from .consumer import consume as consume_loop
        return consume_loop(
            self,
            queue=queue,
            handler=handler,
            poll_interval_s=poll_interval_s,
            promote_interval_s=promote_interval_s,
            promote_batch=promote_batch,
            reap_interval_s=reap_interval_s,
            reap_batch=reap_batch,
            heartbeat_interval_s=heartbeat_interval_s,
            verbose=verbose,
            logger=logger,
            drain=drain,
        )

    @property
    def ops(self) -> OmniqOps:
        return self._ops
