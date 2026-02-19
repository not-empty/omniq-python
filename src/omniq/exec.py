from dataclasses import dataclass
from typing import Optional, Any

from .client import OmniqClient

@dataclass(frozen=True)
class Exec:
    client: OmniqClient
    default_child_id: str
    
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
        return self.client.publish(
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

    def pause(self, *, queue: str) -> str:
        return self.client.pause(queue=queue)

    def resume(self, *, queue: str) -> int:
        return self.client.resume(queue=queue)

    def is_paused(self, *, queue: str) -> bool:
        return self.client.is_paused(queue=queue)
    
    def childs_init(self, key: str, expected: int) -> None:
        self.client.childs_init(key=key, expected=int(expected))

    def child_ack(self, key: str, child_id: Optional[str] = None) -> int:
        cid = (child_id or self.default_child_id or "").strip()
        if not cid:
            raise ValueError("child_id is required (or provide default_child_id)")
        return int(self.client.child_ack(key=key, child_id=cid))
