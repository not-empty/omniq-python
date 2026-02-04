from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional, Protocol

import redis

class RedisLike(Protocol):
    def evalsha(self, sha: str, numkeys: int, *args: str) -> Any: ...
    def script_load(self, script: str) -> str: ...
    def exists(self, key: str) -> int: ...
    def hget(self, key: str, field: str) -> Optional[str]: ...


@dataclass(frozen=True)
class RedisConnOpts:
    redis_url: Optional[str] = None
    host: Optional[str] = None
    port: int = 6379
    db: int = 0
    username: Optional[str] = None
    password: Optional[str] = None
    ssl: bool = False
    socket_timeout: Optional[float] = None
    socket_connect_timeout: Optional[float] = None


def build_redis_client(opts: RedisConnOpts) -> redis.Redis:
    if opts.redis_url:
        return redis.Redis.from_url(opts.redis_url, decode_responses=True)

    if not opts.host:
        raise ValueError("RedisConnOpts requires host (or redis_url)")

    return redis.Redis(
        host=opts.host,
        port=int(opts.port),
        db=int(opts.db),
        username=opts.username,
        password=opts.password,
        ssl=bool(opts.ssl),
        socket_timeout=opts.socket_timeout,
        socket_connect_timeout=opts.socket_connect_timeout,
        decode_responses=True,
    )
