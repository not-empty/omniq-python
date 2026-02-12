from dataclasses import dataclass
from typing import Any, Optional, Protocol, Union

import redis

try:
    from redis.cluster import RedisCluster
except Exception:
    RedisCluster = None

RedisArg = Union[str, bytes, int, float]

class RedisLike(Protocol):
    def evalsha(self, sha: str, numkeys: int, *args: RedisArg) -> Any: ...
    def eval(self, script: str, numkeys: int, *args: RedisArg) -> Any: ...
    def script_load(self, script: str) -> str: ...
    def exists(self, key: str) -> int: ...
    def hget(self, key: str, field: str) -> Optional[str]: ...
    def llen(self, key: str) -> int: ...
    def lrange(self, key: str, start: int, end: int) -> list[Optional[str]]: ...
    def zcard(self, key: str) -> int: ...
    def zrange(self, key: str, start: int, end: int) -> list[Optional[str]]: ...
    def get(self, key: str) -> Optional[str]: ...
    def hmget(self, key: str, *fields: str) -> list[Optional[str]]: ...
    def zscore(self, key: str, member: str) -> list[Optional[str]]: ...

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

def _looks_like_cluster_error(e: Exception) -> bool:
    msg = str(e).lower()

    return (
        "cluster support disabled" in msg
        or "cluster mode is not enabled" in msg
        or "unknown command" in msg and "cluster" in msg
        or "this instance has cluster support disabled" in msg
        or "err this instance has cluster support disabled" in msg
        or "only (p)subscribe / (p)unsubscribe / ping / quit allowed in this context" in msg
        or "moved" in msg
        or "ask" in msg
    )

def build_redis_client(opts: RedisConnOpts) -> redis.Redis:
    if opts.redis_url:
        return redis.Redis.from_url(opts.redis_url, decode_responses=True)

    if not opts.host:
        raise ValueError("RedisConnOpts requires host (or redis_url)")

    if RedisCluster is not None:
        try:
            rc = RedisCluster(
                host=opts.host,
                port=int(opts.port),
                username=opts.username,
                password=opts.password,
                ssl=bool(opts.ssl),
                socket_timeout=opts.socket_timeout,
                socket_connect_timeout=opts.socket_connect_timeout,
                decode_responses=True,
            )
            rc.ping()
            return rc
        except Exception as e:
            if _looks_like_cluster_error(e):
                pass
            else:
                raise

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

