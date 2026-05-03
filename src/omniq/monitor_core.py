from typing import Any, Dict, Iterable, Optional

from .helper import as_str, queue_base, validate_queue_name
from .monitor_models import (
    GroupReady,
    GroupStatus,
    JobInfo,
    LaneJob,
    LaneName,
    QueueOverview,
    QueueStats,
)

class QueueMonitorCore:
    QUEUE_SCAN_MATCH = "*:stats"

    MAX_LIST_LIMIT = 25
    MAX_GROUP_LIMIT = 500

    def __init__(self, uq: Any):
        self._uq = uq
        self._r = (
            getattr(uq, "r", None)
            or getattr(getattr(uq, "ops", None), "r", None)
            or getattr(getattr(uq, "_ops", None), "r", None)
        )
        if self._r is None:
            raise ValueError(
                "QueueMonitor needs redis access (inject from server, do not expose to UI callers)."
            )

    def _base(self, queue: str) -> str:
        return queue_base(queue)

    def _stats_key(self, base: str) -> str:
        return f"{base}:stats"

    def _paused_key(self, base: str) -> str:
        return f"{base}:paused"

    def _ready_key(self, base: str) -> str:
        return f"{base}:groups:ready"

    def _job_key(self, base: str, job_id: str) -> str:
        return f"{base}:job:{job_id}"

    def _idx_key(self, base: str, lane: LaneName) -> str:
        return f"{base}:idx:{lane}"

    def _gwait_key(self, base: str, gid: str) -> str:
        return f"{base}:g:{gid}:wait"

    def _ginflight_key(self, base: str, gid: str) -> str:
        return f"{base}:g:{gid}:inflight"

    def _glimit_key(self, base: str, gid: str) -> str:
        return f"{base}:g:{gid}:limit"

    @staticmethod
    def _to_int(value: Any, default: int = 0) -> int:
        s = as_str(value)
        if s == "":
            return default
        try:
            return int(float(s))
        except Exception:
            return default

    @staticmethod
    def _normalize_queue_name(base_or_queue: str) -> str:
        value = (base_or_queue or "").strip()
        if value.startswith("{") and value.endswith("}"):
            return value[1:-1]
        return value

    @staticmethod
    def _decode_hash(raw: Dict[Any, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for k, v in (raw or {}).items():
            out[as_str(k)] = v
        return out

    def _clamp_list_limit(self, limit: int) -> int:
        return max(1, min(int(limit), self.MAX_LIST_LIMIT))

    def _clamp_group_limit(self, limit: int) -> int:
        return max(1, min(int(limit), self.MAX_GROUP_LIMIT))

    def _read_job_map(self, base: str, job_id: str) -> Optional[Dict[str, Any]]:
        key = self._job_key(base, job_id)
        try:
            if self._r.exists(key) != 1:
                return None
            raw = self._r.hgetall(key) or {}
        except Exception:
            return None
        return self._decode_hash(raw)

    def _is_group_ready(self, base: str, gid: str) -> bool:
        try:
            return self._r.zscore(self._ready_key(base), gid) is not None
        except Exception:
            return False

    def _job_info_from_map(self, job_id: str, m: Dict[str, Any]) -> JobInfo:
        return JobInfo(
            job_id=job_id,
            state=as_str(m.get("state")),
            gid=as_str(m.get("gid")),
            attempt=self._to_int(m.get("attempt")),
            max_attempts=self._to_int(m.get("max_attempts")),
            timeout_ms=self._to_int(m.get("timeout_ms")),
            backoff_ms=self._to_int(m.get("backoff_ms")),
            lease_token=as_str(m.get("lease_token")),
            lock_until_ms=self._to_int(m.get("lock_until_ms")),
            due_ms=self._to_int(m.get("due_ms")),
            payload=as_str(m.get("payload")),
            last_error=as_str(m.get("last_error")),
            last_error_ms=self._to_int(m.get("last_error_ms")),
            created_ms=self._to_int(m.get("created_ms")),
            updated_ms=self._to_int(m.get("updated_ms")),
            queued_ms=self._to_int(m.get("queued_ms")),
            first_started_ms=self._to_int(m.get("first_started_ms")),
            last_started_ms=self._to_int(m.get("last_started_ms")),
            completed_ms=self._to_int(m.get("completed_ms")),
            failed_ms=self._to_int(m.get("failed_ms")),
        )

    def _lane_job_from_map(
        self,
        lane: LaneName,
        job_id: str,
        idx_score_ms: int,
        m: Dict[str, Any],
    ) -> LaneJob:
        return LaneJob(
            lane=lane,
            job_id=job_id,
            idx_score_ms=idx_score_ms,
            state=as_str(m.get("state")),
            gid=as_str(m.get("gid")),
            attempt=self._to_int(m.get("attempt")),
            max_attempts=self._to_int(m.get("max_attempts")),
            due_ms=self._to_int(m.get("due_ms")),
            lock_until_ms=self._to_int(m.get("lock_until_ms")),
            queued_ms=self._to_int(m.get("queued_ms")),
            first_started_ms=self._to_int(m.get("first_started_ms")),
            last_started_ms=self._to_int(m.get("last_started_ms")),
            completed_ms=self._to_int(m.get("completed_ms")),
            failed_ms=self._to_int(m.get("failed_ms")),
            updated_ms=self._to_int(m.get("updated_ms")),
            last_error=as_str(m.get("last_error")),
        )

    def scan_queues(self) -> list[str]:
        try:
            keys = self._r.scan_iter(match=self.QUEUE_SCAN_MATCH, _type="hash")
        except Exception:
            return []

        names: list[str] = []
        seen: set[str] = set()
        for raw_key in keys:
            key = as_str(raw_key)
            if not key.endswith(":stats"):
                continue

            base = key[: -len(":stats")]
            name = self._normalize_queue_name(base)
            if not name or name in seen:
                continue

            try:
                validate_queue_name(name)
            except Exception:
                continue

            seen.add(name)
            names.append(name)

        names.sort()
        return names

    def stats(self, queue: str) -> QueueStats:
        base = self._base(queue)

        try:
            raw = self._r.hgetall(self._stats_key(base)) or {}
        except Exception:
            raw = {}

        stats_map = self._decode_hash(raw)

        try:
            paused = self._r.exists(self._paused_key(base)) == 1
        except Exception:
            paused = False

        waiting = self._to_int(stats_map.get("waiting"))
        group_waiting = self._to_int(stats_map.get("group_waiting"))
        waiting_total = self._to_int(stats_map.get("waiting_total"))

        if waiting_total <= 0 and (waiting > 0 or group_waiting > 0):
            waiting_total = waiting + group_waiting

        return QueueStats(
            queue=self._normalize_queue_name(queue),
            paused=paused,
            waiting=waiting,
            group_waiting=group_waiting,
            waiting_total=waiting_total,
            active=self._to_int(stats_map.get("active")),
            delayed=self._to_int(stats_map.get("delayed")),
            failed=self._to_int(stats_map.get("failed")),
            completed_kept=self._to_int(stats_map.get("completed_kept")),
            groups_ready=self._to_int(stats_map.get("groups_ready")),
            last_activity_ms=self._to_int(stats_map.get("last_activity_ms")),
            last_enqueue_ms=self._to_int(stats_map.get("last_enqueue_ms")),
            last_reserve_ms=self._to_int(stats_map.get("last_reserve_ms")),
            last_finish_ms=self._to_int(stats_map.get("last_finish_ms")),
        )

    def stats_many(self, queues: Optional[Iterable[str]] = None) -> list[QueueStats]:
        target = list(queues) if queues is not None else self.scan_queues()
        return [self.stats(q) for q in target]

    def groups_ready(
        self,
        queue: str,
        offset: int = 0,
        limit: int = 200,
    ) -> list[str]:
        rows = self.groups_ready_with_scores(queue=queue, offset=offset, limit=limit)
        return [x.gid for x in rows]

    def groups_ready_with_scores(
        self,
        queue: str,
        offset: int = 0,
        limit: int = 200,
    ) -> list[GroupReady]:
        base = self._base(queue)
        offset = max(0, int(offset))
        limit = self._clamp_group_limit(limit)

        try:
            rows = self._r.zrange(
                self._ready_key(base),
                offset,
                offset + limit - 1,
                withscores=True,
            )
        except Exception:
            return []

        return [
            GroupReady(gid=as_str(gid), score_ms=self._to_int(score))
            for gid, score in rows
            if as_str(gid)
        ]

    def group_status(
        self,
        queue: str,
        gids: list[str],
        default_limit: int = 1,
    ) -> list[GroupStatus]:
        base = self._base(queue)
        default_limit = max(1, int(default_limit))

        normalized_gids = [as_str(g) for g in gids if as_str(g)]
        normalized_gids = normalized_gids[: self.MAX_GROUP_LIMIT]

        out: list[GroupStatus] = []
        for gid_s in normalized_gids:
            try:
                inflight = self._to_int(self._r.get(self._ginflight_key(base, gid_s)))
            except Exception:
                inflight = 0

            try:
                raw_limit = self._to_int(self._r.get(self._glimit_key(base, gid_s)))
            except Exception:
                raw_limit = 0

            limit = raw_limit if raw_limit > 0 else default_limit

            try:
                waiting_count = self._to_int(self._r.llen(self._gwait_key(base, gid_s)))
            except Exception:
                waiting_count = 0

            out.append(
                GroupStatus(
                    gid=gid_s,
                    inflight=inflight,
                    limit=limit,
                    ready=self._is_group_ready(base, gid_s),
                    waiting_count=waiting_count,
                )
            )
        return out

    def lane_page(
        self,
        queue: str,
        lane: LaneName,
        offset: int = 0,
        limit: int = 25,
        reverse: bool = False,
    ) -> list[LaneJob]:
        base = self._base(queue)
        offset = max(0, int(offset))
        limit = self._clamp_list_limit(limit)
        key = self._idx_key(base, lane)

        try:
            if reverse:
                rows = self._r.zrevrange(key, offset, offset + limit - 1, withscores=True)
            else:
                rows = self._r.zrange(key, offset, offset + limit - 1, withscores=True)
        except Exception:
            return []

        out: list[LaneJob] = []
        for raw_job_id, raw_score in rows:
            job_id = as_str(raw_job_id)
            if not job_id:
                continue

            m = self._read_job_map(base, job_id)
            if not m:
                continue

            out.append(
                self._lane_job_from_map(
                    lane=lane,
                    job_id=job_id,
                    idx_score_ms=self._to_int(raw_score),
                    m=m,
                )
            )
        return out

    def get_job(self, queue: str, job_id: str) -> Optional[JobInfo]:
        base = self._base(queue)
        job_id = as_str(job_id)
        if not job_id:
            return None
        m = self._read_job_map(base, job_id)
        if not m:
            return None
        return self._job_info_from_map(job_id, m)

    def find_jobs(
        self,
        queue: str,
        lane: LaneName,
        job_ids: Iterable[str],
    ) -> list[LaneJob]:
        base = self._base(queue)
        idx_key = self._idx_key(base, lane)
        out: list[LaneJob] = []

        for raw_job_id in job_ids:
            job_id = as_str(raw_job_id)
            if not job_id:
                continue

            try:
                score = self._r.zscore(idx_key, job_id)
            except Exception:
                score = None

            if score is None:
                continue

            m = self._read_job_map(base, job_id)
            if not m:
                continue

            out.append(
                self._lane_job_from_map(
                    lane=lane,
                    job_id=job_id,
                    idx_score_ms=self._to_int(score),
                    m=m,
                )
            )
        return out

    def overview(self, queue: str, samples_per_lane: int = 10) -> QueueOverview:
        samples_per_lane = self._clamp_list_limit(samples_per_lane)

        return QueueOverview(
            stats=self.stats(queue),
            ready_groups=self.groups_ready_with_scores(
                queue,
                limit=samples_per_lane,
            ),
            active=self.lane_page(queue, "active", limit=samples_per_lane),
            delayed=self.lane_page(queue, "delayed", limit=samples_per_lane),
            failed=self.lane_page(queue, "failed", limit=samples_per_lane),
            completed=self.lane_page(queue, "completed", limit=samples_per_lane),
        )
