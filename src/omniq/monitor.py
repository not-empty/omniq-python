from typing import Iterable, Optional

from .monitor_core import QueueMonitorCore
from .monitor_models import (
    GroupReady,
    GroupStatus,
    JobInfo,
    LaneJob,
    LaneName,
    QueueOverview,
    QueueStats,
)

class QueueMonitor:
    def __init__(self, uq):
        self._core = QueueMonitorCore(uq)

    def list_queues(self) -> list[str]:
        return self._core.list_queues()

    def stats(self, queue: str) -> QueueStats:
        return self._core.stats(queue)

    def stats_many(self, queues: Optional[Iterable[str]] = None) -> list[QueueStats]:
        return self._core.stats_many(queues)

    def groups_ready(
        self,
        queue: str,
        offset: int = 0,
        limit: int = 200,
    ) -> list[str]:
        return self._core.groups_ready(
            queue=queue,
            offset=offset,
            limit=limit,
        )

    def groups_ready_with_scores(
        self,
        queue: str,
        offset: int = 0,
        limit: int = 200,
    ) -> list[GroupReady]:
        return self._core.groups_ready_with_scores(
            queue=queue,
            offset=offset,
            limit=limit,
        )

    def group_status(
        self,
        queue: str,
        gids: list[str],
        default_limit: int = 1,
    ) -> list[GroupStatus]:
        return self._core.group_status(
            queue=queue,
            gids=gids,
            default_limit=default_limit,
        )

    def lane_page(
        self,
        queue: str,
        lane: LaneName,
        offset: int = 0,
        limit: int = 25,
        reverse: bool = False,
    ) -> list[LaneJob]:
        return self._core.lane_page(
            queue=queue,
            lane=lane,
            offset=offset,
            limit=limit,
            reverse=reverse,
        )

    def get_job(self, queue: str, job_id: str) -> Optional[JobInfo]:
        return self._core.get_job(queue, job_id)

    def find_jobs(
        self,
        queue: str,
        lane: LaneName,
        job_ids: Iterable[str],
    ) -> list[LaneJob]:
        return self._core.find_jobs(queue, lane, job_ids)

    def overview(self, queue: str, samples_per_lane: int = 10) -> QueueOverview:
        return self._core.overview(queue, samples_per_lane)