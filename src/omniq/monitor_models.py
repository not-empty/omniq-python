from dataclasses import dataclass
from typing import Literal

LaneName = Literal["wait", "active", "delayed", "failed", "completed"]

@dataclass(frozen=True)
class QueueStats:
    queue: str
    paused: bool
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

@dataclass(frozen=True)
class GroupReady:
    gid: str
    score_ms: int

@dataclass(frozen=True)
class GroupStatus:
    gid: str
    inflight: int
    limit: int
    ready: bool
    waiting_count: int

@dataclass(frozen=True)
class LaneJob:
    lane: LaneName
    job_id: str
    idx_score_ms: int
    state: str
    gid: str
    attempt: int
    max_attempts: int
    due_ms: int
    lock_until_ms: int
    queued_ms: int
    first_started_ms: int
    last_started_ms: int
    completed_ms: int
    failed_ms: int
    updated_ms: int
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
    last_error_ms: int
    created_ms: int
    updated_ms: int
    queued_ms: int
    first_started_ms: int
    last_started_ms: int
    completed_ms: int
    failed_ms: int

@dataclass(frozen=True)
class QueueOverview:
    stats: QueueStats
    ready_groups: list[GroupReady]
    active: list[LaneJob]
    delayed: list[LaneJob]
    failed: list[LaneJob]
    completed: list[LaneJob]