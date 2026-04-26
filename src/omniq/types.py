from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple, Union, Literal, List

if TYPE_CHECKING:
    from .exec import Exec

PayloadT = Union[Dict[str, Any], list, str]

@dataclass(frozen=True)
class JobCtx:
    queue: str
    job_id: str
    payload_raw: str
    payload: PayloadT
    attempt: int
    max_attempts: int
    lock_until_ms: int
    lease_token: str
    exec: "Exec"
    gid: str = ""

@dataclass(frozen=True)
class ReservePaused:
    status: Literal["PAUSED"] = "PAUSED"

@dataclass(frozen=True)
class ReserveJob:
    status: Literal["JOB"]
    job_id: str
    payload: str
    lock_until_ms: int
    attempt: int
    max_attempts: int
    gid: str
    lease_token: str

AckFailResult = Tuple[Literal["RETRY", "FAILED"], Optional[int]]

@dataclass(frozen=True)
class BatchResultItem:
    job_id: str
    status: str
    reason: Optional[str] = None

BatchRemoveResult = List[BatchResultItem]
BatchRetryFailedResult = List[BatchResultItem]
ReserveResult = Union[None, ReservePaused, ReserveJob]
