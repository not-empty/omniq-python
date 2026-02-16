from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, Union, Literal, List

PayloadT = Union[Dict[str, Any], list, str]

@dataclass(frozen=True)
class JobCtx:
    queue: str
    job_id: str
    payload_raw: str
    payload: PayloadT
    attempt: int
    lock_until_ms: int
    lease_token: str
    gid: str = ""
    exec: Any = None

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
    gid: str
    lease_token: str

AckFailResult = Tuple[Literal["RETRY", "FAILED"], Optional[int]]
BatchRemoveResult = List[Tuple[str, str, Optional[str]]]
BatchRetryFailedResult = List[Tuple[str, str, Optional[str]]]
ReserveResult = Union[None, ReservePaused, ReserveJob]
