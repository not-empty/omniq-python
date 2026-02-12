from dataclasses import dataclass
from typing import Optional

from ._ops import OmniqOps


@dataclass(frozen=True)
class CheckCompletion:
    ops: OmniqOps
    default_child_id: str

    def InitJobCounter(self, key: str, expected: int) -> None:
        self.ops.check_completion_init_job_counter(key=key, expected=int(expected))

    def JobDecrement(self, key: str, child_id: Optional[str] = None) -> int:
        cid = (child_id or self.default_child_id or "").strip()
        if not cid:
            raise ValueError("child_id is required (or provide default_child_id)")
        return int(self.ops.check_completion_job_decrement(key=key, child_id=cid))
