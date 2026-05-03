import re

from typing import Any

QUEUE_NAME_MAX_LEN = 128
QUEUE_NAME_RE = re.compile(r"^[A-Za-z0-9._-]+$")

def validate_queue_name(queue_name: str, max_len: int = QUEUE_NAME_MAX_LEN) -> str:
    value = "" if queue_name is None else str(queue_name)

    if value == "":
        raise ValueError("queue name is required")

    if value != value.strip():
        raise ValueError("queue name must not have leading or trailing whitespace")

    if len(value) > max_len:
        raise ValueError(f"queue name too long (max {max_len} chars)")

    if not QUEUE_NAME_RE.fullmatch(value):
        raise ValueError(
            "queue name contains invalid characters; allowed: letters, numbers, '.', '_', '-'"
        )

    return value

def queue_base(queue_name: str) -> str:
    value = validate_queue_name(queue_name)
    return "{" + value + "}"

def queue_anchor(queue_name: str) -> str:
    return queue_base(queue_name) + ":meta"

def as_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", errors="replace")
    return str(v)

def childs_anchor(key: str, max_len: int = 128) -> str:
    k = (key or "").strip()
    if not k:
        raise ValueError("childs_anchor key is required")

    if "{" in k or "}" in k:
        raise ValueError("childs_anchor key must not contain '{' or '}'")

    if len(k) > max_len:
        raise ValueError(f"childs_anchor key too long (max {max_len} chars)")

    return "{cc:" + k + "}:meta"
