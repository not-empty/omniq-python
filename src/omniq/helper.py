from typing import Any

def queue_base(queue_name: str) -> str:
    if "{" in queue_name and "}" in queue_name:
        return queue_name
    return "{" + queue_name + "}"

def queue_anchor(queue_name: str) -> str:
    return queue_base(queue_name) + ":meta"

def as_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", errors="replace")
    return str(v)

def check_completion_anchor(key: str, max_len: int = 128) -> str:
    k = (key or "").strip()
    if not k:
        raise ValueError("check_completion key is required")

    if "{" in k or "}" in k:
        raise ValueError("check_completion key must not contain '{' or '}'")

    if len(k) > max_len:
        raise ValueError(f"check_completion key too long (max {max_len} chars)")

    return "{cc:" + k + "}:meta"