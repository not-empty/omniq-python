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

