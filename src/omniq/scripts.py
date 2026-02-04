import os
from dataclasses import dataclass
from typing import Protocol

class ScriptLoader(Protocol):
    def script_load(self, script: str) -> str: ...

@dataclass(frozen=True)
class OmniqScripts:
    enqueue_sha: str
    reserve_sha: str
    ack_success_sha: str
    ack_fail_sha: str
    promote_delayed_sha: str
    reap_expired_sha: str
    heartbeat_sha: str
    pause_sha: str
    resume_sha: str

def default_scripts_dir(current_file: str) -> str:
    here = os.path.dirname(os.path.abspath(current_file))
    return os.path.abspath(os.path.join(here, "..", "..", "scripts"))

def load_scripts(r: ScriptLoader, scripts_dir: str) -> OmniqScripts:
    def load_one(name: str) -> str:
        path = os.path.join(scripts_dir, name)
        with open(path, "r", encoding="utf-8") as f:
            return r.script_load(f.read())

    return OmniqScripts(
        enqueue_sha=load_one("enqueue.lua"),
        reserve_sha=load_one("reserve.lua"),
        ack_success_sha=load_one("ack_success.lua"),
        ack_fail_sha=load_one("ack_fail.lua"),
        promote_delayed_sha=load_one("promote_delayed.lua"),
        reap_expired_sha=load_one("reap_expired.lua"),
        heartbeat_sha=load_one("heartbeat.lua"),
        pause_sha=load_one("pause.lua"),
        resume_sha=load_one("resume.lua"),
    )
