import os
from dataclasses import dataclass
from typing import Protocol

class ScriptLoader(Protocol):
    def script_load(self, script: str) -> str: ...

@dataclass(frozen=True)
class ScriptDef:
    sha: str
    src: str

@dataclass(frozen=True)
class OmniqScripts:
    enqueue: ScriptDef
    reserve: ScriptDef
    ack_success: ScriptDef
    ack_fail: ScriptDef
    promote_delayed: ScriptDef
    reap_expired: ScriptDef
    heartbeat: ScriptDef
    pause: ScriptDef
    resume: ScriptDef

def default_scripts_dir() -> str:
    here = os.path.dirname(__file__)
    return os.path.join(here, "core", "scripts")

def load_scripts(r: ScriptLoader, scripts_dir: str) -> OmniqScripts:
    def load_one(name: str) -> ScriptDef:
        path = os.path.join(scripts_dir, name)
        with open(path, "r", encoding="utf-8") as f:
            src = f.read()
        sha = r.script_load(src)  # loads into *some* node cache (fine)
        return ScriptDef(sha=sha, src=src)

    return OmniqScripts(
        enqueue=load_one("enqueue.lua"),
        reserve=load_one("reserve.lua"),
        ack_success=load_one("ack_success.lua"),
        ack_fail=load_one("ack_fail.lua"),
        promote_delayed=load_one("promote_delayed.lua"),
        reap_expired=load_one("reap_expired.lua"),
        heartbeat=load_one("heartbeat.lua"),
        pause=load_one("pause.lua"),
        resume=load_one("resume.lua"),
    )
