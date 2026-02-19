import json
import threading
import time
import signal
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from .client import OmniqClient
from .types import JobCtx, ReserveJob
from .exec import Exec

@dataclass
class StopController:
    stop: bool = False
    sigint_count: int = 0

@dataclass
class HeartbeatHandle:
    stop_evt: threading.Event
    flags: Dict[str, bool]
    thread: threading.Thread

def start_heartbeater(
    client: OmniqClient,
    *,
    queue: str,
    job_id: str,
    lease_token: str,
    interval_s: float,
) -> HeartbeatHandle:
    stop_evt = threading.Event()
    flags: Dict[str, bool] = {"lost": False}

    def _lost(msg: str) -> bool:
        msg_u = (msg or "").upper()
        return ("NOT_ACTIVE" in msg_u) or ("TOKEN_MISMATCH" in msg_u)

    def hb_loop():
        try:
            client.heartbeat(queue=queue, job_id=job_id, lease_token=lease_token)
        except Exception as e:
            if stop_evt.is_set():
                return
            msg = str(e)
            if _lost(msg):
                flags["lost"] = True
                stop_evt.set()
                return
            time.sleep(min(0.2, max(0.01, float(interval_s))))

        while True:
            if stop_evt.wait(interval_s):
                return
            try:
                client.heartbeat(queue=queue, job_id=job_id, lease_token=lease_token)
            except Exception as e:
                if stop_evt.is_set():
                    return
                msg = str(e)
                if _lost(msg):
                    flags["lost"] = True
                    stop_evt.set()
                    return
                time.sleep(min(0.2, max(0.01, float(interval_s))))

    t = threading.Thread(target=hb_loop, daemon=True)
    t.start()
    return HeartbeatHandle(stop_evt=stop_evt, flags=flags, thread=t)


def _safe_log(logger: Callable[[str], None], msg: str) -> None:
    try:
        logger(msg)
    except Exception:
        pass


def _payload_preview(payload: Any, max_len: int = 300) -> str:
    try:
        s = payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False)
    except Exception:
        s = str(payload)
    if len(s) > max_len:
        return s[:max_len] + "…"
    return s


def consume(
    client: OmniqClient,
    *,
    queue: str,
    handler: Callable[[JobCtx], None],
    poll_interval_s: float = 0.05,
    promote_interval_s: float = 1.0,
    promote_batch: int = 1000,
    reap_interval_s: float = 1.0,
    reap_batch: int = 1000,
    heartbeat_interval_s: Optional[float] = None,
    verbose: bool = False,
    logger: Callable[[str], None] = print,
    stop_on_ctrl_c: bool = True,
    drain: bool = True,
) -> None:
    ops = client.ops

    last_promote = 0.0
    last_reap = 0.0

    ctrl = StopController(stop=False, sigint_count=0)

    prev_sigterm = None
    prev_sigint = None

    try:
        if stop_on_ctrl_c and threading.current_thread() is threading.main_thread():
            def on_sigterm(signum, _frame):
                ctrl.stop = True
                if verbose:
                    _safe_log(logger, f"[consume] SIGTERM received; stopping... queue={queue}")

            prev_sigterm = signal.getsignal(signal.SIGTERM)
            signal.signal(signal.SIGTERM, on_sigterm)

            if drain:
                prev_sigint = signal.getsignal(signal.SIGINT)

                def on_sigint(signum, frame):
                    ctrl.sigint_count += 1
                    if ctrl.sigint_count >= 2:
                        if verbose:
                            _safe_log(logger, f"[consume] SIGINT x2; hard exit now. queue={queue}")
                        raise KeyboardInterrupt

                    ctrl.stop = True
                    if verbose:
                        _safe_log(logger, f"[consume] Ctrl+C received; draining current job then exiting. queue={queue}")

                signal.signal(signal.SIGINT, on_sigint)

        while True:
            if ctrl.stop:
                if verbose:
                    _safe_log(logger, f"[consume] stop requested; exiting (idle). queue={queue}")
                return

            now_s = time.time()

            if now_s - last_promote >= promote_interval_s:
                try:
                    client.promote_delayed(queue=queue, max_promote=promote_batch)
                except Exception:
                    pass
                last_promote = now_s

            if now_s - last_reap >= reap_interval_s:
                try:
                    client.reap_expired(queue=queue, max_reap=reap_batch)
                except Exception:
                    pass
                last_reap = now_s

            try:
                res = client.reserve(queue=queue)
            except Exception as e:
                if verbose:
                    _safe_log(logger, f"[consume] reserve error: {e}")
                time.sleep(0.2)
                continue

            if res is None:
                time.sleep(poll_interval_s)
                continue

            if getattr(res, "status", "") == "PAUSED":
                time.sleep(ops.paused_backoff_s(poll_interval_s))
                continue

            assert isinstance(res, ReserveJob)
            if not res.lease_token:
                if verbose:
                    _safe_log(logger, f"[consume] invalid reserve (missing lease_token) job_id={res.job_id}")
                time.sleep(0.2)
                continue

            if ctrl.stop and not drain:
                if verbose:
                    _safe_log(logger, f"[consume] stop requested; fast-exit after reserve job_id={res.job_id}")
                return

            try:
                payload_obj: Any = json.loads(res.payload)
            except Exception:
                payload_obj = res.payload

            exec = Exec(client=client, default_child_id=res.job_id)
            ctx = JobCtx(
                queue=queue,
                job_id=res.job_id,
                payload_raw=res.payload,
                payload=payload_obj,
                attempt=res.attempt,
                lock_until_ms=res.lock_until_ms,
                lease_token=res.lease_token,
                gid=res.gid,
                exec=exec,
            )

            if verbose:
                pv = _payload_preview(ctx.payload)
                gid_s = ctx.gid or "-"
                _safe_log(logger, f"[consume] received job_id={ctx.job_id} attempt={ctx.attempt} gid={gid_s} payload={pv}")

            if heartbeat_interval_s is not None:
                hb_s = float(heartbeat_interval_s)
            else:
                timeout_ms = ops.job_timeout_ms(queue=queue, job_id=res.job_id)
                hb_s = ops.derive_heartbeat_interval_s(timeout_ms)

            hb = start_heartbeater(
                client,
                queue=queue,
                job_id=res.job_id,
                lease_token=res.lease_token,
                interval_s=hb_s,
            )

            try:
                handler(ctx)

                if not hb.flags.get("lost", False):
                    try:
                        client.ack_success(queue=queue, job_id=res.job_id, lease_token=res.lease_token)
                        if verbose:
                            _safe_log(logger, f"[consume] ack success job_id={ctx.job_id}")
                    except Exception as e:
                        if verbose:
                            _safe_log(logger, f"[consume] ack success error job_id={ctx.job_id}: {e}")

            except KeyboardInterrupt:
                if verbose:
                    _safe_log(logger, f"[consume] KeyboardInterrupt; exiting now. queue={queue}")
                return

            except Exception as e:
                if not hb.flags.get("lost", False):
                    try:
                        err = f"{type(e).__name__}: {e}"
                        result = client.ack_fail(
                            queue=queue,
                            job_id=res.job_id,
                            lease_token=res.lease_token,
                            error=err,
                        )
                        if verbose:
                            if result[0] == "RETRY":
                                _safe_log(logger, f"[consume] ack fail job_id={ctx.job_id} => RETRY due_ms={result[1]}")
                            else:
                                _safe_log(logger, f"[consume] ack fail job_id={ctx.job_id} => FAILED")
                            _safe_log(logger, f"[consume] error job_id={ctx.job_id} => {err}")
                    except Exception as e2:
                        if verbose:
                            _safe_log(logger, f"[consume] ack fail error job_id={ctx.job_id}: {e2}")

            finally:
                try:
                    hb.stop_evt.set()
                except Exception:
                    pass
                try:
                    join_timeout = max(0.2, min(2.0, hb_s * 1.5))
                    hb.thread.join(timeout=join_timeout)
                except Exception:
                    pass

            if ctrl.stop and drain:
                if verbose:
                    _safe_log(logger, f"[consume] stop requested; exiting after draining job_id={ctx.job_id}")
                return

    except KeyboardInterrupt:
        if verbose:
            _safe_log(logger, f"[consume] KeyboardInterrupt (outer); exiting now. queue={queue}")
        return

    finally:
        if stop_on_ctrl_c and threading.current_thread() is threading.main_thread():
            try:
                if prev_sigterm is not None:
                    signal.signal(signal.SIGTERM, prev_sigterm)
            except Exception:
                pass
            try:
                if prev_sigint is not None:
                    signal.signal(signal.SIGINT, prev_sigint)
            except Exception:
                pass

        try:
            client.close()
        except Exception:
            pass
