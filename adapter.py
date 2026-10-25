"""Asar platform adapter for Hermes Agent.

Asar (https://asar.skp.kz) is a workspace where people and AI agents work as one team. The agent dials
out to Asar over a WebSocket (no inbound ports, no tunnels): ``wss://<asar>/agent/v1/ws?key=ask_…``.
Protocol (docs/agent-link-protocol.md in skp-kz/asar):
  Asar → agent: {type:"hello", agent, channels, skills, cursor} · {type:"message", id, channel{id,name,kind},
                 from{id,name,kind}, text, mentioned, parent_id, ts, cursor} · {type:"skills", skills} · {type:"pong"}
  agent → Asar: {type:"ack", cursor} · {type:"post", ref, channel_id, text} · {type:"status", state} · {type:"ping"}
Env (``~/.hermes/.env``): ASAR_KEY (required), ASAR_URL, ASAR_WAKE_ON (mention|all), ASAR_HOME_CHANNEL,
ASAR_ALLOW_ALL_USERS (the installer sets it to true: who may write to the agent is decided in Asar).
Uses the ``websockets`` package that ships with Hermes. Works with Hermes from Aug 2026 onwards (no gateway.platforms._shared).
"""

import asyncio
import contextlib
import datetime
import inspect
import json
import logging
import os
import shutil
import tempfile
import time
import urllib.request
import zipfile
from typing import Any, Dict, Optional

from gateway.platforms.base import BasePlatformAdapter, SendResult
try:  # новые версии
    from gateway.platforms.event import MessageEvent, MessageType
except ImportError:  # старые: реэкспорт из base
    from gateway.platforms.base import MessageEvent, MessageType
from gateway.config import Platform
try:
    from gateway.platforms.helpers import MessageDeduplicator
except ImportError:
    MessageDeduplicator = None

# --- секреты: новый _shared или старый agent.secret_scope, иначе os.environ ---
try:
    from gateway.platforms._shared import get_scoped_secret as _secret
except ImportError:
    try:
        from agent.secret_scope import UnscopedSecretError as _Unscoped, get_secret as _scoped_get
        def _secret(name, default=""):
            try:
                v = _scoped_get(name, default)
            except _Unscoped:
                v = os.environ.get(name, default)
            return v if v is not None else default
    except ImportError:
        def _secret(name, default=""):
            return os.environ.get(name, default)

logger = logging.getLogger(__name__)

DEFAULT_URL = "https://asar.skp.kz"
BACKOFF = (1, 2, 5, 10, 30)
BACKLOG_MINUTES = 30  # events older than this (queued while the gateway was down) are acked but not answered


def _env_or_extra(extra: dict, env: str, key: str, default: Any = "") -> Any:
    return _secret(env, "") or extra.get(key, default)


def _ws_url(base: str, key: str) -> str:
    base = base.rstrip("/")
    return base.replace("https://", "wss://", 1).replace("http://", "ws://", 1) + "/agent/v1/ws?key=" + key


def _age_minutes(ts: Optional[str]) -> float:
    if not ts:
        return 0.0
    try:
        t = datetime.datetime.fromisoformat(ts.replace(" ", "T").replace("Z", "+00:00"))
        if t.tzinfo is None:
            t = t.replace(tzinfo=datetime.timezone.utc)
        return (datetime.datetime.now(datetime.timezone.utc) - t).total_seconds() / 60
    except Exception:
        return 0.0


async def _cancel(task: Optional[asyncio.Task]) -> None:
    if task and not task.done():
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task


class _Dedup:
    """Запасной дедупликатор, если в этой версии нет helpers.MessageDeduplicator."""
    def __init__(self, ttl_seconds=600):
        self.ttl = ttl_seconds; self.seen: Dict[str, float] = {}
    def is_duplicate(self, mid: str) -> bool:
        now = time.time()
        for k in [k for k, t in self.seen.items() if now - t > self.ttl]:
            self.seen.pop(k, None)
        if mid in self.seen:
            return True
        self.seen[mid] = now
        return False


class AsarAdapter(BasePlatformAdapter):
    def __init__(self, config, **kwargs):
        super().__init__(config=config, platform=Platform("asar"))
        extra = getattr(config, "extra", {}) or {}
        self.url: str = (_env_or_extra(extra, "ASAR_URL", "url", DEFAULT_URL) or DEFAULT_URL).rstrip("/")
        self.key: str = _env_or_extra(extra, "ASAR_KEY", "key", "")
        self.wake_on: str = (_env_or_extra(extra, "ASAR_WAKE_ON", "wake_on", "mention") or "mention").lower()
        self._ws = None
        self._task: Optional[asyncio.Task] = None
        self._ping_task: Optional[asyncio.Task] = None
        self._closed = False
        self._dedup = (MessageDeduplicator or _Dedup)(ttl_seconds=600)
        self._channels: Dict[str, dict] = {}
        self._agent: dict = {}
        self._ref = 0
        self._pending: Dict[str, asyncio.Future] = {}

    @property
    def name(self) -> str:
        return "Asar"

    def _fatal(self, code: str, msg: str, retryable: bool) -> bool:
        if hasattr(self, "_set_fatal_error"):
            self._set_fatal_error(code, msg, retryable=retryable)
        else:
            logger.error("Asar: %s — %s", code, msg)
        return False

    # ── lifecycle ────────────────────────────────────────────────────────────
    async def connect(self, *, is_reconnect: bool = False) -> bool:
        if not self.key:
            return self._fatal("config_missing", "ASAR_KEY is not set (run the Asar installer with a code)", False)
        if hasattr(self, "_acquire_platform_lock") and not self._acquire_platform_lock("asar", self.key[-12:], "Asar agent key"):
            return False
        try:
            import websockets  # bundled with Hermes
        except ImportError:
            return self._fatal("deps_missing", "websockets package is missing", False)
        self._closed = False
        ready: asyncio.Future = asyncio.get_running_loop().create_future()
        self._task = asyncio.create_task(self._run(websockets, ready))
        try:
            ok = await asyncio.wait_for(ready, timeout=30.0)
        except asyncio.TimeoutError:
            ok = False
        if not ok:
            await self.disconnect()
            return self._fatal("connect_failed", "Asar did not answer hello in 30 s", True)
        self._mark_connected()
        if hasattr(self, "_wire_plugin_handlers"):
            with contextlib.suppress(Exception):
                self._wire_plugin_handlers(None)
        logger.info("Asar: connected as %s (%s), channels: %d", self._agent.get("name"), self._agent.get("id"), len(self._channels))
        return True

    async def disconnect(self) -> None:
        self._closed = True
        if hasattr(self, "_release_platform_lock"):
            with contextlib.suppress(Exception):
                self._release_platform_lock()
        self._mark_disconnected()
        if self._ws:
            with contextlib.suppress(Exception):
                await self._ws.close(1000, "stop")
        await _cancel(self._ping_task)
        await _cancel(self._task)
        self._ws = None

    # ── socket loop with reconnect ───────────────────────────────────────────
    async def _run(self, websockets, ready: asyncio.Future) -> None:
        attempt = 0
        while not self._closed:
            try:
                async with websockets.connect(_ws_url(self.url, self.key), max_size=4 * 1024 * 1024) as ws:
                    self._ws = ws
                    attempt = 0
                    self._ping_task = asyncio.create_task(self._pinger())
                    async for raw in ws:
                        await self._on_raw(raw, ready)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                code = getattr(e, "code", None) or getattr(getattr(e, "rcvd", None), "code", None)
                if code == 4401:
                    logger.error("Asar: key revoked — reissue a code in Asar → Agents")
                    self._fatal("key_revoked", "Asar key revoked", False)
                    if hasattr(self, "_notify_fatal_error"):
                        with contextlib.suppress(Exception):
                            await self._notify_fatal_error()
                    return
                logger.warning("Asar: socket closed (%s), reconnecting", e)
            finally:
                self._ws = None
                await _cancel(self._ping_task)
                self._ping_task = None
                for f in self._pending.values():
                    if not f.done():
                        f.set_exception(ConnectionError("closed"))
                self._pending.clear()
            if self._closed:
                break
            if not ready.done():
                ready.set_result(False)
                return
            await asyncio.sleep(BACKOFF[min(attempt, len(BACKOFF) - 1)])
            attempt += 1

    async def _pinger(self) -> None:
        while True:
            await asyncio.sleep(30)
            await self._send({"type": "ping", "t": int(time.time() * 1000)})

    async def _send(self, obj: dict) -> None:
        if self._ws:
            await self._ws.send(json.dumps(obj, ensure_ascii=False))

    async def _on_raw(self, raw, ready: asyncio.Future) -> None:
        try:
            m = json.loads(raw)
        except Exception:
            return
        t = m.get("type")
        if t == "hello":
            self._agent = m.get("agent") or {}
            self._channels = {c["id"]: c for c in m.get("channels") or []}
            if not ready.done():
                ready.set_result(True)
            asyncio.create_task(self._sync_skills(m.get("skills") or []))
            return
        if t == "skills":
            asyncio.create_task(self._sync_skills(m.get("skills") or []))
            return
        if t in ("posted", "error"):
            f = self._pending.pop(str(m.get("ref")), None)
            if f and not f.done():
                f.set_exception(RuntimeError(m.get("error", "error"))) if t == "error" else f.set_result(m)
            return
        if t == "message":
            if "cursor" in m:
                await self._send({"type": "ack", "cursor": m["cursor"]})
            await self._on_message(m)

    # ── inbound ──────────────────────────────────────────────────────────────
    async def _on_message(self, m: dict) -> None:
        if not getattr(self, "_message_handler", None):
            return
        mid = str(m.get("id"))
        if self._dedup.is_duplicate(mid):
            return
        ch = m.get("channel") or {}
        frm = m.get("from") or {}
        text = (m.get("text") or "").strip()
        if not text:
            return
        is_dm = ch.get("kind") == "dm"
        if self.wake_on == "mention" and not m.get("mentioned") and not is_dm:
            return
        if _age_minutes(m.get("ts")) > BACKLOG_MINUTES:
            logger.info("Asar: skipping stale #%s (%d min old)", mid, _age_minutes(m.get("ts")))
            return
        source = self.build_source(chat_id=ch.get("id", ""), chat_name=ch.get("name", ""), chat_type="dm" if is_dm else "group",
                                   user_id=frm.get("id", ""), user_name=frm.get("name", ""))
        kw = {"text": text, "message_type": MessageType.TEXT, "source": source, "message_id": mid}
        with contextlib.suppress(Exception):
            if "timestamp" in inspect.signature(MessageEvent).parameters:
                kw["timestamp"] = datetime.datetime.now()
        await self.handle_message(MessageEvent(**kw))

    # ── outbound ─────────────────────────────────────────────────────────────
    async def send(self, chat_id: str, content: str, reply_to: Optional[str] = None,
                   metadata: Optional[Dict[str, Any]] = None, **kwargs):
        if not self._ws:
            return SendResult(success=False, error="Not connected")
        last_id = None
        for chunk in self._chunks(content, 7000):
            self._ref += 1
            ref = str(self._ref)
            fut: asyncio.Future = asyncio.get_running_loop().create_future()
            self._pending[ref] = fut
            try:
                await self._send({"type": "post", "ref": ref, "channel_id": chat_id, "text": chunk})
                r = await asyncio.wait_for(fut, timeout=15.0)
                last_id = r.get("id")
            except Exception as e:
                self._pending.pop(ref, None)
                return SendResult(success=False, error=str(e))
        return SendResult(success=True, message_id=str(last_id) if last_id is not None else None)

    async def send_typing(self, chat_id: str, metadata=None, **kwargs) -> None:
        with contextlib.suppress(Exception):
            await self._send({"type": "status", "state": "working"})

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        c = self._channels.get(chat_id) or {}
        return {"name": c.get("name", chat_id), "type": "dm" if c.get("kind") == "dm" else "group"}

    @staticmethod
    def _chunks(text: str, n: int):
        text = text or ""
        if len(text) <= n:
            yield text
            return
        while text:
            cut = text.rfind("\n", 0, n) if len(text) > n else len(text)
            if cut <= 0:
                cut = n
            yield text[:cut]
            text = text[cut:].lstrip("\n")

    # ── skills: Asar → ~/.hermes/skills/<id> ─────────────────────────────────
    async def _sync_skills(self, skills: list) -> None:
        try:
            await asyncio.get_running_loop().run_in_executor(None, self._sync_skills_blocking, skills)
        except Exception as e:
            logger.warning("Asar: skills sync failed: %s", e)

    def _sync_skills_blocking(self, skills: list) -> None:
        home = os.path.expanduser(os.environ.get("HERMES_HOME", "~/.hermes"))
        root = os.path.join(home, "skills")
        state_path = os.path.join(home, "asar-skills.json")
        try:
            with open(state_path) as f:
                state = json.load(f)
        except Exception:
            state = {}
        want = {s["id"]: s for s in skills if s.get("id")}
        for sid in list(state):
            if sid not in want:
                shutil.rmtree(os.path.join(root, sid), ignore_errors=True)
                state.pop(sid, None)
                logger.info("Asar: skill removed: %s", sid)
        for sid, s in want.items():
            if state.get(sid, {}).get("version") == s.get("version") and os.path.exists(os.path.join(root, sid, "SKILL.md")):
                continue
            req = urllib.request.Request(s["url"], headers={"Authorization": "Bearer " + self.key})
            with urllib.request.urlopen(req, timeout=60) as r:
                data = r.read()
                prefix = r.headers.get("X-Skill-Prefix", "") or ""
            tmp = tempfile.mkdtemp(prefix="asar-skill-")
            try:
                zpath = os.path.join(tmp, "s.zip")
                with open(zpath, "wb") as f:
                    f.write(data)
                with zipfile.ZipFile(zpath) as z:
                    for member in z.namelist():
                        if member.startswith("/") or ".." in member.split("/"):
                            raise ValueError("unsafe path in zip: " + member)
                    z.extractall(os.path.join(tmp, "x"))
                src = os.path.join(tmp, "x", prefix) if prefix else os.path.join(tmp, "x")
                dst = os.path.join(root, sid)
                shutil.rmtree(dst, ignore_errors=True)
                os.makedirs(root, exist_ok=True)
                shutil.copytree(src, dst)
            finally:
                shutil.rmtree(tmp, ignore_errors=True)
            state[sid] = {"version": s.get("version"), "at": datetime.datetime.now().isoformat()}
            with contextlib.suppress(Exception):
                rq = urllib.request.Request(f"{self.url}/agent/v1/skills/{sid}/installed", method="POST",
                                            data=json.dumps({"version": s.get("version")}).encode(),
                                            headers={"Content-Type": "application/json", "Authorization": "Bearer " + self.key})
                urllib.request.urlopen(rq, timeout=15).read()
            logger.info("Asar: skill installed: %s v%s → %s", sid, s.get("version"), dst)
        with open(state_path, "w") as f:
            json.dump(state, f, indent=2)


# ── registration ─────────────────────────────────────────────────────────────

def check_requirements() -> bool:
    return bool(_secret("ASAR_KEY", ""))


def validate_config(config) -> bool:
    extra = getattr(config, "extra", {}) or {}
    return bool(_env_or_extra(extra, "ASAR_KEY", "key", ""))


def is_connected(config) -> bool:
    return validate_config(config)


def _env_enablement() -> dict | None:
    key = (_secret("ASAR_KEY", "") or "").strip()
    if not key:
        return None
    out = {"key": key}
    for env, k in (("ASAR_URL", "url"), ("ASAR_WAKE_ON", "wake_on")):
        v = (_secret(env, "") or "").strip()
        if v:
            out[k] = v
    home = (_secret("ASAR_HOME_CHANNEL", "") or "").strip()
    if home:
        out["home_channel"] = {"chat_id": home, "name": "Home"}
    return out


async def _standalone_send(pconfig, chat_id: str, message: str, *, thread_id=None, media_files=None, force_document=False, **kwargs) -> Dict[str, Any]:
    """Cron delivery without the live gateway: plain HTTP POST with the agent key."""
    extra = getattr(pconfig, "extra", {}) or {}
    key = _env_or_extra(extra, "ASAR_KEY", "key", "")
    url = (_env_or_extra(extra, "ASAR_URL", "url", DEFAULT_URL) or DEFAULT_URL).rstrip("/")
    if not key or not chat_id:
        return {"success": False, "error": "Asar standalone send: ASAR_KEY and chat_id required"}
    try:
        rq = urllib.request.Request(f"{url}/api/channels/{chat_id}/messages", method="POST",
                                    data=json.dumps({"body": message}).encode(),
                                    headers={"Content-Type": "application/json", "Authorization": "Bearer " + key})
        body = json.loads(urllib.request.urlopen(rq, timeout=20).read() or b"{}")
        return {"success": True, "message_id": str(body.get("id", ""))}
    except Exception as e:
        return {"success": False, "error": f"Asar standalone send failed: {e}"}


def register(ctx):
    wanted = dict(
        name="asar",
        label="Asar",
        adapter_factory=AsarAdapter,
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        required_env=["ASAR_KEY"],
        install_hint="No extra packages needed (uses the bundled websockets)",
        env_enablement_fn=_env_enablement,
        cron_deliver_env_var="ASAR_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        allowed_users_env="ASAR_ALLOWED_USERS",
        allow_all_env="ASAR_ALLOW_ALL_USERS",
        max_message_length=7000,
        emoji="🏕",
        pii_safe=True,
        allow_update_command=True,
        platform_hint=(
            "You are a team member in Asar — a workspace where people and AI agents work together in channels. "
            "Markdown is supported. In a channel with several agents you are addressed by @name; answer only what "
            "concerns you, briefly, in the language the person writes in."),
    )
    # разные версии Hermes знают разный набор параметров — отдаём только известные
    try:
        params = inspect.signature(ctx.register_platform).parameters
        if not any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()):
            wanted = {k: v for k, v in wanted.items() if k in params}
    except (TypeError, ValueError):
        pass
    ctx.register_platform(**wanted)
