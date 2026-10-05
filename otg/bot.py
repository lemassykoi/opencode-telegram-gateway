#!/usr/bin/env python3
"""Ask — Telegram <-> opencode serve gateway.

Allowlisted aiogram bridge: one opencode session per chat, prompt_async +
/event SSE streamed into throttled Telegram message edits, permission asks
relayed as inline yes/no buttons.
"""

import asyncio
import json
import logging
import os
import re
import tempfile
import time
from collections import OrderedDict
from pathlib import Path

import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    BotCommand,
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)

BASE_DIR = Path(__file__).resolve().parent.parent
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("otg")

OPENCODE_URL = os.environ.get("OPENCODE_URL", "http://127.0.0.1:4097")
MODEL = {"providerID": "flashnext", "modelID": "qwen3.8-flash-next"}
AGENT = os.environ.get("OTG_AGENT", "ask")
INTERNAL_AGENTS = {"compaction", "summary", "title"}
STATIC_AGENTS: dict[str, dict | None] = {"ask": None, "build": None, "plan": None}
agents_cache: dict[str, dict | None] = {}  # name -> agent model dict or None (uses global model)
BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
ALLOWED = {int(x) for x in os.environ.get("ALLOWED_USER_IDS", "").replace(",", " ").split()}
STATE_FILE = BASE_DIR / "state.json"
MEMORY_DIR = BASE_DIR / "memory"
MEMORY_LIMIT = 32768
EDIT_INTERVAL = 2.0
WAIT_EMOJI = "⏳"  # lone-emoji message: Telegram shows it jumbo and animates it
TG_LIMIT = 4096
API_TIMEOUT = aiohttp.ClientTimeout(total=30)
VARIANTS = ("lean", "low", "medium", "xhigh")
VOICE_TOOLS = ("voicebox_generate", "voicebox_get_audio")  # opencode prefixes MCP tools with <server>_
AUDIO_TIMEOUT = aiohttp.ClientTimeout(total=120)
DENY_MSG = (
    "⛔ Sorry — Ask is a private gateway and you're not on its allowlist.\n"
    "Your Telegram user id: {id}\n"
    "Ask the machine owner to add it to ALLOWED_USER_IDS."
)
COPY = {
    "en": {
        "welcome": "Hi{name}! I'm Ask — your opencode gateway. Every message runs as "
        "an agent on this machine; any tool use asks you first.\n"
        "Commands: /stop /reset /session /variant /agent /id",
        "pick_lang": "🌐 Choose your language / Choisis ta langue :",
        "menu_label": "Commands:",
        "need_start": "👋 Send /start to begin.",
        "still_working": "⏳ still working — /stop to cancel",
        "variant_usage": "current variant: {cur}\nusage: /variant default|{opts}",
        "variant_set": "variant set: {val}",
        "variant_unknown": "unknown variant: {val}\nuse default|{opts}",
        "agent_usage": "current agent: {cur}\nusage: /agent {opts}",
        "agent_set": "agent set: {val}",
        "agent_unknown": "unknown agent: {val}\nuse: {opts}",
        "your_id": "your telegram user id: {id}",
        "session": "session: {sid}",
        "session_none": "session: (none yet — send a message first)",
        "reset_done": "🧹 new session {sid}",
        "nothing_running": "nothing running",
        "stopping": "🛑 stopping…",
        "abort_failed": "abort failed ({status})",
        "perm": "🔐 Ask needs approval — {perm}\n{detail}",
    },
    "fr": {
        "welcome": "Salut{name}! Je suis Ask — ta passerelle opencode. Chaque message "
        "lance un agent sur cette machine ; toute utilisation d'outil te demande "
        "d'abord.\nCommandes : /stop /reset /session /variant /agent /id",
        "pick_lang": "🌐 Choose your language / Choisis ta langue :",
        "menu_label": "Commandes :",
        "need_start": "👋 Envoie /start pour commencer.",
        "still_working": "⏳ traitement en cours — /stop pour annuler",
        "variant_usage": "variante actuelle : {cur}\nusage : /variant default|{opts}",
        "variant_set": "variante définie : {val}",
        "variant_unknown": "variante inconnue : {val}\nchoix : default|{opts}",
        "agent_usage": "agent actuel : {cur}\nusage : /agent {opts}",
        "agent_set": "agent défini : {val}",
        "agent_unknown": "agent inconnu : {val}\nchoix : {opts}",
        "your_id": "ton id utilisateur Telegram : {id}",
        "session": "session : {sid}",
        "session_none": "session : (aucune pour l'instant — envoie d'abord un message)",
        "reset_done": "🧹 nouvelle session {sid}",
        "nothing_running": "rien en cours",
        "stopping": "🛑 arrêt en cours…",
        "abort_failed": "échec de l'arrêt ({status})",
        "perm": "🔐 Ask a besoin d'approbation — {perm}\n{detail}",
    },
}
MENU = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="/stop"), KeyboardButton(text="/reset")],
        [KeyboardButton(text="/session"), KeyboardButton(text="/variant")],
        [KeyboardButton(text="/agent"), KeyboardButton(text="/id")],
    ],
    resize_keyboard=True,
    persistent=True,
)
COMMANDS = {
    "en": [
        BotCommand(command="start", description="Start / choose language"),
        BotCommand(command="stop", description="Abort current turn"),
        BotCommand(command="reset", description="New session"),
        BotCommand(command="session", description="Show opencode session id"),
        BotCommand(command="variant", description="Set reasoning effort"),
        BotCommand(command="agent", description="Switch agent"),
        BotCommand(command="id", description="Show your Telegram id"),
    ],
    "fr": [
        BotCommand(command="start", description="Démarrer / choisir la langue"),
        BotCommand(command="stop", description="Interrompre le tour en cours"),
        BotCommand(command="reset", description="Nouvelle session"),
        BotCommand(command="session", description="Afficher l'id de session opencode"),
        BotCommand(command="variant", description="Régler l'effort de raisonnement"),
        BotCommand(command="agent", description="Changer d'agent"),
        BotCommand(command="id", description="Afficher votre id Telegram"),
    ],
}

bot: Bot
http: aiohttp.ClientSession
dp = Dispatcher()
chats: dict[int, dict] = {}  # chat_id -> {"session_id", "started", "variant"}
renderers: dict[str, "Renderer"] = {}  # session_id -> active renderer
roles: dict[str, str] = {}  # opencode message_id -> role
perm_msgs: dict[str, tuple[int, int]] = {}  # permission_id -> (chat_id, tg_message_id)
unknown_notified: set[int] = set()
sent_audio: set[str] = set()  # voicebox generation_ids already delivered to Telegram
_bg_tasks: set[asyncio.Task] = set()


def _spawn(coro) -> asyncio.Task:
    task = asyncio.create_task(coro)
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)
    return task


def load_state() -> None:
    if not STATE_FILE.is_file():
        return
    for k, v in json.loads(STATE_FILE.read_text()).items():
        if isinstance(v, str):  # legacy flat chat_id -> session_id
            v = {"session_id": v}
        v.setdefault("session_id", None)
        v.setdefault("started", True)  # chats predating the /start gate stay open
        v.setdefault("variant", None)
        v.setdefault("lang", None)
        v.setdefault("agent", None)
        v.setdefault("user_id", None)
        chats[int(k)] = v


def save_state() -> None:
    STATE_FILE.write_text(json.dumps({str(k): v for k, v in chats.items()}))


def meta_of(chat_id: int) -> dict:
    return chats.setdefault(chat_id, {"session_id": None, "started": False, "variant": None})


def L(chat_id: int) -> dict:
    return COPY.get((chats.get(chat_id) or {}).get("lang") or "en", COPY["en"])


def ensure_memory(user_id: int) -> Path:
    MEMORY_DIR.mkdir(exist_ok=True)
    path = MEMORY_DIR / f"{user_id}.md"
    if not path.is_file():
        path.write_text(f"# Personal notepad — Telegram user {user_id}\n")
    return path


def memory_prefix(user_id: int) -> str:
    return (
        f"[otg] user_id={user_id} personal notepad: {ensure_memory(user_id)} (max 32KB). "
        "Read it when relevant; update it when the user asks you to remember something or "
        "reveals durable personal facts (personality, preferences, projects, people); "
        "compact it in place, preserving key facts, and stay under the cap."
    )


def memory_over_note(chat_id: int) -> str:
    uid = (chats.get(chat_id) or {}).get("user_id")
    if not uid:
        return ""
    try:
        size = (MEMORY_DIR / f"{uid}.md").stat().st_size
    except OSError:
        return ""
    if size > MEMORY_LIMIT:
        return f"⚠️ personal notepad is {size} bytes — over the 32KB cap; compact it now, preserving key facts."
    return ""


async def fetch_agents() -> dict[str, dict | None]:
    """Primary agents offered by /agent; name -> model dict (or None = global model)."""
    global agents_cache
    try:
        async with http.get(f"{OPENCODE_URL}/agent", timeout=API_TIMEOUT) as r:
            r.raise_for_status()
            data = await r.json()
        found = {
            a["name"]: a.get("model")
            for a in data
            if a.get("mode") == "primary"
            and a.get("name") not in INTERNAL_AGENTS
            and a.get("model") is None  # agents with their own model (e.g. Hacker -> llama.cpp)
            # are not offered: only the SGLang engine is loaded; prompt would fail
        }
        if found:
            agents_cache = found
    except Exception:
        log.warning("GET /agent failed; keeping previous list")
    return agents_cache or dict(STATIC_AGENTS)


def build_prompt_body(agent: str, text: str, variant: str | None) -> dict:
    body: dict = {"agent": agent, "parts": [{"type": "text", "text": text}]}
    if not agents_cache.get(agent):  # agent has no own model -> pin ours
        body["model"] = MODEL
        if variant:
            body["variant"] = variant
    return body


def split_tg(text: str) -> list[str]:
    out: list[str] = []
    while len(text) > TG_LIMIT:
        cut = text.rfind("\n", 0, TG_LIMIT)
        if cut < TG_LIMIT // 2:
            cut = TG_LIMIT
        out.append(text[:cut])
        text = text[cut:].lstrip("\n")
    if text:
        out.append(text)
    return out


def _esc(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def md_to_html(text: str) -> str:
    """Best-effort markdown -> Telegram HTML; total function, never raises."""
    store: list[str] = []

    def keep(html: str) -> str:
        store.append(html)
        return f"\x00{len(store) - 1}\x00"

    # fenced code blocks first; unterminated fences are closed implicitly
    text = re.sub(r"```[^\n]*\n(.*?)```", lambda m: keep("<pre>" + _esc(m.group(1)) + "</pre>"), text, flags=re.S)
    text = re.sub(r"```[^\n]*\n(.*)$", lambda m: keep("<pre>" + _esc(m.group(1)) + "</pre>"), text, flags=re.S)
    # inline code
    text = re.sub(r"`([^`\n]+)`", lambda m: keep("<code>" + _esc(m.group(1)) + "</code>"), text)
    # escape the remaining prose, then apply inline markup on the escaped text
    text = _esc(text)
    text = re.sub(r"\[([^\]]+)\]\((https?://[^\s)]+)\)", r'<a href="\2">\1</a>', text)
    text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text, flags=re.S)
    text = re.sub(r"__(.+?)__", r"<b>\1</b>", text, flags=re.S)
    text = re.sub(r"(?<![\w*])\*([^*\n]+)\*(?!\w)", r"<i>\1</i>", text)
    text = re.sub(r"~~(.+?)~~", r"<s>\1</s>", text, flags=re.S)
    text = re.sub(r"(?m)^#{1,6}\s+(.+)$", r"<b>\1</b>", text)
    return re.sub(r"\x00(\d+)\x00", lambda m: store[int(m.group(1))], text)


async def tg_send(chat_id: int, text: str, html: bool = True):
    if html:
        h = md_to_html(text)
        if len(h) <= TG_LIMIT:
            try:
                return await bot.send_message(chat_id, h, parse_mode="HTML")
            except TelegramBadRequest as exc:
                if "parse" not in str(exc).lower():
                    raise
    return await bot.send_message(chat_id, text[:TG_LIMIT])


async def tg_edit(chat_id: int, msg_id: int, text: str, html: bool = True) -> None:
    if html:
        h = md_to_html(text)
        if len(h) <= TG_LIMIT:
            try:
                await bot.edit_message_text(text=h, chat_id=chat_id, message_id=msg_id, parse_mode="HTML")
                return
            except TelegramBadRequest as exc:
                if "parse" not in str(exc).lower():
                    raise
    await bot.edit_message_text(text=text[:TG_LIMIT], chat_id=chat_id, message_id=msg_id, parse_mode=None)


async def send_voice_note(chat_id: int, url: str) -> None:
    """Fetch a voicebox WAV and deliver it as a Telegram voice note (OGG/Opus)."""
    async with http.get(url, timeout=AUDIO_TIMEOUT) as r:
        r.raise_for_status()
        mime = (r.headers.get("Content-Type") or "").split(";")[0].strip().lower()
        payload = await r.read()
    if not mime.startswith("audio/") and payload[:4] != b"RIFF":
        raise RuntimeError(f"unexpected audio payload from {url} (content-type {mime!r})")
    with tempfile.TemporaryDirectory() as td:
        src, dst = Path(td) / "audio.in", Path(td) / "voice.ogg"
        src.write_bytes(payload)
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-loglevel", "error", "-i", str(src),
            "-c:a", "libopus", "-b:a", "32k", "-ar", "48000", "-ac", "1",
            "-y", str(dst),
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE,
        )
        _, err = await proc.communicate()
        if proc.returncode == 0 and dst.is_file() and dst.stat().st_size > 0:
            await bot.send_voice(chat_id, FSInputFile(str(dst)))
        else:
            log.warning("ffmpeg voice convert failed: %.200s", err.decode(errors="replace"))
            await bot.send_document(chat_id, FSInputFile(str(src)))


async def drain_audio(renderer: "Renderer") -> None:
    while renderer.pending_audio:
        gid, url = renderer.pending_audio.pop(0)
        if gid in sent_audio:
            continue
        sent_audio.add(gid)
        try:
            await send_voice_note(renderer.chat_id, url)
        except Exception as exc:
            log.warning("voice relay failed (gen %s): %s", gid, exc)


class Renderer:
    """Accumulates one turn of assistant output and edits it into one TG message."""

    def __init__(self, chat_id: int, session_id: str) -> None:
        self.chat_id = chat_id
        self.session_id = session_id
        self.parts: OrderedDict[str, dict] = OrderedDict()  # part_id -> {"type", "text"}
        self.done_tools: set[str] = set()
        self.tool_lines: list[str] = []
        self.running_tools: dict[str, str] = {}  # call_id -> tool name
        self.pending_audio: list[tuple[str, str]] = []  # (generation_id, download_url)
        self.notes: list[str] = []
        self.msg_id: int | None = None
        self.last_edit = 0.0
        self.html_ok = True
        self.first_delta = 0.0
        self.m_msgs: dict[str, tuple[int, float]] = {}  # msg_id -> (out+reasoning tokens, cost)
        self.m_model = MODEL["modelID"]

    def reset_turn(self) -> None:
        self.parts.clear()
        self.done_tools.clear()
        self.tool_lines.clear()
        self.running_tools.clear()
        self.pending_audio.clear()
        self.notes.clear()
        self.first_delta = 0.0
        self.m_msgs.clear()

    async def show_placeholder(self) -> None:
        await self._put(WAIT_EMOJI)

    def on_part(self, part: dict) -> None:
        pid = part["id"]
        ptype = part.get("type", "")
        if ptype == "text":
            slot = self.parts.setdefault(pid, {"type": "text", "text": ""})
            slot["type"], slot["text"] = "text", part.get("text", "")
        elif ptype in ("reasoning",):
            self.parts.setdefault(pid, {"type": "reasoning", "text": ""})
        elif ptype == "tool":
            call = part.get("callID", pid)
            name = part.get("tool", "tool")
            status = (part.get("state") or {}).get("status", "")
            if status in ("pending", "running"):
                self.running_tools[call] = name
            elif call not in self.done_tools:
                self.running_tools.pop(call, None)
                self.done_tools.add(call)
                self.tool_lines.append(f"🔧 {name}" + (" ⚠️" if status == "error" else ""))
                if name.endswith(VOICE_TOOLS):
                    self._queue_audio(part)

    def _queue_audio(self, part: dict) -> None:
        try:
            data = json.loads((part.get("state") or {}).get("output") or "")
        except (json.JSONDecodeError, TypeError):
            return
        if not isinstance(data, dict):
            return
        gid, url = data.get("generation_id"), data.get("download_url")
        if gid and url and gid not in sent_audio:
            self.pending_audio.append((gid, url))

    def on_delta(self, props: dict) -> None:
        slot = self.parts.get(props["partID"])
        if slot is not None and slot["type"] == "text" and props.get("field") == "text":
            if not props.get("delta"):
                return
            if not self.first_delta:
                self.first_delta = time.monotonic()
            slot["text"] += props.get("delta", "")

    def on_metrics(self, msg_id: str, tokens: dict, cost: float, model: str) -> None:
        self.m_msgs[msg_id] = (
            int(tokens.get("output", 0) or 0) + int(tokens.get("reasoning", 0) or 0),
            float(cost or 0.0),
        )
        if model:
            self.m_model = model

    def turn_seconds(self) -> float:
        return time.monotonic() - self.first_delta if self.first_delta else 0.0

    def metrics_line(self) -> str:
        out = sum(v[0] for v in self.m_msgs.values())
        cost = sum(v[1] for v in self.m_msgs.values())
        secs = self.turn_seconds()
        bits = [f"📊 {self.m_model}"]
        if secs > 0:
            bits.append(f"⏱ {secs:.1f}s")
        if out:
            bits.append(f"{out} tok out" + (f" ({out / secs:.1f} tok/s)" if secs > 0 else ""))
        if cost > 0:
            bits.append(f"${cost:.4f}")
        return " · ".join(bits) if len(bits) > 1 else ""

    def build_text(self) -> str:
        head = list(self.tool_lines)
        head += [f"⚙️ {n}" for n in self.running_tools.values()]
        body = "\n\n".join(
            p["text"].strip() for p in self.parts.values() if p["type"] == "text" and p["text"].strip()
        )
        chunks = [s for s in (*head, body) if s]
        chunks += self.notes
        return "\n\n".join(chunks)

    async def _put(self, text: str) -> None:
        try:
            if self.msg_id is None:
                sent = await tg_send(self.chat_id, text, self.html_ok)
                self.msg_id = sent.message_id
            else:
                await tg_edit(self.chat_id, self.msg_id, text, self.html_ok)
        except TelegramRetryAfter as exc:
            await asyncio.sleep(exc.retry_after)
        except TelegramBadRequest as exc:
            s = str(exc).lower()
            if "parse" in s:
                self.html_ok = False
            elif "not modified" not in s:
                log.debug("render: %s", exc)

    async def render(self) -> None:
        text = self.build_text()
        if not text:
            return
        if time.monotonic() - self.last_edit < EDIT_INTERVAL:
            return
        self.last_edit = time.monotonic()
        await self._put(text[:TG_LIMIT])

    async def finalize(self, note: str = "") -> None:
        if note:
            self.notes.append(note)
        chunks = split_tg(self.build_text()) or ["(empty)"]
        await self._put(chunks[0])
        for extra in chunks[1:]:
            try:
                await tg_send(self.chat_id, extra, self.html_ok)
            except Exception as exc:
                log.warning("finalize split: %s", exc)
        line = self.metrics_line()
        if line:
            try:
                await bot.send_message(self.chat_id, line)
            except Exception as exc:
                log.warning("metrics send: %s", exc)


def _error_note(err: dict) -> str:
    data = err.get("data") if isinstance(err.get("data"), dict) else {}
    return f"⚠️ {err.get('name', 'error')}: {data.get('message', '')}".strip()


def session_title(user) -> str:  # noqa: ANN001
    if user is None:
        return "Telegram Session"
    handle = f" (@{user.username})" if user.username else ""
    return f"Telegram Session from {user.first_name or 'unnamed'}{handle} {time.strftime('%Y-%m-%d')}"


async def ensure_session(chat_id: int, user=None) -> str:  # noqa: ANN001
    meta = meta_of(chat_id)
    sid = meta.get("session_id")
    if sid:
        async with http.get(f"{OPENCODE_URL}/session/{sid}", timeout=API_TIMEOUT) as r:
            if r.status == 200:
                return sid
    async with http.post(
        f"{OPENCODE_URL}/session", json={"title": session_title(user)}, timeout=API_TIMEOUT
    ) as r:
        r.raise_for_status()
        sid = (await r.json())["id"]
    meta["session_id"] = sid
    save_state()
    return sid


async def handle_event(ev: dict) -> None:
    etype, props = ev.get("type", ""), ev.get("properties", {})
    sid = props.get("sessionID")
    if not sid:
        return
    chat_id = next((c for c, v in chats.items() if v.get("session_id") == sid), None)
    if chat_id is None:
        return
    renderer = renderers.get(sid)

    if etype == "message.updated":
        info = props.get("info", {})
        roles[info.get("id", "")] = info.get("role", "")
        if info.get("error") and renderer:
            renderer.notes.append(_error_note(info["error"]))
        if renderer and info.get("role") == "assistant" and (info.get("time") or {}).get("completed"):
            renderer.on_metrics(
                info.get("id", ""), info.get("tokens") or {}, info.get("cost") or 0, info.get("modelID") or ""
            )
    elif etype == "message.part.updated":
        part = props.get("part", {})
        if renderer and roles.get(part.get("messageID", "")) == "assistant":
            renderer.on_part(part)
            if renderer.pending_audio:
                _spawn(drain_audio(renderer))
            await renderer.render()
    elif etype == "message.part.delta":
        if renderer and roles.get(props.get("messageID", "")) == "assistant":
            renderer.on_delta(props)
            await renderer.render()
    elif etype == "permission.asked":
        await ask_permission(chat_id, props)
    elif etype == "permission.replied":
        resolve = perm_msgs.pop(props.get("requestID") or props.get("id", ""), None)
        if resolve:
            c, m = resolve
            try:
                await bot.edit_message_text(
                    text="✔️ permission resolved", chat_id=c, message_id=m, reply_markup=None
                )
            except TelegramBadRequest:
                pass
    elif etype == "session.error":
        if renderer:
            renderers.pop(sid, None)
            await renderer.finalize(_error_note(props.get("error") or {}))
    elif etype == "session.idle":
        if renderer:
            renderers.pop(sid, None)
            await renderer.finalize(memory_over_note(chat_id))


async def ask_permission(chat_id: int, props: dict) -> None:
    pid = props["id"]
    meta = props.get("metadata") or {}
    detail = meta.get("command") or " ".join(props.get("patterns") or []) or meta.get("description") or ""
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(text="✅ Yes", callback_data=f"perm|{pid}|once"),
            InlineKeyboardButton(text="❌ No", callback_data=f"perm|{pid}|reject"),
        ]]
    )
    msg = await bot.send_message(
        chat_id, L(chat_id)["perm"].format(perm=props.get("permission", "?"), detail=detail)[:TG_LIMIT],
        reply_markup=kb,
    )
    perm_msgs[pid] = (chat_id, msg.message_id)


async def resync() -> None:
    """After SSE reconnect: rebuild active renderers from history; finalize finished ones."""
    if not renderers:
        return
    try:
        async with http.get(f"{OPENCODE_URL}/session/status", timeout=API_TIMEOUT) as r:
            statuses = await r.json() if r.status == 200 else {}
    except Exception:
        statuses = {}
    for sid, renderer in list(renderers.items()):
        try:
            async with http.get(f"{OPENCODE_URL}/session/{sid}/message", timeout=API_TIMEOUT) as r:
                if r.status != 200:
                    continue
                items = await r.json()
        except Exception:
            continue
        start = max(
            (i for i, it in enumerate(items) if it.get("info", {}).get("role") == "user"),
            default=-1,
        )
        renderer.reset_turn()
        for item in items[start:]:
            info = item.get("info", {})
            roles[info.get("id", "")] = info.get("role", "")
            if info.get("role") == "assistant":
                for part in item.get("parts", []):
                    renderer.on_part(part)
        if renderer.pending_audio:
            _spawn(drain_audio(renderer))
        if statuses.get(sid, {}).get("type", "idle") == "idle":
            renderers.pop(sid, None)
            await renderer.finalize()


async def sse_loop() -> None:
    while True:
        try:
            async with http.get(f"{OPENCODE_URL}/event") as r:
                r.raise_for_status()
                await resync()
                async for raw in r.content:
                    line = raw.decode(errors="replace").strip()
                    if not line.startswith("data:"):
                        continue
                    try:
                        await handle_event(json.loads(line[5:]))
                    except Exception:
                        log.exception("event handling failed: %.200s", line)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("SSE dropped, reconnecting in 2s")
            await asyncio.sleep(2)


async def gate(handler, event, *args, **kwargs):  # noqa: ANN001
    user = getattr(event, "from_user", None)
    if user is not None and user.id is not None and user.id not in ALLOWED:
        if user.id not in unknown_notified:
            unknown_notified.add(user.id)
            log.info("unauthorized sender id: %s", user.id)
            if isinstance(event, Message):
                await event.answer(DENY_MSG.format(id=user.id))
            elif isinstance(event, CallbackQuery):
                await event.answer("not allowed", show_alert=True)
        return
    if isinstance(event, Message) and not (event.text or "").startswith("/start"):
        if not chats.get(event.chat.id, {}).get("started"):
            await event.answer(L(event.chat.id)["need_start"])
            return
    return await handler(event, *args, **kwargs)


dp.message.outer_middleware.register(gate)
dp.callback_query.outer_middleware.register(gate)


@dp.message(CommandStart())
async def cmd_start(message: Message) -> None:
    meta = meta_of(message.chat.id)
    meta["started"] = True
    arg = (message.text or "").split()[1:]
    if arg:
        v = arg[0].lower()
        if v.startswith("fr"):
            meta["lang"] = "fr"
        elif v.startswith("en"):
            meta["lang"] = "en"
    save_state()
    name = f" {message.from_user.first_name}" if message.from_user and message.from_user.first_name else ""
    lang = meta.get("lang")
    c = COPY.get(lang or "en", COPY["en"])
    if lang:
        await message.answer(c["welcome"].format(name=name), reply_markup=MENU)
        return
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(text="🇫🇷 Français", callback_data="lang|fr"),
            InlineKeyboardButton(text="🇬🇧 English", callback_data="lang|en"),
        ]]
    )
    await message.answer(c["welcome"].format(name=name) + "\n\n" + c["pick_lang"], reply_markup=kb)


@dp.callback_query(F.data.startswith("lang|"))
async def on_lang_cb(cb: CallbackQuery) -> None:
    lang = cb.data.split("|", 1)[1]
    if cb.message is None or cb.message.chat is None or lang not in COPY:
        await cb.answer("bad request", show_alert=True)
        return
    meta_of(cb.message.chat.id)["lang"] = lang
    save_state()
    name = f" {cb.from_user.first_name}" if cb.from_user.first_name else ""
    await cb.message.edit_text(COPY[lang]["welcome"].format(name=name), reply_markup=None)
    await bot.send_message(cb.message.chat.id, COPY[lang]["menu_label"], reply_markup=MENU)
    await cb.answer("ok")


@dp.message(Command("id"))
async def cmd_id(message: Message) -> None:
    await message.answer(L(message.chat.id)["your_id"].format(id=message.from_user.id))


@dp.message(Command("session"))
async def cmd_session(message: Message) -> None:
    c = L(message.chat.id)
    sid = chats.get(message.chat.id, {}).get("session_id")
    await message.answer(c["session"].format(sid=sid) if sid else c["session_none"])


@dp.message(Command("variant"))
async def cmd_variant(message: Message) -> None:
    meta, c = meta_of(message.chat.id), L(message.chat.id)
    opts = "|".join(VARIANTS)
    args = (message.text or "").split()
    if len(args) < 2:
        await message.answer(c["variant_usage"].format(cur=meta.get("variant") or "default", opts=opts))
        return
    val = args[1].lower()
    if val in ("default", "none", "off"):
        meta["variant"] = None
    elif val in VARIANTS:
        meta["variant"] = val
    else:
        await message.answer(c["variant_unknown"].format(val=val, opts=opts))
        return
    save_state()
    await message.answer(c["variant_set"].format(val=meta["variant"] or "default"))


@dp.message(Command("agent"))
async def cmd_agent(message: Message) -> None:
    meta, c = meta_of(message.chat.id), L(message.chat.id)
    agents = await fetch_agents()
    args = (message.text or "").split(maxsplit=1)
    cur = meta.get("agent") or AGENT
    if len(args) < 2:
        await message.answer(c["agent_usage"].format(cur=cur, opts=" | ".join(agents)))
        return
    want = args[1].strip()
    match = next((n for n in agents if n.lower() == want.lower()), None)
    if match is None:
        await message.answer(c["agent_unknown"].format(val=want, opts=" | ".join(agents)))
        return
    meta["agent"] = match
    save_state()
    await message.answer(c["agent_set"].format(val=match))


@dp.message(Command("reset"))
async def cmd_reset(message: Message) -> None:
    sid = meta_of(message.chat.id).pop("session_id", None)
    if sid:
        renderers.pop(sid, None)
        try:
            async with http.delete(f"{OPENCODE_URL}/session/{sid}", timeout=API_TIMEOUT) as r:
                pass
        except Exception:
            log.warning("reset: delete failed for %s", sid)
    save_state()
    new_sid = await ensure_session(message.chat.id, message.from_user)
    await message.answer(L(message.chat.id)["reset_done"].format(sid=new_sid))


@dp.message(Command("stop"))
async def cmd_stop(message: Message) -> None:
    sid = chats.get(message.chat.id, {}).get("session_id")
    c = L(message.chat.id)
    if not sid or sid not in renderers:
        await message.answer(c["nothing_running"])
        return
    async with http.post(f"{OPENCODE_URL}/session/{sid}/abort", json={}, timeout=API_TIMEOUT) as r:
        await message.answer(
            c["stopping"] if r.status in (200, 204) else c["abort_failed"].format(status=r.status)
        )


@dp.callback_query(F.data.startswith("perm|"))
async def on_perm_cb(cb: CallbackQuery) -> None:
    _, pid, decision = cb.data.split("|")
    entry = perm_msgs.pop(pid, None)
    if not entry or cb.message is None or cb.message.chat.id != entry[0]:
        await cb.answer("already answered", show_alert=True)
        return
    sid = chats.get(entry[0], {}).get("session_id", "")
    ok = False
    try:
        async with http.post(
            f"{OPENCODE_URL}/session/{sid}/permissions/{pid}", json={"response": decision}, timeout=API_TIMEOUT
        ) as r:
            ok = r.status in (200, 204)
    except Exception:
        log.exception("permission reply POST failed")
    label = "✅ approved" if decision == "once" else "❌ rejected"
    try:
        await cb.message.edit_text(f"{label} — {pid}", reply_markup=None)
    except TelegramBadRequest:
        pass
    await cb.answer("ok" if ok else "failed to reach opencode", show_alert=not ok)


@dp.message(F.text & ~F.text.startswith("/"))
async def on_text(message: Message) -> None:
    chat_id = message.chat.id
    sid = await ensure_session(chat_id, message.from_user)
    if sid in renderers:
        await message.reply(L(chat_id)["still_working"])
        return
    renderer = Renderer(chat_id, sid)
    renderers[sid] = renderer
    await renderer.show_placeholder()
    if not agents_cache:
        await fetch_agents()
    meta = meta_of(chat_id)
    uid = message.from_user.id if message.from_user else chat_id
    if meta.get("user_id") != uid:
        meta["user_id"] = uid
        save_state()
    text = f"{memory_prefix(uid)}\n\n{message.text}"
    body = build_prompt_body(meta.get("agent") or AGENT, text, meta.get("variant"))
    try:
        async with http.post(
            f"{OPENCODE_URL}/session/{sid}/prompt_async",
            json=body,
            timeout=API_TIMEOUT,
        ) as r:
            if r.status not in (200, 204):
                raise RuntimeError(f"prompt_async {r.status}: {(await r.text())[:200]}")
    except Exception as exc:
        renderers.pop(sid, None)
        if renderer.msg_id:
            try:
                await bot.delete_message(chat_id, renderer.msg_id)
            except Exception:
                pass
        await message.answer(f"⚠️ {exc}")


async def main() -> None:
    global bot, http
    load_state()
    http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None, sock_connect=15))
    bot = Bot(token=BOT_TOKEN)
    await bot.delete_webhook(drop_pending_updates=True)
    try:
        await bot.set_my_commands(COMMANDS["en"])
        await bot.set_my_commands(COMMANDS["fr"], language_code="fr")
    except Exception:
        log.warning("could not update Telegram command menu")
    await fetch_agents()
    sse = asyncio.create_task(sse_loop())
    log.info("Ask starting: model=%s allowed=%d chats=%d", MODEL["modelID"], len(ALLOWED), len(chats))
    try:
        await dp.start_polling(bot)
    finally:
        sse.cancel()
        await http.close()


if __name__ == "__main__":
    asyncio.run(main())
