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
import time
from collections import OrderedDict
from pathlib import Path

import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    CallbackQuery,
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
BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
ALLOWED = {int(x) for x in os.environ.get("ALLOWED_USER_IDS", "").replace(",", " ").split()}
STATE_FILE = BASE_DIR / "state.json"
EDIT_INTERVAL = 2.0
TG_LIMIT = 4096
API_TIMEOUT = aiohttp.ClientTimeout(total=30)
VARIANTS = ("lean", "low", "medium", "xhigh")
DENY_MSG = (
    "⛔ Sorry — Ask is a private gateway and you're not on its allowlist.\n"
    "Your Telegram user id: {id}\n"
    "Ask the machine owner to add it to ALLOWED_USER_IDS."
)

bot: Bot
http: aiohttp.ClientSession
dp = Dispatcher()
chats: dict[int, dict] = {}  # chat_id -> {"session_id", "started", "variant"}
renderers: dict[str, "Renderer"] = {}  # session_id -> active renderer
roles: dict[str, str] = {}  # opencode message_id -> role
perm_msgs: dict[str, tuple[int, int]] = {}  # permission_id -> (chat_id, tg_message_id)
unknown_notified: set[int] = set()


def load_state() -> None:
    if not STATE_FILE.is_file():
        return
    for k, v in json.loads(STATE_FILE.read_text()).items():
        if isinstance(v, str):  # legacy flat chat_id -> session_id
            v = {"session_id": v}
        v.setdefault("session_id", None)
        v.setdefault("started", True)  # chats predating the /start gate stay open
        v.setdefault("variant", None)
        chats[int(k)] = v


def save_state() -> None:
    STATE_FILE.write_text(json.dumps({str(k): v for k, v in chats.items()}))


def meta_of(chat_id: int) -> dict:
    return chats.setdefault(chat_id, {"session_id": None, "started": False, "variant": None})


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


class Renderer:
    """Accumulates one turn of assistant output and edits it into one TG message."""

    def __init__(self, chat_id: int, session_id: str) -> None:
        self.chat_id = chat_id
        self.session_id = session_id
        self.parts: OrderedDict[str, dict] = OrderedDict()  # part_id -> {"type", "text"}
        self.done_tools: set[str] = set()
        self.tool_lines: list[str] = []
        self.running_tools: dict[str, str] = {}  # call_id -> tool name
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
        self.notes.clear()
        self.first_delta = 0.0
        self.m_msgs.clear()

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
        if time.monotonic() - self.last_edit < EDIT_INTERVAL:
            return
        self.last_edit = time.monotonic()
        await self._put(self.build_text()[:TG_LIMIT] or "…")

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
            await renderer.finalize()


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
        chat_id, f"🔐 Ask needs approval — {props.get('permission', '?')}\n{detail}"[:TG_LIMIT], reply_markup=kb
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
            await event.answer("👋 Send /start to begin.")
            return
    return await handler(event, *args, **kwargs)


dp.message.outer_middleware.register(gate)
dp.callback_query.outer_middleware.register(gate)


@dp.message(CommandStart())
async def cmd_start(message: Message) -> None:
    meta = meta_of(message.chat.id)
    meta["started"] = True
    save_state()
    name = message.from_user.first_name if message.from_user else "there"
    await message.answer(
        f"Hi {name}! I'm Ask — your opencode gateway. Every message runs as an agent "
        "on this machine; any tool use asks you first.\n"
        "Commands: /stop /reset /session /variant /id"
    )


@dp.message(Command("id"))
async def cmd_id(message: Message) -> None:
    await message.answer(f"your telegram user id: {message.from_user.id}")


@dp.message(Command("session"))
async def cmd_session(message: Message) -> None:
    sid = chats.get(message.chat.id, {}).get("session_id")
    await message.answer(f"session: {sid or '(none yet — send a message first)'}")


@dp.message(Command("variant"))
async def cmd_variant(message: Message) -> None:
    meta = meta_of(message.chat.id)
    args = (message.text or "").split()
    if len(args) < 2:
        await message.answer(
            f"current variant: {meta.get('variant') or 'default'}\n"
            f"usage: /variant default|{'|'.join(VARIANTS)}"
        )
        return
    val = args[1].lower()
    if val in ("default", "none", "off"):
        meta["variant"] = None
    elif val in VARIANTS:
        meta["variant"] = val
    else:
        await message.answer(f"unknown variant: {val}\nuse default|{'|'.join(VARIANTS)}")
        return
    save_state()
    await message.answer(f"variant set: {meta['variant'] or 'default'}")


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
    await message.answer(f"🧹 new session {new_sid}")


@dp.message(Command("stop"))
async def cmd_stop(message: Message) -> None:
    sid = chats.get(message.chat.id, {}).get("session_id")
    if not sid or sid not in renderers:
        await message.answer("nothing running")
        return
    async with http.post(f"{OPENCODE_URL}/session/{sid}/abort", json={}, timeout=API_TIMEOUT) as r:
        await message.answer("🛑 stopping…" if r.status in (200, 204) else f"abort failed ({r.status})")


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
        await message.reply("⏳ still working — /stop to cancel")
        return
    renderer = Renderer(chat_id, sid)
    renderers[sid] = renderer
    body: dict = {"model": MODEL, "parts": [{"type": "text", "text": message.text}]}
    variant = chats.get(chat_id, {}).get("variant")
    if variant:
        body["variant"] = variant
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
        await message.answer(f"⚠️ {exc}")


async def main() -> None:
    global bot, http
    load_state()
    http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None, sock_connect=15))
    bot = Bot(token=BOT_TOKEN)
    await bot.delete_webhook(drop_pending_updates=True)
    sse = asyncio.create_task(sse_loop())
    log.info("Ask starting: model=%s allowed=%d chats=%d", MODEL["modelID"], len(ALLOWED), len(chats))
    try:
        await dp.start_polling(bot)
    finally:
        sse.cancel()
        await http.close()


if __name__ == "__main__":
    asyncio.run(main())
