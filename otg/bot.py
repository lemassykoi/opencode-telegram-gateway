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
import time
from collections import OrderedDict
from pathlib import Path

import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter
from aiogram.filters import Command, CommandStart
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

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

bot: Bot
http: aiohttp.ClientSession
dp = Dispatcher()
sessions: dict[int, str] = {}  # chat_id -> session_id
renderers: dict[str, "Renderer"] = {}  # session_id -> active renderer
roles: dict[str, str] = {}  # opencode message_id -> role
perm_msgs: dict[str, tuple[int, int]] = {}  # permission_id -> (chat_id, tg_message_id)
unknown_notified: set[int] = set()


def load_state() -> None:
    if STATE_FILE.is_file():
        sessions.update({int(k): v for k, v in json.loads(STATE_FILE.read_text()).items()})


def save_state() -> None:
    STATE_FILE.write_text(json.dumps({str(k): v for k, v in sessions.items()}))


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

    def reset_turn(self) -> None:
        self.parts.clear()
        self.done_tools.clear()
        self.tool_lines.clear()
        self.running_tools.clear()
        self.notes.clear()

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
            slot["text"] += props.get("delta", "")

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
                sent = await bot.send_message(self.chat_id, text)
                self.msg_id = sent.message_id
            else:
                await bot.edit_message_text(text=text, chat_id=self.chat_id, message_id=self.msg_id)
        except TelegramRetryAfter as exc:
            await asyncio.sleep(exc.retry_after)
        except TelegramBadRequest as exc:
            if "not modified" not in str(exc).lower():
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
                await bot.send_message(self.chat_id, extra)
            except Exception as exc:
                log.warning("finalize split: %s", exc)


def _error_note(err: dict) -> str:
    data = err.get("data") if isinstance(err.get("data"), dict) else {}
    return f"⚠️ {err.get('name', 'error')}: {data.get('message', '')}".strip()


async def ensure_session(chat_id: int) -> str:
    sid = sessions.get(chat_id)
    if sid:
        async with http.get(f"{OPENCODE_URL}/session/{sid}", timeout=API_TIMEOUT) as r:
            if r.status == 200:
                return sid
    async with http.post(f"{OPENCODE_URL}/session", json={"title": f"tg:{chat_id}"}, timeout=API_TIMEOUT) as r:
        r.raise_for_status()
        sid = (await r.json())["id"]
    sessions[chat_id] = sid
    save_state()
    return sid


async def handle_event(ev: dict) -> None:
    etype, props = ev.get("type", ""), ev.get("properties", {})
    sid = props.get("sessionID")
    if not sid:
        return
    chat_id = next((c for c, s in sessions.items() if s == sid), None)
    if chat_id is None:
        return
    renderer = renderers.get(sid)

    if etype == "message.updated":
        info = props.get("info", {})
        roles[info.get("id", "")] = info.get("role", "")
        if info.get("error") and renderer:
            renderer.notes.append(_error_note(info["error"]))
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
                await event.answer(f"⛔ you are not on the allowlist. your id: {user.id}")
            elif isinstance(event, CallbackQuery):
                await event.answer("not allowed", show_alert=True)
        return
    return await handler(event, *args, **kwargs)


dp.message.outer_middleware.register(gate)
dp.callback_query.outer_middleware.register(gate)


@dp.message(CommandStart())
async def cmd_start(message: Message) -> None:
    await message.answer(
        "I'm Ask — your opencode gateway. Each message runs as an agent on this "
        "machine; any tool use asks you first.\nCommands: /stop /reset /id"
    )


@dp.message(Command("id"))
async def cmd_id(message: Message) -> None:
    await message.answer(f"session: {sessions.get(message.chat.id, '(none yet)')}")


@dp.message(Command("reset"))
async def cmd_reset(message: Message) -> None:
    sid = sessions.pop(message.chat.id, None)
    if sid:
        renderers.pop(sid, None)
        try:
            async with http.delete(f"{OPENCODE_URL}/session/{sid}", timeout=API_TIMEOUT) as r:
                pass
        except Exception:
            log.warning("reset: delete failed for %s", sid)
    save_state()
    new_sid = await ensure_session(message.chat.id)
    await message.answer(f"🧹 new session {new_sid}")


@dp.message(Command("stop"))
async def cmd_stop(message: Message) -> None:
    sid = sessions.get(message.chat.id)
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
    sid = sessions.get(entry[0], "")
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
    sid = await ensure_session(chat_id)
    if sid in renderers:
        await message.reply("⏳ still working — /stop to cancel")
        return
    renderer = Renderer(chat_id, sid)
    renderers[sid] = renderer
    try:
        async with http.post(
            f"{OPENCODE_URL}/session/{sid}/prompt_async",
            json={"model": MODEL, "parts": [{"type": "text", "text": message.text}]},
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
    asyncio.create_task(sse_loop())
    log.info("Ask starting: model=%s allowed=%d chats=%d", MODEL["modelID"], len(ALLOWED), len(sessions))
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
