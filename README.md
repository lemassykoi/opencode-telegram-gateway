# Opencode Telegram Gateway (OTG)

A Telegram bot that turns [opencode](https://opencode.ai) into your
personal assistant: message the bot, and your words run as a real agent
session on this machine — full tools (bash, file edit, MCP servers),
machine context via `AGENTS.md`, and real-world awareness (dates,
services, system state). OTG itself is a thin, allowlisted bridge: the
aiogram layer plus the Telegram user-ID allowlist. opencode is the brain.

## Why

A hand-rolled "LLM + tools" Telegram bot re-implements badly what
opencode already provides: tool loops, permissions, session persistence,
context files. OTG keeps only what's worth owning — the Telegram side —
and delegates everything else to `opencode serve`'s HTTP API.

## What it does

- **One opencode session per Telegram chat** — conversation state
  persists across messages; `/reset` starts fresh.
- **Live streaming** — the reply is edited into a single Telegram
  message as it streams (throttled to Telegram's flood limits), with a
  jumbo ⏳ placeholder while waiting and tool activity shown as a status
  header (`🔧 tool`, `⚙️ running…`).
- **Permission relay** — whenever the agent wants to run a tool, you get
  inline **✅ Yes / ❌ No** buttons in Telegram. Nothing is ever
  auto-approved.
- **Per-user memory** — a 32 KB markdown notepad per Telegram user
  (`memory/<user_id>.md`). Each prompt carries a one-line pointer to it;
  the agent reads and updates it with its own tools when you ask it to
  remember something or reveal durable personal facts, compacting it in
  place near the cap.
- **Voice notes** — if the agent calls the voicebox TTS MCP tool, the
  audio is converted (ffmpeg → OGG/Opus) and delivered as a real
  Telegram voice message.
- **Mid-turn restarts** — if the bot restarts while a turn is running,
  it re-adopts the busy session and keeps streaming.
- **Follow-up queue** — send a message while the bot is working and it
  is queued (one slot) and runs as soon as the current turn finishes.
- **UX niceties** — EN/FR interface, HTML formatting (code blocks,
  bold, links) with plain-text fallback, per-turn metrics line (model,
  wall time, tokens/s, cost), `/stop` `/reset` `/session` `/variant`
  `/agent` `/id` commands.

## Architecture

```
Telegram <-> aiogram bot (allowlist, streaming edits, permission relay)
              |  HTTP 127.0.0.1 only
              v
        opencode serve --port 4097 --hostname 127.0.0.1
              v
        opencode sessions in the user's home dir (AGENTS.md loaded)
```

- User message → `POST /session/:id/prompt_async`
- Stream → `GET /event` SSE: `message.part.delta` text deltas are
  accumulated per part and edited into one message; `session.idle`
  finalizes the turn.
- Permission asks (`permission.asked`) → inline buttons →
  `POST /session/:id/permissions/:permissionID` (`once`/`reject` only).
- `/stop` → `POST /session/:id/abort`; `/reset` → delete + recreate.

## Requirements

- **opencode 1.x** (`opencode serve`). The v2 server API is an
  intentional breaking change — see
  [`docs/opencode-v2.md`](docs/opencode-v2.md) for the full compat
  report. A v2-based rewrite lives in a separate project
  (`opencode-v2-telegram-gateway`).
- Python 3.12+ with `aiogram` and `aiohttp` (`pip install -r
  requirements.txt`).
- `ffmpeg` on PATH (voice-note conversion only).
- A model/provider configured in `~/.config/opencode/opencode.json`
  (this deployment uses a local SGLang endpoint).

## Setup

```bash
python3 -m venv venv && venv/bin/pip install -r requirements.txt
cp bot.env.example bot.env        # TELEGRAM_BOT_TOKEN, ALLOWED_USER_IDS
venv/bin/python otg/bot.py        # foreground test run
```

For production, install the systemd user units (source of truth in
`systemd/`, symlinked into `~/.config/systemd/user/`):

```bash
ln -s "$PWD/systemd/"*.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now opencode-serve otg
```

## Configuration

- `bot.env` (gitignored): `TELEGRAM_BOT_TOKEN`, `ALLOWED_USER_IDS`
  (space/comma-separated Telegram user ids).
- `state.json` (gitignored): chat_id → `{session_id, started, variant,
  lang, agent, user_id}`. Delete to re-map chats; stale sessions are
  auto-recreated on the next message.
- `memory/` (gitignored): one `<user_id>.md` notepad per user.
- `OPENCODE_URL` (default `http://127.0.0.1:4097`), `OTG_AGENT`
  (default `ask`).

## Operations

```bash
systemctl --user {status|restart|stop} otg opencode-serve
journalctl --user -u otg -n 50 --no-pager
```

Only one getUpdates consumer may run per bot token — never run
`bot.py` manually while `otg.service` is active (TelegramConflictError).

## Security

- The Telegram user-ID allowlist is the **only** security boundary:
  allowlisted users effectively have shell on this machine. The
  permission relay (approve every tool ask) is the mitigating control.
- `opencode serve` is unauthenticated on loopback: **never** bind beyond
  127.0.0.1; set `OPENCODE_SERVER_PASSWORD` if anything else must reach
  it.
- Keep secrets out of chat history (`state.json`, opencode storage
  under `~/.local/share/opencode`, `memory/`).
