# Opencode Telegram Gateway (OTG)

Telegram bot that proxies chats to **opencode sessions** running on this
ThinkStation PGX. The bot is a thin, allowlisted bridge; opencode is the
agent brain — real tools (bash, edit, read), MCP servers, AGENTS.md machine
context, and thus real-world awareness (dates, system state, services).

## Why

A hand-rolled "LLM + MCP tools" Telegram bot (the `~/qwen-tgbot` prototype)
re-implements badly what opencode already provides: tool loops, permissions,
session persistence, context files. OTG replaces the prototype's brain with
`opencode serve`'s HTTP API and keeps only the parts worth owning: the
aiogram layer and the Telegram user allowlist.

## Architecture

```
Telegram <-> aiogram bot (allowlist, streaming edits, /stop relay)
              |  HTTP 127.0.0.1 only
              v
        opencode serve  --port 4097 --hostname 127.0.0.1
              v
        opencode sessions in /home/clement (AGENTS.md loaded)
```

- One opencode session per Telegram chat, mapped in a small JSON state file.
- User message  -> `POST /session/:id/prompt_async`
- Stream        -> `GET /event` (SSE): assistant text deltas are edited into
  one Telegram message (throttled); tool activity shown as a status header.
- `/stop`       -> `POST /session/:id/abort`
- `/reset`      -> `DELETE /session/:id` + create fresh
- Permission asks (`permission.asked` events) are **relayed to Telegram**
  as inline yes/no buttons -> `POST /session/:id/permissions/:permissionID`
  with `{"response": "once"|"reject"}` (never `always`).
- Streaming: `message.part.delta` deltas (field `text`) accumulated per
  part; `message.part.updated` snapshots are authoritative; `session.idle`
  finalizes the turn.

## Decisions

| Topic      | Choice                                                        |
|------------|---------------------------------------------------------------|
| Model      | `flashnext/qwen3.8-flash-next` (local SGLang), set per message; `/model` later |
| Working dir| `/home/clement` (project root; loads `AGENTS.md`)              |
| Permission | Relay asks to Telegram (yes/no buttons), never auto-approve    |
| Access     | Telegram user-ID allowlist is the only security boundary; server stays on localhost |
| TG lib     | aiogram (long polling), as in the prototype                    |

## Server API surface used (opencode 1.18.34, `/doc` for full OpenAPI)

- `POST /session`, `DELETE /session/:id`, `POST /session/:id/abort`
- `POST /session/:id/prompt_async` (body: `model`, `parts`)
- `GET /event` (SSE bus events: message/part/permission/status)
- `POST /session/:id/permissions/:permissionID` (body: `response`)

Verified 2026-10-02: health, session create, sync message with bash tool
use (model returned real local date), delete, no permission prompt for bash
under current config (relay path still required for safety).

## Components (to build)

1. `opencode-serve.service` — systemd user unit running `opencode serve`
   on 127.0.0.1:4097.
2. `otg/bot.py` — aiogram bot: allowlist gate, chat->session map,
   prompt_async, SSE consumer, throttled edits, permission relay buttons,
   `/start /stop /reset /id` (+ `/model`, `/sessions` later).
3. `otg.service` — systemd unit; runs after `opencode-serve.service`.
4. `state.json` — chat_id -> session_id (small JSON, no external store).

## Security notes

- `opencode serve` unauthenticated on loopback: **never** bind beyond
  127.0.0.1; set `OPENCODE_SERVER_PASSWORD` if anything else must reach it.
- Allowlisted Telegram users effectively have shell on this machine. The
  permission relay (approve each tool ask) is the mitigating control.
- Do not put any real secrets in chat history files (`state.json`, opencode
  storage under `~/.local/share/opencode`).

## Status

- [x] Feasibility probe: serve + session + tool-using prompt round-trip
- [x] Design decisions (model, permissions, working dir)
- [x] `opencode-serve.service` unit
- [x] Bot: allowlist + session mapping + prompt/SSE plumbing
- [x] Permission relay with inline buttons
- [x] Streaming edit polish (flood limits, 4096 splits)
- [x] systemd wiring + docs for ops
- [x] Retire `~/qwen-tgbot` (process killed incl. stray respawn, dir
      deleted, live parity verified 2026-10-02: streaming, tool status,
      permission buttons)

## Next steps (new thread)

- [x] Welcome message at `/start` includes the user's first name
- [x] Gate: deny chatting until `/start` has been sent (tracked in
      state.json; legacy chats auto-migrated as started)
- [x] Variant switching (`/variant default|lean|low|medium|xhigh`) —
      stored per chat in state.json, passed to `prompt_async` as
      `variant` (verified accepted: 204)
- [x] `/session` command printing the session_id so the owner can resume
      the conversation from the desktop; `/id` now returns the Telegram
      user id
- [x] Pretty session titles: "Telegram Session from Clement (@username)
      YYYY-MM-DD" built from `from_user` at session creation
- [x] Custom message for rejected (non-allowlisted) users (one-time,
      includes their id)
- [ ] Language setting (French / English) chosen at `/start`
- [ ] Reply-keyboard "Menu" button grouping all commands (/reset /id …)
- [x] Output formatting for Telegram: **HTML** chosen (simplest escaping) —
      `md_to_html()` converts fenced/inline code, bold, italic, strike,
      headings, links; falls back to plain markdown on parse errors or
      over-long HTML and disables HTML for that renderer
- [x] Per-turn metrics as a separate message after each turn: model,
      wall time (first delta -> idle), output tokens + tok/s, cost —
      summed over all assistant messages of the turn

## Operations

```
systemctl --user {status|restart|stop} otg opencode-serve
journalctl --user -u otg -n 50 --no-pager
```

- Files: `systemd/*.service` symlinked into `~/.config/systemd/user/`
  (edit in repo, then `systemctl --user daemon-reload`).
- `bot.env` (gitignored): `TELEGRAM_BOT_TOKEN`, `ALLOWED_USER_IDS`.
  Only one getUpdates consumer per token — `hermes-gateway.service`
  must stay disabled.
- `state.json` (gitignored): chat_id -> `{session_id, started, variant}`
  (legacy flat `chat_id -> session_id` maps migrate on load, keeping
  those chats started); delete or edit to re-map chats; stale sessions
  are auto-recreated on next message.
- Model/provider comes from `~/.config/opencode/opencode.json`
  (`flashnext` = SGLang on 127.0.0.1:30001, key via file). The SGLang
  engine runs as the `qwen38-flash` docker container.
- Ops logs use `journalctl --user -u <unit> -n <N> --no-pager` (never bare
  dumps).

## Relationship to ~/qwen-tgbot

Prototype (direct sglang + MCP client loop). Decommissioned 2026-10-02:
process killed, directory deleted, token now exclusively used by OTG.
