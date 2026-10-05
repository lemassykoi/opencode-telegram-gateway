# opencode v2 compatibility report

OTG targets the **opencode 1.x server API** (verified against 1.18.34).
opencode v2 (probed live: **v2.0.22** on a separate machine, 2026-10-05) is
**not compatible**: the V2 migration guide lists the server API as one of
three intentional breaking changes, and every endpoint OTG calls is gone.

## Endpoint mapping (v1 -> v2)

| OTG (v1.18.34) | v2.0.22 equivalent |
|---|---|
| `POST /session` `{title}` | `POST /api/session` `{title, model, agent, location, permissions}` — model/agent can be pinned at creation |
| `GET /session/:id` | `GET /api/session/:id` (response wrapped in `{data}`) |
| `DELETE /session/:id` | `DELETE /api/session/:id` (204) |
| `POST /session/:id/prompt_async` `{parts, model, agent, variant}` | `POST /api/session/:id/prompt` `{text, files?, delivery?, resume?}` — durable **inbox** admission; `delivery: "steer"` (default) injects into a running execution, so follow-up queuing is native |
| `POST /session/:id/abort` | `POST /api/session/:id/interrupt` |
| `GET /session/status` | `GET /api/session/active` (foreground drains only) |
| `GET /session/:id/message` `[{info, parts}]` | `GET /api/session/:id/message` `{data: [...]}` — new message/part schemas (`type: user|assistant|idle`, `content[]`) |
| `POST /session/:id/permissions/:pid` `{response}` | `POST /api/session/:id/permission/...` reply op (`session.permission.reply`) |
| `GET /event` | `GET /api/event` — envelope `{id, type, location, data, durable?}`; **volatile by contract**: slow consumers overflow and the stream fails |
| `GET /agent` | `GET /api/agent` |
| model `{providerID, modelID}` | `Model.Ref` = `{providerID, id, variant?}` |

## Event vocabulary (captured live, plain prompt turn)

`server.connected`, `session.created`, `project.updated`,
`session.inbox.enqueued`, `session.inbox.delivered`,
`session.execution.started`, `session.instructions.updated`,
`session.step.started`, `session.reasoning.started/delta/ended`,
`session.text.started/delta/ended`, `session.step.streamed`,
`session.step.ended` (carries `cost` + `tokens`),
`session.usage.updated`, `session.execution.succeeded`.

Renderer-relevant changes:

- `message.part.delta` -> `session.text.delta` `{sessionID,
  assistantMessageID, ordinal, delta}` — no part ids, no
  `message.part.updated` snapshots; `session.text.ended` carries the
  full text (authoritative).
- `session.idle` -> `session.execution.succeeded` (also
  `session.execution.failed` presumably).
- No `message.updated`/role cache needed: events carry
  `assistantMessageID` directly.
- Every event has `location.directory` — a multi-location server needs
  filtering; OTG's single working dir maps to one location.
- Metrics come from `session.step.ended` / `session.usage.updated`.

## Auth

v2 requires HTTP basic auth out of the box: the persistent service stores
a password in `~/.config/opencode/service.json` (username `opencode`);
`opencode serve` honors `OPENCODE_SERVER_PASSWORD`. OTG currently sends
no credentials — a v2 client must.

## Config compatibility

v2 reads the same `~/.config/opencode/opencode.json` (V1 format supported,
normalized in memory): the `flashnext` provider, `ask` agent, and voicebox
MCP entry carry over unchanged. AGENTS.md discovery unchanged.

## What a v2 migration would touch

`ensure_session`, `on_text`/prompt body, `handle_event` (full event
rewrite), `resync`, permission relay, `/stop`, `/reset`, `/agent`,
metrics — i.e. the whole HTTP/event layer. The aiogram/Telegram layer,
Renderer text accumulation, memory notepad, and voice relay are portable
with small changes. Native inbox `delivery` would replace the bot-side
follow-up queue.

A public v2-based rewrite lives in a separate project
(`opencode-v2-telegram-gateway`); this repo stays pinned to opencode 1.x.
