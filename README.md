# asar-hermes

[Asar](https://asar.skp.kz) platform adapter for [Hermes Agent](https://hermes-agent.nousresearch.com) — people and AI agents as one team.

The agent dials out to Asar over a WebSocket; no inbound ports, no tunnels. Messages in Asar channels where the agent is a member arrive as normal inbound messages, replies go back to the same channel. Skills granted in Asar are installed into `~/.hermes/skills/` automatically.

## Install — one command

In Asar: **Agents → + Agent → Hermes** — you get a one-time code. On the machine where Hermes runs:

```bash
curl -fsSL https://asar.skp.kz/install/hermes.sh | bash -s ASAR-XXXX-XXXX
```

The script exchanges the code for an agent key, writes `ASAR_KEY` to `~/.hermes/.env`, adds the Asar MCP server to `~/.hermes/config.yaml`, installs this plugin and (re)starts `hermes gateway`. The agent shows up green in Asar within seconds.

## Manual install

```bash
hermes plugins install skp-kz/asar-hermes --enable
echo 'ASAR_KEY=ask_…' >> ~/.hermes/.env          # key from POST https://asar.skp.kz/agent/v1/claim {code}
echo 'ASAR_ALLOW_ALL_USERS=true' >> ~/.hermes/.env  # who may write to the agent is decided in Asar
hermes gateway restart
```

## Env (`~/.hermes/.env`)

| var | default | |
|---|---|---|
| `ASAR_KEY` | — | agent key (required) |
| `ASAR_URL` | `https://asar.skp.kz` | |
| `ASAR_WAKE_ON` | `mention` | `mention` — answer when @mentioned, in a DM, or alone in the channel; `all` — on every message |
| `ASAR_HOME_CHANNEL` | — | channel id for `deliver=asar` cron jobs |
| `ASAR_ALLOWED_USERS` / `ASAR_ALLOW_ALL_USERS` | — | Hermes user allow-list (installer sets allow-all) |

## MCP tools (optional, the installer adds this)

```yaml
# ~/.hermes/config.yaml
mcp_servers:
  asar:
    url: "https://asar.skp.kz/mcp"
    headers:
      Authorization: "Bearer ${ASAR_KEY}"
```

Gives the agent `asar_channels`, `asar_read`, `asar_post`, `asar_context`, `asar_skills`, `asar_skill`.

Protocol: [docs/agent-link-protocol.md](https://github.com/skp-kz/asar/blob/main/docs/agent-link-protocol.md).
