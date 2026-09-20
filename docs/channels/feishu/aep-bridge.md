# Feishu AEP Bridge — 数字员工接入

The feishu channel can run as a digital employee anchored on AEP: platform
senders resolve to enterprise identities, and conversations route through the
resident/ephemeral model. With no `aep` section the channel keeps its
personal-assistant behavior unchanged.

## Configuration

```json
{
  "channel_list": {
    "feishu": {
      "enabled": true,
      "type": "feishu",
      "group_trigger": {"mention_only": true},
      "settings": {
        "app_id": "cli_xxx",
        "app_secret": "...",
        "encrypt_key": "...",
        "verification_token": "...",
        "domain": "",
        "aep": {
          "identity_source_id": "feishu",
          "identity_cache_seconds": 60,
          "warden": {
            "runtime_role_id": "agent-runner",
            "ttl_minutes": 30
          }
        }
      }
    }
  }
}
```

- `app_id` / `app_secret`: 自建应用凭证 (open.feishu.cn → 凭证与基础信息).
- `encrypt_key` / `verification_token`: optional; the websocket long-connection
  mode receives events through the official SDK without a public callback URL.
- `domain`: override for the open-platform base URL (websocket bootstrap and
  REST). Empty keeps the official Feishu (or Lark with `is_lark`) endpoints;
  test doubles point it at a local mock.
- `group_trigger.mention_only`: recommended for digital employees — answer
  only when @-mentioned in groups. Without it the permissive default answers
  every group message.
- Requires the process-level `aep` block (`base_url`, `deployment_id`,
  `username`, `password`, and `home_team_id` when the warden is enabled).

## Identity mapping

The sender key is the ID the channel extracts, in precedence order
`user_id` → `open_id` → `union_id`. When the app may see `user_id` the
mapping should use it (tenant-stable); otherwise `open_id`.

Create the source and mappings through the AEP admin API:

```
POST /aep/v1/admin/identity-sources           {"id":"feishu","kind":"directory","displayName":"Feishu"}
PUT  /aep/v1/admin/identity-sources/feishu/mappings
     {"externalSubjectType":"user","externalId":"ou_xxx","localSubjectId":"<aep user id>"}
```

Unmapped senders are rejected fail-closed (one notice per chat per minute,
configurable text via `aep.unmapped_reply`); disabled mappings resolve as
unmapped. The resident's role needs `identity.read`, and `data_scope.read`
for warden routing and delegated tools.

## Routing

With `aep.warden` configured, requesters whose teams lie strictly inside the
resident's home-team subtree are served by scoped ephemeral forks: the turn
runs in the fork (frozen requester scope) and the reply returns through the
resident's Feishu connection. Peers and superiors converse with the resident
directly, with tools scoped by the mapped requester's identity. Media in
fork-routed chats is limited to text while relayed synchronously.

## Notes

- One bot app per digital employee; several employees in one group compose
  naturally (each answers its own @-mentions).
- WebSocket mode needs no public ingress; the channel dials out.
