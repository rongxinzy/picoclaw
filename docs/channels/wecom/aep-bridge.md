# WeCom AEP Bridge — 数字员工接入

The wecom channel (智能机器人 websocket 长连接) can run as a digital employee
anchored on AEP: bot DMs and group @-mentions resolve to enterprise
identities and route through the resident/ephemeral model. With no `aep`
section the channel keeps its personal-assistant behavior unchanged.

## Configuration

```json
{
  "channel_list": {
    "wecom": {
      "enabled": true,
      "type": "wecom",
      "settings": {
        "bot_id": "xxx",
        "secret": "...",
        "websocket_url": "",
        "aep": {
          "identity_source_id": "wecom",
          "identity_cache_seconds": 60
        }
      }
    }
  }
}
```

- `bot_id` / `secret`: 智能机器人凭证 (企业微信管理后台 → 智能机器人).
- `websocket_url`: override for the long-connection endpoint (defaults to
  `wss://openws.work.weixin.qq.com`); test doubles point it at a local mock.
- The warden block is optional on this channel — the routing pair is
  process-wide, so configuring it on any one channel (feishu, wecom, aepchat)
  enables it for all.
- Requires the process-level `aep` block; see the feishu bridge doc for the
  shared identity and routing semantics.

## Identity mapping

The sender key is `from.userid` as delivered by the bot callback. Note: when
the robot's owner is not the corp super admin, WeCom may deliver an encrypted
userid — build mappings against the ID as actually seen (check the channel
logs on the first message). One bot per digital employee.

Unmapped senders are rejected fail-closed with one notice per chat per
minute; group messages are additionally gated by the platform's @-mention
requirement.
