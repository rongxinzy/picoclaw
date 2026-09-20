# picoclaw Runtime API

一个 picoclaw 网关进程（作为 AEP 数字员工运行）暴露的全部 HTTP 接口的
OpenAPI 契约。

- `picoclaw-runtime.openapi.yaml` — OpenAPI 3.1 契约（唯一真相，改接口先改这里）
- `index.html` — Swagger UI 查看器

## 查看方式

Swagger UI 需要从 HTTP 提供（浏览器不允许 file:// 直接加载 spec），任选其一：

```bash
# 本目录起一个静态服务，浏览器打开 http://127.0.0.1:8099
python3 -m http.server 8099

# 或用 redocly（AEP 仓里有）
npx redocly preview-docs picoclaw-runtime.openapi.yaml
```

## 覆盖范围

| 分组 | 接口 | 认证 |
|---|---|---|
| aepchat | `POST/GET /aepchat/v1/chats/{chatId}/messages`、`GET /aepchat/v1/health` | 人类账号的 AEP access token（kind=agent 拒绝，防 bot 环） |
| Relay | `POST /aepchat/v1/relay/turns` | fork supervisor 生成的一次性 relay secret（只在临时实例上有效） |
| A2A | `POST /a2a/`（JSON-RPC 2.0） | 对端数字员工的 AEP token + `agents.invoke` 权限 |
| Agent Card | `GET /.well-known/agent-card.json`、`GET /a2a/card.json` | 无 |
| Health | `GET /health`、`GET /ready` | 无 |

明确**不在**本契约内：

- **AEP 控制面 API**（登录、identity sources/mappings、数字员工目录、
  data-scope 规则等）归 AEP 契约仓所有（`Agent-Enterprise-Protocol/openapi/`），
  此处不重复。
- 平台通道（飞书/企微）的入站连接是平台→运行时的 websocket 长连接，
  不是本进程暴露的服务端接口，不在 HTTP API 之列。

## 契约纪律

- 改动任何路径、状态码、错误码（`ChannelError.code`、JSON-RPC error code）
  或 schema 字段，先改 YAML 再改代码，保持二者一致。
- 错误码是稳定契约：`INVALID_REQUEST`、`RELAY_UNAUTHORIZED`、
  `RELAY_TURN_TIMEOUT` 等已在 schema 的 enum 中枚举，新增即扩展。
- lint（`--skip-rule=no-path-trailing-slash`，因为 `/a2a/` 是真实的子树路由）：

```bash
npx redocly lint --skip-rule=no-path-trailing-slash picoclaw-runtime.openapi.yaml
```
