# Go WhatsApp ims-bridge 兼容接口说明

本文档说明 Go 版 `go-whatsapp-web-multidevice bridge` 对原 `ims-bridge` 的兼容接口。目标是原调用方不改 proto、不改 NATS 订阅逻辑，只修改 bridge 地址和端口。

## 测试服地址

- gRPC: `70.39.76.234:29091`
- Health/Metrics: `http://70.39.76.234:29191`
- gRPC package/service: `bridge.WhatsAppBridge`
- 传输: plaintext gRPC
- 单次 gRPC 收发上限: 50 MB
- NATS: 事件 subject 等于事件类型，例如 `message.received`

原服务仍保留：

- 原 `ims-bridge`: `9091/9191`
- 已有 `ims-wa-api2`: `19091/19191`
- 当前 Go 兼容实例: `29091/29191`

## 核心兼容约定

- `account_id == device_id`，调用方继续使用原来的 `account_id`。
- proto 不变，接口仍为 `bridge.WhatsAppBridge`。
- 后续发送、状态、联系人、群组等接口均只传 `account_id`，不再传代理。
- `AccountStatusResponse.windows` 固定返回空数组 `[]`，兼容 BitBrowser 字段但不代表真实浏览器窗口。
- `CloseAllTabs` 是 no-op，返回 `success=true`。
- `GetBridgeStats` 返回一个 Go 伪 worker。
- `GetWebServerStats` 用当前账号连接数估算打开数。

## 代理与 UA 环境

代理以平台数据库当前配置为准，由调用方在 `ConnectRequest.proxy` 传入：

1. 每次 `Connect(account_id, proxy)` 都会用本次传入的非空 `proxy` 覆盖 `bridge_environments` 中保存的代理。
2. 如果 `ConnectRequest.proxy` 为空但该账号已有保存代理，则继续复用保存代理，不会清空代理。
3. 如果账号没有保存代理，且本次 `Connect` / 登录流程也没有传入代理，则直接失败；不允许无代理登录或重连。
4. 已有 client 在下一次 `Connect()` / 重连前会重新应用保存的代理；如果 `Connect` 发现代理变化且当前 client 已连接，会先断开再按新代理连接。
5. `SendMessage`、`SendMedia`、`GetAccountStatus` 等接口不接受代理参数，只使用账号环境中保存的代理。
6. UA 仍按 `account_id` 稳定选择并持久化；普通代理变更不会改变 UA。需要重建 UA 时才调用 `Disconnect(clear_session=true)`。
7. `GetQRCode`、`GetLinkCode` 如果环境不存在，会用全局默认代理和 UA 池创建环境；没有全局默认代理时会失败。正常平台流程应先调用 `Connect` 写入数据库当前代理。

代理支持：

- `socks5`
- `http`
- 可带 `username/password`

UA 池：

- 测试服文件: `/opt/go-whatsapp-ims-bridge/ua_US.txt`
- 本地默认参考: `/Users/eric/Downloads/ua_US.txt`
- 支持配置项 `ua.file_path` 或环境变量 `BRIDGE_UA_FILE`
- 启动时过滤空行、重复和非法 UA；无有效 UA 时回退到 Go 默认设备信息。

## gRPC 接口

### Account Connection

| RPC | 说明 | 关键入参 | 关键出参 |
| --- | --- | --- | --- |
| `Connect` | 连接或恢复账号；用本次传入代理覆盖账号环境代理，UA 保持稳定 | `account_id`, `tenant_id`, `proxy` | `success`, `status`, `message` |
| `Disconnect` | 断开账号；可清 session | `account_id`, `clear_session`, `close_mode` | `success` |
| `GetQRCode` | 服务端流式返回 QR 登录码 | `account_id`, `phone_number` | `account_id`, `qr_code`, `stage`, `message` |
| `GetLinkCode` | 生成手机号配对码 | `account_id`, `phone_number` | `account_id`, `link_code`, `expires_at` |

`ConnectResponse.status` 常见值：

- `connected`: 已连接且已登录
- `qr_pending`: 需要扫码登录
- `connecting`: 正在连接
- `failed`: 连接失败

`QRCodeResponse.stage` 常见值：

- `qr_generated`
- `scanned`
- `authenticated`
- `failed`

`ConnectRequest.proxy`:

```json
{
  "type": "socks5",
  "host": "127.0.0.1",
  "port": 1080,
  "username": "user",
  "password": "pass"
}
```

### Message Operations

| RPC | 说明 | 关键入参 | 关键出参 |
| --- | --- | --- | --- |
| `SendMessage` | 发送文本消息 | `account_id`, `to`, `type=text`, `content.text`, `quoted_msg_id` | `success`, `message_id`, `status`, `error` |
| `SendMedia` | 发送图片、视频、文档、音频 | `account_id`, `to`, `type`, `media_url`, `filename`, `caption`, `mime_type`, `send_audio_as_voice` | `success`, `message_id`, `status`, `error` |
| `SendContact` | 发送联系人卡片 | `account_id`, `to`, `contact_data` | `success`, `message_id`, `status`, `error` |
| `GetMessageStatus` | 查询消息状态 | `account_id`, `message_id` | `message_id`, `status`, `timestamp` |
| `DeleteMessage` | 删除消息 | `account_id`, `chat_id`, `message_id` | `success`, `error` |

`SendMedia.type` 支持：

- `image`
- `video`
- `audio`
- `document`

`to` 可以是手机号或群 JID，格式沿用原 ims-bridge 调用方。

### Contact Operations

| RPC | 说明 | 关键入参 | 关键出参 |
| --- | --- | --- | --- |
| `GetContacts` | 获取联系人列表 | `account_id` | `contacts[]` |
| `CheckNumber` | 检查号码是否注册 WhatsApp | `account_id`, `phone_numbers[]` | `results` |
| `AddContact` | 添加联系人 | `account_id`, `phone`, `first_name`, `last_name` | `success`, `error` |
| `GetContactDetail` | 查询联系人详情 | `account_id`, `phone` | `contact` |

### Profile Operations

| RPC | 说明 | 关键入参 | 关键出参 |
| --- | --- | --- | --- |
| `SetProfilePicture` | 设置头像 | `account_id`, `image_url` | `success`, `error` |
| `SetStatus` | 设置个人状态文本 | `account_id`, `status_text` | `success`, `error` |
| `SetDisplayName` | 设置显示名 | `account_id`, `display_name` | `success`, `error` |

### Group Operations

| RPC | 说明 | 关键入参 | 关键出参 |
| --- | --- | --- | --- |
| `GetGroups` | 获取群列表 | `account_id` | `groups[]` |
| `GetGroupMembers` | 获取群成员 | `account_id`, `group_jid` | `members[]` |
| `CreateGroup` | 创建群 | `account_id`, `name`, `participants[]`, `avatar_url` | `success`, `group_jid`, `error` |
| `UpdateGroup` | 更新群名称/描述 | `account_id`, `group_jid`, `name`, `description` | `success`, `error` |
| `AddGroupMembers` | 添加群成员 | `account_id`, `group_jid`, `participants[]` | `success`, `added[]`, `failed[]`, `error` |
| `RemoveGroupMembers` | 移除群成员 | `account_id`, `group_jid`, `participants[]` | `success`, `removed[]`, `failed[]`, `error` |
| `PromoteGroupMembers` | 提升管理员 | `account_id`, `group_jid`, `participants[]` | `success`, `promoted[]`, `failed[]`, `error` |
| `DemoteGroupMembers` | 取消管理员 | `account_id`, `group_jid`, `participants[]` | `success`, `demoted[]`, `failed[]`, `error` |
| `LeaveGroup` | 退出群 | `account_id`, `group_jid` | `success`, `error` |
| `JoinGroupByLink` | 通过邀请链接入群 | `account_id`, `invite_link` | `success`, `group_jid`, `error` |
| `SetGroupAdminsOnly` | 设置仅管理员发言 | `account_id`, `group_jid`, `admins_only` | `success`, `error` |

### Reactions

| RPC | 说明 | 关键入参 | 关键出参 |
| --- | --- | --- | --- |
| `ReactToMessage` | 添加或移除消息表情反应；`emoji` 为空表示移除 | `account_id`, `message_id`, `emoji` | `success`, `message_id`, `emoji`, `action`, `error` |
| `GetMessageReactions` | 查询消息反应 | `account_id`, `message_id` | `success`, `message_id`, `has_reaction`, `reactions[]`, `error` |

### Status/Story

| RPC | 说明 | 关键入参 | 关键出参 |
| --- | --- | --- | --- |
| `SendStatus` | 发布文本状态 | `account_id`, `content` | `success`, `message_id`, `has_media`, `error` |
| `CommentStatus` | 兼容接口；当前 Go bridge 返回不支持 | `account_id`, `message_id` 或 `user_id`, `comment` | `success=false`, `error` |
| `LikeStatus` | 兼容接口；当前 Go bridge 返回不支持 | `account_id`, `message_id` 或 `user_id` | `success=false`, `error` |
| `GetStatusViewers` | 兼容接口；当前返回空 viewer 列表 | `account_id`, `message_id` | `success`, `viewers[]`, `total_count`, `remaining_count` |

### Status Queries / Stats

| RPC | 说明 | 关键入参 | 关键出参 |
| --- | --- | --- | --- |
| `GetAccountStatus` | 查询账号可用状态 | `account_id` | `status`, `is_usable`, `status_detail`, `windows[]` |
| `GetConnectionState` | 查询连接状态 | `account_id` | `state`, `worker_id` |
| `GetAccountStats` | 查询账号在线统计 | `account_id` | `total_online_seconds`, `current_session_seconds`, `is_online` |
| `GetBridgeStats` | 查询 bridge worker 统计 | `include_workers` | `instance_id`, `total_workers`, `ready_workers`, `total_accounts`, `workers[]` |
| `GetWebServerStats` | 查询单个 web server 容量估算 | `server` | `capacity`, `opened_estimate`, `actual_open_count` |
| `BatchGetWebServerStats` | 批量查询 web server 容量估算 | `servers[]` | `stats[]` |
| `CloseAllTabs` | 兼容 BitBrowser 清理接口；no-op | `account_id` | `success=true` |

`GetConnectionState.state` 常见值：

- `CONNECTED`
- `DISCONNECTED`
- `CONNECTING`
- `QR_PENDING`

`GetAccountStatus.status` 常见值：

- `online`
- `qr_pending`
- `offline`

## NATS 事件

NATS subject 等于事件类型，payload 是 JSON。所有事件默认包含：

```json
{
  "type": "message.received",
  "accountId": "account_001",
  "timestamp": 1778548311651
}
```

常用事件：

| Subject | 说明 | 关键字段 |
| --- | --- | --- |
| `bridge.started` | bridge 启动 | `instanceId` |
| `worker.ready` | worker 就绪 | `workerId`, `pid` |
| `account.connected` | 账号连接成功 | `phoneNumber`, `workerId`, `connectedAt`, `verified` |
| `account.disconnected` | 账号断开 | `reason` |
| `account.authenticated` | 扫码或配对成功 | `phoneNumber` |
| `account.logout` | 账号登出 | `reason` |
| `account.qrcode` | 生成 QR | `qrCode`, `stage`, `expiresAt` |
| `account.linkcode` | 生成配对码 | `linkCode`, `expiresAt` |
| `account.heartbeat_batch` | 心跳批次 | `accountIds[]`, `instanceId` |
| `message.received` | 收到消息 | `message`, `source` |
| `message.media_ready` | 入站媒体下载完成 | `messageId`, `mediaLocalPath`, `mimetype` |
| `message.sent` | 发送成功 | `messageId`, `to` |
| `message.failed` | 发送失败 | `to`, `error` |
| `message.status` | 消息状态变化 | `messageId`, `status` |
| `message.revoked` | 消息撤回 | `messageId`, `revokedBy` |
| `group.join` | 群成员加入 | `groupJid`, `participant` |
| `group.leave` | 群成员离开 | `groupJid`, `participant` |
| `group.joined` | 当前账号加入群 | `groupJid` |

`message.received.message` 关键字段：

```json
{
  "id": "MESSAGE_ID",
  "accountId": "account_001",
  "chatId": "8613800138000@s.whatsapp.net",
  "from": "8613800138000@s.whatsapp.net",
  "to": "",
  "type": "text",
  "content": { "text": "hello" },
  "timestamp": 1778548311,
  "isGroup": false,
  "isFromMe": false,
  "quotedMessage": null,
  "mimetype": "",
  "author": "8613800138000@s.whatsapp.net",
  "hasMedia": false,
  "senderName": "name",
  "senderPhone": "8613800138000",
  "filename": ""
}
```

## Health / Metrics

| Path | 说明 |
| --- | --- |
| `/health` | 存活检查，返回 `{"status":"ok"}` |
| `/health/ready` | 就绪检查 |
| `/ready` | 就绪检查，兼容旧探针 |
| `/health/detail` | worker 和账号明细 |
| `/metrics` | Prometheus 文本指标 |

`/ready` 成功示例：

```json
{
  "status": "ready",
  "redis": true,
  "nats": true,
  "workers": 1
}
```

主要 metrics：

- `bridge_active_connections`
- `bridge_active_workers`
- `bridge_workers_ready`
- `bridge_worker_account_count`
- `bridge_events_published_total`

## grpcurl 示例

列出服务方法：

```bash
grpcurl -plaintext \
  -import-path proto \
  -proto bridge.proto \
  70.39.76.234:29091 \
  list bridge.WhatsAppBridge
```

连接并写入/覆盖 SOCKS5 代理：

```bash
grpcurl -plaintext \
  -import-path proto \
  -proto bridge.proto \
  -d '{
    "accountId": "account_001",
    "tenantId": "tenant_001",
    "proxy": {
      "type": "socks5",
      "host": "proxy.example.com",
      "port": 1080,
      "username": "user",
      "password": "pass"
    }
  }' \
  70.39.76.234:29091 \
  bridge.WhatsAppBridge.Connect
```

获取 QR：

```bash
grpcurl -plaintext \
  -import-path proto \
  -proto bridge.proto \
  -d '{"accountId":"account_001"}' \
  70.39.76.234:29091 \
  bridge.WhatsAppBridge.GetQRCode
```

发送文本：

```bash
grpcurl -plaintext \
  -import-path proto \
  -proto bridge.proto \
  -d '{
    "accountId": "account_001",
    "to": "8613800138000",
    "type": "text",
    "content": { "text": "hello" }
  }' \
  70.39.76.234:29091 \
  bridge.WhatsAppBridge.SendMessage
```

清 session 并重建 UA 与账号环境：

```bash
grpcurl -plaintext \
  -import-path proto \
  -proto bridge.proto \
  -d '{"accountId":"account_001","clearSession":true}' \
  70.39.76.234:29091 \
  bridge.WhatsAppBridge.Disconnect
```

## 配置项

常用环境变量：

| 变量 | 说明 | 当前测试服值 |
| --- | --- | --- |
| `INSTANCE_ID` | bridge 实例 ID | `go-whatsapp-ims-bridge-test` |
| `NATS_URL` | NATS 地址 | `nats://127.0.0.1:4222` |
| `BRIDGE_UA_FILE` | UA 池文件 | `/opt/go-whatsapp-ims-bridge/ua_US.txt` |
| `UPLOAD_MEDIA_URL` | 入站媒体上传地址 | `http://127.0.0.1:8080/internal/media/upload` |
| `UPLOAD_API_KEY` | 入站媒体上传 API key | 部署时通过环境变量注入 |

启动参数：

```bash
/opt/go-whatsapp-ims-bridge/go-whatsapp-web bridge \
  --grpc-port=29091 \
  --metrics-port=29191 \
  --ua-file=/opt/go-whatsapp-ims-bridge/ua_US.txt
```

## 对接方改造点

最小改造只需要改地址：

- gRPC bridge 地址改为 `70.39.76.234:29091`
- 健康检查地址改为 `http://70.39.76.234:29191`
- proto、method、message、field number、NATS subject 和消费者逻辑保持不变
