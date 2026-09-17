# douyin-spark-login（DouyinSpark 外置配置服务）

[DouyinSpark](https://github.com/wei-la-ya/DouyinSpark) 抖音续火插件的外置配置服务：把「添加/修改账号」的配置网页（纯 API 扫码登录 + 会话列表点选续火目标）部署到独立服务器。插件核心通过 **HTTP start + WebSocket listen** 回调拿结果（无密钥签名）。

完全独立可运行：协议核心（扫码登录 / 会话列表 / a_bogus 签名 / protobuf）自带于 `core/`，不依赖 gsuid_core 或插件仓库。

## 运行

```bash
git clone https://github.com/wei-la-ya/douyin-spark-login.git
cd douyin-spark-login
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8080
```

## 插件侧对接

DouyinSpark 的 Web 控制台配置 → 「外置配置服务地址」填本服务地址（如 `https://dyspark-login.example.com`），之后 `dy添加账号` / `dy添加好友` / `dy修改账号` 自动走外置流程。

## 协议

| 端点 | 说明 |
|---|---|
| `POST /dyspark/start` | body: `{auth, user_id, bot_id, account_id, initial}`，返回 `{ok, page_url}` |
| `GET /dyspark/i/{auth}` | 配置页面（扫码 + 粘贴 Cookie + 点选目标 + 保存） |
| `POST /dyspark/api/scan/start\|refresh/{auth}`、`GET /dyspark/api/scan/status/{auth}`、`POST /dyspark/api/scan/sms/{auth}` | 扫码登录（抖音 PC 客户端 passport，纯 API） |
| `POST /dyspark/api/conversations/{auth}` | 拉取私信会话列表（get_message_by_init） |
| `POST /dyspark/api/setup/{auth}` | 保存（终态） |
| `WS /dyspark/ws/{auth}` | 状态推送 `{status, msg, payload?}`，success 时 payload 携带完整账号数据 |
| `GET /dyspark/health` | 健康检查 |

## 安全提示

- 本服务无鉴权（按设计），请仅在内网/受信任环境暴露，或用反代加访问控制
- Cookie 明文经过 HTTP 传输，公网部署务必套 HTTPS
