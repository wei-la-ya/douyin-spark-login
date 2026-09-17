"""DouyinSpark 外置配置服务（独立运行，零插件依赖）

DouyinSpark 插件（github.com/wei-la-ya/DouyinSpark）通过 HTTP start + WS listen 与本服务通信（无密钥签名）。
协议核心（扫码登录/会话列表/私信）在本项目 core/ 内自带，可直接运行。

运行：
    pip install -r requirements.txt
    uvicorn main:app --host 0.0.0.0 --port 8080
"""
from __future__ import annotations

import asyncio
import json
import re
import time
from typing import Any, Dict, Optional, Set

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse

from core.api import build_cookie_header
from core.conversations import list_conversations
from core.qrlogin import QrLoginSession, SmsLoginSession
from page import _SETUP_PAGE_HTML

PREFIX = "/dyspark"
SESSION_TTL_S = 600
_WS_PING_S = 20.0

app = FastAPI(title="DouyinSpark 外置配置服务", docs_url=None, redoc_url=None)


@app.on_event("startup")
async def _check_websocket_dep() -> None:
    """启动时检查 websockets 依赖：缺失则直接报错，避免 /dyspark/ws/* 静默退化为 HTTP 404"""
    try:
        import websockets  # noqa: F401
    except ImportError:
        raise RuntimeError(
            "缺少 websockets 依赖，无法启用 WS 回调。请执行：\n"
            "  pip install -r requirements.txt\n"
            "或：\n"
            "  uv sync"
        )


class Session:
    def __init__(self, auth: str, body: Dict[str, Any]) -> None:
        self.auth = auth
        self.user_id = body.get("user_id", "")
        self.bot_id = body.get("bot_id", "")
        self.account_id = body.get("account_id")
        self.initial = body.get("initial") or {}
        self.created_at = time.time()
        self.status = "pending"  # pending | success | failed | expired
        self.msg = ""
        self.payload: Optional[Dict[str, Any]] = None
        self.listeners: Set[asyncio.Queue] = set()

    @property
    def expired(self) -> bool:
        return time.time() - self.created_at > SESSION_TTL_S

    def snapshot(self) -> Dict[str, Any]:
        if self.expired and self.status == "pending":
            self.status = "expired"
        out: Dict[str, Any] = {"status": self.status, "msg": self.msg}
        if self.status == "success" and self.payload is not None:
            out["payload"] = self.payload
        return out

    async def broadcast(self) -> None:
        snap = self.snapshot()
        for queue in list(self.listeners):
            queue.put_nowait(snap)


sessions: Dict[str, Session] = {}
scans: Dict[str, QrLoginSession] = {}
sms_sessions: Dict[str, SmsLoginSession] = {}


def _get(auth: str) -> Optional[Session]:
    session = sessions.get(auth)
    if session is None:
        return None
    if session.expired and session.status == "pending":
        session.status = "expired"
    return session


def _fail(message: str, status: int = 400) -> JSONResponse:
    return JSONResponse({"ok": False, "message": message}, status_code=status)


def _ok(**kwargs: Any) -> JSONResponse:
    return JSONResponse({"ok": True, **kwargs})


@app.post(PREFIX + "/start")
async def start(request: Request) -> JSONResponse:
    body = await request.json()
    auth = str(body.get("auth", "")).strip()
    if not auth:
        return _fail("缺少 auth")
    sessions[auth] = Session(auth, body)
    old = scans.pop(auth, None)
    if old is not None:
        old.cancel()
    return _ok(page_url=f"{str(request.base_url).rstrip('/')}{PREFIX}/i/{auth}")


@app.get(PREFIX + "/i/{auth}", response_class=HTMLResponse)
async def page(auth: str) -> HTMLResponse:
    session = _get(auth)
    if session is None:
        return HTMLResponse("<!doctype html><meta charset=utf-8><body><p>链接无效或已过期。</p></body>")
    editing = session.account_id is not None
    initial = {
        "name": session.initial.get("name", ""),
        "messageTemplate": session.initial.get("messageTemplate", ""),
        "targets": session.initial.get("targets", []),
        "cookieRequired": not editing,
        "email": session.initial.get("email", ""),
        "successEmailEnabled": session.initial.get("successEmailEnabled", False),
    }
    data = json.dumps(initial, ensure_ascii=False).replace("<", "\\u003c").replace(">", "\\u003e").replace("&", "\\u0026")
    html = (
        _SETUP_PAGE_HTML
        .replace("__TITLE__", "修改抖音账号" if editing else "添加抖音账号")
        .replace("__TOKEN__", auth)
        .replace("__DATA__", data)
        .replace("__PREFIX__", PREFIX)
        .replace("__COOKIE_REQUIRED__", "" if editing else "required")
        .replace(
            "__COOKIE_PLACEHOLDER__",
            "留空则保留当前 Cookie；需要更新时粘贴或选择 .txt 文件。" if editing else "粘贴 Cookie-Editor 导出的 JSON 数组，或先选择 .txt 文件。",
        )
        .replace("__SUBMIT_LABEL__", "保存修改" if editing else "添加账号")
    )
    return HTMLResponse(html)


# ---------- 扫码登录 ----------

async def _start_scan(auth: str, force: bool) -> JSONResponse:
    if _get(auth) is None:
        return _fail("链接无效或已过期。", 404)
    current = scans.get(auth)
    if not force and current is not None and current.status in ("waiting", "scanned", "sms") and current.qr:
        return _ok(status=current.status, qr=current.qr, message=current.message)
    old = scans.pop(auth, None)
    if old is not None:
        old.cancel()
        await old.aclose()
    scan = QrLoginSession()
    scans[auth] = scan
    try:
        await scan.start()
        for _ in range(40):
            if scan.qr or scan.status == "error":
                break
            await asyncio.sleep(0.25)
        if not scan.qr:
            raise RuntimeError(scan.error or "获取二维码失败")
        return _ok(status="waiting", qr=scan.qr)
    except Exception as e:
        scans.pop(auth, None)
        return _fail(f"启动扫码登录失败：{e}")


@app.post(PREFIX + "/api/scan/start/{auth}")
async def scan_start(auth: str) -> JSONResponse:
    return await _start_scan(auth, force=False)


@app.post(PREFIX + "/api/scan/refresh/{auth}")
async def scan_refresh(auth: str) -> JSONResponse:
    return await _start_scan(auth, force=True)


@app.get(PREFIX + "/api/scan/status/{auth}")
async def scan_status(auth: str) -> JSONResponse:
    if _get(auth) is None:
        return _fail("链接无效或已过期。", 404)
    scan = scans.get(auth)
    if scan is None:
        return _ok(status="idle")
    if scan.status == "error":
        message = scan.error or "扫码登录失败。"
        scans.pop(auth, None)
        return _ok(status="error", message=message)
    if scan.status == "success":
        return _ok(status="success", cookies=scan.cookies)
    if scan.status == "sms":
        return _ok(status="sms", message=scan.message or "请输入短信验证码。")
    if scan.status == "scanned":
        return _ok(status="waiting", message=scan.message or "已扫码，请在抖音 App 上确认。")
    return _ok(status="waiting", message=scan.message or "")


@app.post(PREFIX + "/api/scan/sms/{auth}")
async def scan_sms(auth: str, request: Request) -> JSONResponse:
    scan = scans.get(auth)
    if scan is None or scan.status != "sms":
        return _fail("当前不在短信验证环节。")
    body = await request.json()
    code = str(body.get("code", "")).strip()
    if not re.fullmatch(r"\d{4,8}", code):
        return _fail("请输入 4 到 8 位短信验证码。")
    scan.submit_sms_code(code)
    return _ok(message="验证码已提交，请等待登录结果。")


# ---------- 手机号短信验证码登录（扫码备选） ----------


@app.post(PREFIX + "/api/sms/send/{auth}")
async def sms_send(auth: str, request: Request) -> JSONResponse:
    if _get(auth) is None:
        return _fail("链接无效或已过期。", 404)
    body = await request.json()
    mobile = str(body.get("mobile", "")).strip()
    if not mobile:
        return _fail("请输入手机号")
    old = sms_sessions.pop(auth, None)
    if old is not None:
        await old.aclose()
    session = SmsLoginSession()
    sms_sessions[auth] = session
    try:
        await session.send_code(mobile)
        return _ok(status=session.status, message=session.message)
    except Exception as e:
        sms_sessions.pop(auth, None)
        return _fail(str(e))


@app.post(PREFIX + "/api/sms/submit/{auth}")
async def sms_submit(auth: str, request: Request) -> JSONResponse:
    if _get(auth) is None:
        return _fail("链接无效或已过期。", 404)
    session = sms_sessions.get(auth)
    if session is None:
        return _fail("请先发送短信验证码")
    body = await request.json()
    code = str(body.get("code", "")).strip()
    try:
        await session.submit_code(code)
        return _ok(status="success", cookies=session.cookies, message="短信登录成功，Cookie 已填入下方文本框，请继续提交。")
    except Exception as e:
        return _fail(str(e))


# ---------- 会话列表 ----------

@app.post(PREFIX + "/api/conversations/{auth}")
async def conversations(auth: str, request: Request) -> JSONResponse:
    session = _get(auth)
    if session is None:
        return _fail("链接无效或已过期。", 404)
    body = await request.json()
    cookie_text = str(body.get("cookieText", "")).strip()
    if not cookie_text:
        return _fail("请先粘贴 Cookie JSON 或完成扫码登录，再拉取会话列表")
    try:
        cookies = json.loads(cookie_text)
        if not isinstance(cookies, list):
            raise ValueError
    except ValueError:
        return _fail("Cookie JSON 格式不正确")
    try:
        build_cookie_header(cookies)
        people = await list_conversations(cookies)
        return _ok(list=[
            {
                "secUid": p["sec_uid"],
                "uid": p["uid"],
                "nickname": p.get("nickname", ""),
                "uniqueId": p.get("unique_id", ""),
                "avatar": p.get("avatar", ""),
                "conversationId": p.get("conversation_id", ""),
                "conversationShortId": p.get("conversation_short_id", ""),
                "ticket": p.get("ticket", ""),
            }
            for p in people
        ])
    except Exception as e:
        return _fail(f"拉取会话列表失败：{e}")


# ---------- 保存（终态，WS 回调给插件） ----------

_SEC_UID_RE = re.compile(r"^MS4w[\w-]{10,}$")


@app.post(PREFIX + "/api/setup/{auth}")
async def save(auth: str, request: Request) -> JSONResponse:
    session = _get(auth)
    if session is None:
        return _fail("链接无效或已过期。", 404)
    body = await request.json()
    editing = session.account_id is not None

    name = str(body.get("name", "")).strip()
    if not name or len(name) > 40:
        return _fail("账号名称必填且不能超过 40 个字符")
    cookie_text = str(body.get("cookieText", "")).strip()
    cookies = None
    if cookie_text:
        try:
            cookies = json.loads(cookie_text)
            if not isinstance(cookies, list) or not all(isinstance(c, dict) and c.get("name") for c in cookies):
                raise ValueError
        except ValueError:
            return _fail("Cookie JSON 格式不正确，应为 Cookie-Editor 导出的数组")
        if not any(c.get("name") == "sessionid" for c in cookies):
            return _fail("Cookie 中缺少 sessionid，请导出完整 Cookie")
    elif not editing:
        return _fail("请粘贴 Cookie JSON 或先扫码登录")

    targets = [
        {
            "sec_uid": str(t["secUid"]),
            "uid": str(t.get("uid", "")),
            "nickname": str(t.get("nickname", ""))[:60],
            "unique_id": str(t.get("uniqueId", ""))[:60],
            "avatar": str(t.get("avatar", ""))[:500],
            "conversation_id": str(t.get("conversationId", "")),
            "conversation_short_id": str(t.get("conversationShortId", "")),
            "ticket": str(t.get("ticket", "")),
        }
        for t in (body.get("targets") or [])
        if isinstance(t, dict) and isinstance(t.get("secUid"), str) and _SEC_UID_RE.match(str(t.get("secUid")))
    ]

    session.payload = {
        "account_id": session.account_id,
        "name": name,
        "cookies": cookies,
        "message_template": str(body.get("messageTemplate", "")),
        "targets": targets,
        "email": str(body.get("email", "")).strip(),
        "success_email_enabled": bool(body.get("successEmailEnabled")),
    }
    session.status = "success"
    session.msg = f"账号已{'更新' if editing else '添加'}，已保存 {len(targets)} 个续火目标。现在可以关闭此页面。"
    await session.broadcast()
    return _ok(message=session.msg)


# ---------- WS 回调（插件侧 listen） ----------

@app.websocket(PREFIX + "/ws/{auth}")
async def listen_ws(websocket: WebSocket, auth: str) -> None:
    await websocket.accept()
    session = sessions.get(auth)
    if session is None:
        await websocket.send_json({"status": "expired", "msg": ""})
        await websocket.close()
        return
    queue: asyncio.Queue = asyncio.Queue()
    session.listeners.add(queue)
    queue.put_nowait(session.snapshot())
    try:
        while True:
            try:
                snap = await asyncio.wait_for(queue.get(), timeout=_WS_PING_S)
            except asyncio.TimeoutError:
                await websocket.send_json({"status": "heartbeat", "msg": ""})
                continue
            await websocket.send_json(snap)
            if snap["status"] in ("success", "failed", "expired"):
                await websocket.close()
                return
    except WebSocketDisconnect:
        return
    finally:
        session.listeners.discard(queue)
        if session.status in ("success", "failed", "expired"):
            sessions.pop(auth, None)
            old = scans.pop(auth, None)
            if old is not None:
                old.cancel()


@app.get(PREFIX + "/health")
async def health() -> Dict[str, str]:
    return {"status": "ok"}


# ===================== 启动入口（uv run start） =====================

def _load_server_config() -> tuple[str, int]:
    """读取 config.toml 的 [server] 节；缺失时用默认值"""
    import tomllib
    from pathlib import Path

    config_file = Path(__file__).parent / "config.toml"
    host, port = "0.0.0.0", 8080
    if config_file.exists():
        with config_file.open("rb") as f:
            server = tomllib.load(f).get("server", {})
        host = str(server.get("host", host))
        port = int(server.get("port", port))
    return host, port


def start() -> None:
    """uv run start 入口：按 config.toml 启动服务（host 默认 0.0.0.0，port 默认 8080）"""
    import uvicorn

    host, port = _load_server_config()
    uvicorn.run(app, host=host, port=port, log_level="info")


if __name__ == "__main__":
    start()
