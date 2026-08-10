from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from aiohttp import WSMsgType, web

log = logging.getLogger("funfernus-realtime")


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _env_bool(name: str, default: bool = False) -> bool:
    raw = _env(name)
    if not raw:
        return default
    return raw.lower() in {"1", "true", "yes", "on", "да"}


def _env_int(name: str, default: int) -> int:
    raw = _env(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} должен быть числом, сейчас: {raw!r}") from exc


def _env_float(name: str, default: float) -> float:
    raw = _env(name)
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} должен быть числом, сейчас: {raw!r}") from exc


def _split_csv(value: str) -> tuple[str, ...]:
    return tuple(part.strip().rstrip("/") for part in value.split(",") if part.strip())


def _base64url_decode(value: str) -> bytes:
    value = value.replace("-", "+").replace("_", "/")
    value += "=" * ((4 - len(value) % 4) % 4)
    return base64.b64decode(value, validate=True)


@dataclass(frozen=True)
class RealtimeConfig:
    enabled: bool
    host: str
    port: int
    path: str
    secret: str
    allowed_origins: tuple[str, ...]
    max_connections_per_user: int
    heartbeat_seconds: float
    public_url: str

    @classmethod
    def from_env(cls) -> "RealtimeConfig":
        # PaaS-хостинги (включая Bothost) обычно сами назначают web-порт
        # через переменную PORT. Она имеет приоритет над REALTIME_PORT.
        platform_port = _env("PORT")
        if platform_port:
            try:
                resolved_port = int(platform_port)
            except ValueError as exc:
                raise RuntimeError(f"PORT должен быть числом, сейчас: {platform_port!r}") from exc
        else:
            resolved_port = _env_int("REALTIME_PORT", 8000)

        path = _env("REALTIME_PATH", "/ws") or "/ws"
        if not path.startswith("/"):
            path = "/" + path
        return cls(
            enabled=_env_bool("REALTIME_ENABLED", False),
            # 0.0.0.0 нужен для reverse proxy/container hosting.
            # На VPS при желании можно явно указать 127.0.0.1 в .env.
            host=_env("REALTIME_HOST", "0.0.0.0") or "0.0.0.0",
            port=resolved_port,
            path=path,
            secret=_env("REALTIME_SECRET"),
            allowed_origins=_split_csv(_env("REALTIME_ALLOWED_ORIGINS")),
            max_connections_per_user=max(1, _env_int("REALTIME_MAX_CONNECTIONS_PER_USER", 8)),
            heartbeat_seconds=max(10.0, _env_float("REALTIME_HEARTBEAT_SECONDS", 25.0)),
            public_url=_env("REALTIME_PUBLIC_URL"),
        )

    def validate(self) -> None:
        if not self.enabled:
            return
        if not (1 <= self.port <= 65535):
            raise RuntimeError("REALTIME_PORT должен быть в диапазоне 1..65535")
        if len(self.secret.encode("utf-8")) < 32:
            raise RuntimeError("REALTIME_SECRET должен содержать минимум 32 символа")
        if not self.path.startswith("/"):
            raise RuntimeError("REALTIME_PATH должен начинаться с /")


@dataclass
class Ticket:
    user_id: int
    exp: int
    nonce: str


class RealtimeConnection:
    __slots__ = ("ws", "user_id", "ticket_exp", "connected_at", "send_lock")

    def __init__(self, ws: web.WebSocketResponse, user_id: int, ticket_exp: int) -> None:
        self.ws = ws
        self.user_id = user_id
        self.ticket_exp = ticket_exp
        self.connected_at = time.monotonic()
        self.send_lock = asyncio.Lock()

    async def send_json(self, payload: dict[str, Any]) -> bool:
        if self.ws.closed:
            return False
        try:
            async with self.send_lock:
                await self.ws.send_json(payload, dumps=lambda obj: json.dumps(obj, ensure_ascii=False, separators=(",", ":")))
            return True
        except (ConnectionResetError, RuntimeError, asyncio.CancelledError):
            return False
        except Exception:
            log.debug("Не удалось отправить realtime-пакет user_id=%s", self.user_id, exc_info=True)
            return False


class FunFernusRealtimeServer:
    """Realtime transport embedded into the existing Discord bot process.

    Persistent messenger state is NOT mutated from the browser WebSocket.
    PHP/API remains the source of truth and publishes committed events via
    POST /internal/publish. This keeps messenger permissions in one place and
    makes this service a small, auditable fan-out layer.
    """

    MAX_INTERNAL_BODY = 512 * 1024
    MAX_BATCH_DELIVERIES = 5000

    def __init__(self, config: RealtimeConfig | None = None) -> None:
        self.config = config or RealtimeConfig.from_env()
        self.config.validate()
        self._connections_by_user: dict[int, list[RealtimeConnection]] = defaultdict(list)
        self._app: web.Application | None = None
        self._runner: web.AppRunner | None = None
        self._site: web.BaseSite | None = None
        self._started = False
        self._stopping = False

    @property
    def enabled(self) -> bool:
        return self.config.enabled

    @property
    def started(self) -> bool:
        return self._started

    def snapshot(self) -> dict[str, Any]:
        users = 0
        sockets = 0
        for uid in list(self._connections_by_user):
            alive = [c for c in self._connections_by_user[uid] if not c.ws.closed]
            if alive:
                users += 1
                sockets += len(alive)
        return {
            "enabled": self.enabled,
            "started": self.started,
            "host": self.config.host,
            "port": self.config.port,
            "path": self.config.path,
            "public_url": self.config.public_url,
            "users_online": users,
            "sockets": sockets,
            "allowed_origins": list(self.config.allowed_origins),
        }

    def _verify_ticket(self, raw_ticket: str) -> Ticket | None:
        try:
            body, sig_text = raw_ticket.split(".", 1)
        except ValueError:
            return None
        try:
            expected = hmac.new(self.config.secret.encode("utf-8"), body.encode("ascii"), hashlib.sha256).digest()
            supplied = _base64url_decode(sig_text)
        except Exception:
            return None
        if not hmac.compare_digest(expected, supplied):
            return None
        try:
            payload = json.loads(_base64url_decode(body).decode("utf-8"))
        except (ValueError, UnicodeDecodeError, json.JSONDecodeError):
            return None
        if not isinstance(payload, dict):
            return None
        now = int(time.time())
        try:
            version = int(payload.get("v", 0))
            uid = int(payload.get("uid", 0))
            iat = int(payload.get("iat", 0))
            exp = int(payload.get("exp", 0))
        except (TypeError, ValueError):
            return None
        if version != 1 or uid <= 0:
            return None
        if exp < now - 5 or iat > now + 30:
            return None
        return Ticket(user_id=uid, exp=exp, nonce=str(payload.get("nonce") or ""))

    def _origin_allowed(self, request: web.Request) -> bool:
        if not self.config.allowed_origins:
            return True
        origin = (request.headers.get("Origin") or "").rstrip("/")
        return origin in self.config.allowed_origins

    def _internal_authorized(self, request: web.Request) -> bool:
        supplied = request.headers.get("X-FunFernus-Realtime", "")
        if not supplied:
            return False
        try:
            return hmac.compare_digest(supplied.encode("utf-8"), self.config.secret.encode("utf-8"))
        except Exception:
            return False

    async def _read_json(self, request: web.Request) -> dict[str, Any]:
        if request.content_length is not None and request.content_length > self.MAX_INTERNAL_BODY:
            raise web.HTTPRequestEntityTooLarge(max_size=self.MAX_INTERNAL_BODY, actual_size=request.content_length)
        try:
            payload = await request.json(loads=json.loads)
        except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as exc:
            raise web.HTTPBadRequest(text=json.dumps({"ok": False, "error": "invalid_json"}), content_type="application/json") from exc
        if not isinstance(payload, dict):
            raise web.HTTPBadRequest(text=json.dumps({"ok": False, "error": "invalid_json"}), content_type="application/json")
        return payload

    async def _health(self, request: web.Request) -> web.Response:
        snap = self.snapshot()
        return web.json_response(
            {
                "ok": True,
                "service": "FunFernus Realtime / Discord Bot",
                "websocket_path": self.config.path,
                "users_online": snap["users_online"],
                "sockets": snap["sockets"],
                "discord_bot_embedded": True,
                "now": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            },
            headers={"Cache-Control": "no-store"},
        )

    async def _register(self, connection: RealtimeConnection) -> None:
        bucket = self._connections_by_user[connection.user_id]
        bucket[:] = [item for item in bucket if not item.ws.closed]
        while len(bucket) >= self.config.max_connections_per_user:
            oldest = min(bucket, key=lambda item: item.connected_at)
            bucket.remove(oldest)
            try:
                await oldest.ws.close(code=4008, message=b"Too many sessions")
            except Exception:
                pass
        bucket.append(connection)

    def _unregister(self, connection: RealtimeConnection) -> None:
        bucket = self._connections_by_user.get(connection.user_id)
        if not bucket:
            return
        try:
            bucket.remove(connection)
        except ValueError:
            pass
        bucket[:] = [item for item in bucket if not item.ws.closed]
        if not bucket:
            self._connections_by_user.pop(connection.user_id, None)

    async def _websocket(self, request: web.Request) -> web.StreamResponse:
        if not self._origin_allowed(request):
            raise web.HTTPForbidden(text="Forbidden origin")
        ticket = self._verify_ticket(request.query.get("ticket", ""))
        if ticket is None:
            raise web.HTTPUnauthorized(text="Invalid realtime ticket")

        ws = web.WebSocketResponse(
            heartbeat=self.config.heartbeat_seconds,
            autoping=True,
            autoclose=True,
            max_msg_size=64 * 1024,
            compress=False,
        )
        await ws.prepare(request)
        connection = RealtimeConnection(ws, ticket.user_id, ticket.exp)
        await self._register(connection)
        await connection.send_json(
            {
                "type": "hello",
                "user_id": ticket.user_id,
                "server_time": int(time.time() * 1000),
                "ticket_expires_at": ticket.exp * 1000,
                "transport": "python-discord-bot",
            }
        )
        log.info("Realtime connected: user_id=%s sockets=%s", ticket.user_id, self.snapshot()["sockets"])

        try:
            async for msg in ws:
                if msg.type == WSMsgType.TEXT:
                    # Browser WebSocket cannot directly mutate messenger state.
                    # Only application ping is accepted; all state-changing actions
                    # continue to go through authenticated PHP API endpoints.
                    try:
                        data = json.loads(msg.data)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(data, dict) and data.get("type") == "ping":
                        await connection.send_json({"type": "pong", "server_time": int(time.time() * 1000)})
                elif msg.type in {WSMsgType.ERROR, WSMsgType.CLOSE, WSMsgType.CLOSED}:
                    break
        finally:
            self._unregister(connection)
            log.info("Realtime disconnected: user_id=%s sockets=%s", ticket.user_id, self.snapshot()["sockets"])
        return ws

    async def _send_to_user(self, user_id: int, packet: dict[str, Any]) -> int:
        if user_id <= 0:
            return 0
        bucket = self._connections_by_user.get(user_id, [])
        if not bucket:
            return 0
        delivered = 0
        dead: list[RealtimeConnection] = []
        # Copy the list because cleanup may happen while a send is awaited.
        for connection in list(bucket):
            if connection.ws.closed:
                dead.append(connection)
                continue
            if await connection.send_json(packet):
                delivered += 1
            else:
                dead.append(connection)
        for connection in dead:
            self._unregister(connection)
        return delivered

    async def _fanout(self, recipients: Any, packet: dict[str, Any]) -> int:
        if not isinstance(recipients, list):
            return 0
        unique: set[int] = set()
        for raw in recipients:
            try:
                uid = int(raw)
            except (TypeError, ValueError):
                continue
            if uid > 0:
                unique.add(uid)
        delivered = 0
        for uid in unique:
            delivered += await self._send_to_user(uid, packet)
        return delivered

    async def _internal_publish(self, request: web.Request) -> web.Response:
        if not self._internal_authorized(request):
            return web.json_response({"ok": False, "error": "forbidden"}, status=403, headers={"Cache-Control": "no-store"})

        try:
            payload = await self._read_json(request)
        except web.HTTPException:
            raise
        kind = str(payload.get("kind") or "")
        now_ms = int(time.time() * 1000)

        if kind == "messenger.batch":
            deliveries = payload.get("deliveries")
            if not isinstance(deliveries, list) or not deliveries or len(deliveries) > self.MAX_BATCH_DELIVERIES:
                return web.json_response({"ok": False, "error": "invalid_deliveries"}, status=422)
            delivered_sockets = 0
            for delivery in deliveries:
                if not isinstance(delivery, dict):
                    continue
                try:
                    user_id = int(delivery.get("user_id") or 0)
                except (TypeError, ValueError):
                    continue
                packet = delivery.get("packet")
                if user_id <= 0 or not isinstance(packet, dict):
                    continue
                if str(packet.get("type") or "") not in {"messenger.event", "notification"}:
                    continue
                outgoing = dict(packet)
                outgoing["server_time"] = now_ms
                delivered_sockets += await self._send_to_user(user_id, outgoing)
            return web.json_response({"ok": True, "delivered_sockets": delivered_sockets}, headers={"Cache-Control": "no-store"})

        if kind not in {"messenger.event", "messenger.typing", "notification"}:
            return web.json_response({"ok": False, "error": "invalid_kind"}, status=422)

        packet: dict[str, Any] = {"type": kind, "server_time": now_ms}
        if kind == "messenger.event":
            packet["event"] = payload.get("event")
        elif kind == "messenger.typing":
            try:
                packet.update(
                    {
                        "conversation_id": int(payload.get("conversation_id") or 0),
                        "actor_user_id": int(payload.get("actor_user_id") or 0),
                        "actor_name": str(payload.get("actor_name") or ""),
                        "typing": bool(payload.get("typing")),
                        "expires_in_ms": int(payload.get("expires_in_ms") or 0),
                    }
                )
            except (TypeError, ValueError):
                return web.json_response({"ok": False, "error": "invalid_payload"}, status=422)
        else:
            packet["notification"] = payload.get("notification")

        delivered = await self._fanout(payload.get("recipients"), packet)
        return web.json_response({"ok": True, "delivered_sockets": delivered}, headers={"Cache-Control": "no-store"})

    async def start(self) -> "FunFernusRealtimeServer":
        if not self.enabled:
            log.info("Realtime отключён (REALTIME_ENABLED=false)")
            return self
        if self._started:
            return self

        app = web.Application(client_max_size=self.MAX_INTERNAL_BODY)
        # Root endpoint удобен для health-check домена на PaaS-хостинге.
        app.router.add_get("/", self._health)
        app.router.add_get("/health", self._health)
        app.router.add_get(self.config.path, self._websocket)
        app.router.add_post("/internal/publish", self._internal_publish)
        runner = web.AppRunner(app, access_log=None)
        await runner.setup()
        site = web.TCPSite(runner, host=self.config.host, port=self.config.port, reuse_address=True)
        try:
            await site.start()
        except Exception:
            await runner.cleanup()
            raise

        self._app = app
        self._runner = runner
        self._site = site
        self._started = True
        log.info(
            "FunFernus Realtime запущен внутри Discord-бота: http://%s:%s%s | origins=%s",
            self.config.host,
            self.config.port,
            self.config.path,
            ", ".join(self.config.allowed_origins) or "ANY (не рекомендуется для production)",
        )
        return self

    async def stop(self) -> None:
        if self._stopping:
            return
        self._stopping = True
        try:
            connections = [item for bucket in self._connections_by_user.values() for item in bucket]
            for connection in connections:
                if not connection.ws.closed:
                    try:
                        await connection.ws.close(code=1012, message=b"Server restart")
                    except Exception:
                        pass
            self._connections_by_user.clear()
            if self._runner is not None:
                await self._runner.cleanup()
        finally:
            self._app = None
            self._runner = None
            self._site = None
            self._started = False
            self._stopping = False
            log.info("FunFernus Realtime остановлен")


async def setup_realtime_server() -> FunFernusRealtimeServer:
    server = FunFernusRealtimeServer()
    await server.start()
    return server


def validate_realtime_settings() -> None:
    RealtimeConfig.from_env().validate()
