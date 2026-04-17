from __future__ import annotations

import json
from typing import Any

try:
    from aiohttp import web  # type: ignore
except ModuleNotFoundError:
    class _CompatResponse:
        def __init__(
            self,
            *,
            status: int = 200,
            text: str = "",
            content_type: str = "application/json",
            headers: dict[str, str] | None = None,
        ):
            self.status = status
            self.text = text
            self.headers = headers or {}
            self.content_type = content_type
            if "Content-Type" in self.headers:
                self.content_type = self.headers["Content-Type"]

    class _CompatStreamResponse(_CompatResponse):
        def __init__(
            self,
            *,
            status: int = 200,
            headers: dict[str, str] | None = None,
        ):
            super().__init__(status=status, text="", content_type="text/event-stream", headers=headers)

        async def prepare(self, _request: Any) -> "_CompatStreamResponse":
            return self

        async def write(self, data: bytes) -> None:
            chunk = data.decode("utf-8")
            self.text += chunk

        async def write_eof(self) -> None:
            return None

    class _CompatWSMsgType:
        TEXT = "text"
        ERROR = "error"
        CLOSE = "close"
        CLOSED = "closed"
        CLOSING = "closing"

    class _CompatWSMessage:
        def __init__(self, type: str, data: str | None = None):
            self.type = type
            self.data = data

    class _CompatWebSocketResponse(_CompatResponse):
        def __init__(
            self,
            *,
            heartbeat: float | None = None,
            autoping: bool = True,
        ):
            del heartbeat, autoping
            super().__init__(status=101, text="", content_type="application/websocket", headers={})
            self.closed = False
            self.close_code: int | None = None
            self.sent_messages: list[dict[str, Any]] = []
            self._incoming_messages: list[Any] = []

        async def prepare(self, request: Any) -> "_CompatWebSocketResponse":
            self._incoming_messages = list(getattr(request, "ws_messages", []) or [])
            return self

        async def receive(self) -> "_CompatWSMessage":
            if self.closed:
                return _CompatWSMessage(_CompatWSMsgType.CLOSED)
            if self._incoming_messages:
                message = self._incoming_messages.pop(0)
                if isinstance(message, bytes):
                    payload = message.decode("utf-8")
                elif isinstance(message, str):
                    payload = message
                else:
                    payload = json.dumps(message)
                return _CompatWSMessage(_CompatWSMsgType.TEXT, payload)
            self.closed = True
            return _CompatWSMessage(_CompatWSMsgType.CLOSE)

        async def send_json(self, data: dict[str, Any]) -> None:
            self.sent_messages.append(data)
            self.text += json.dumps(data) + "\n"

        async def close(self, *, code: int = 1000, message: bytes = b"") -> None:
            del message
            self.closed = True
            self.close_code = code

    class _CompatWeb:
        Response = _CompatResponse
        StreamResponse = _CompatStreamResponse
        WebSocketResponse = _CompatWebSocketResponse
        WSMsgType = _CompatWSMsgType

        @staticmethod
        def json_response(data: dict, *, status: int = 200):
            return _CompatResponse(status=status, text=json.dumps(data))

    web = _CompatWeb()  # type: ignore


def json_response(status: int, payload: dict):
    return web.json_response(payload, status=status)


def error_response(status: int, code: str, message: str):
    return json_response(
        status=status,
        payload={
            "ok": False,
            "error": {
                "code": code,
                "message": message,
            },
        },
    )


def parse_json_response(response: Any) -> dict:
    text = getattr(response, "text", "")
    return json.loads(text) if text else {}
