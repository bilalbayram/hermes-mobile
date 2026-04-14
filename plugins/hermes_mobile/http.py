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

    class _CompatWeb:
        Response = _CompatResponse
        StreamResponse = _CompatStreamResponse

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
