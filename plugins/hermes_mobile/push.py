from __future__ import annotations

import base64
import json
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _json_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")


@dataclass(frozen=True)
class PushDiagnostics:
    enabled: bool
    mode: str
    reason: str | None = None
    provider: str = "noop"

    def as_dict(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "mode": self.mode,
            "reason": self.reason,
            "provider": self.provider,
        }


class NoopPushSender:
    def __init__(self, reason: str = "missing_credentials"):
        self._reason = reason

    def diagnostics(self) -> dict[str, Any]:
        return PushDiagnostics(
            enabled=False,
            mode="disabled",
            reason=self._reason,
            provider="noop",
        ).as_dict()

    def send(self, **kwargs: Any) -> dict[str, Any]:
        del kwargs
        return {
            "ok": False,
            "status": "disabled",
            "enabled": False,
            "mode": "disabled",
            "reason": self._reason,
        }


class APNsPushSender:
    def __init__(
        self,
        *,
        topic: str,
        team_id: str,
        key_id: str,
        auth_key_path: Path,
    ):
        self.topic = topic
        self.team_id = team_id
        self.key_id = key_id
        self.auth_key_path = auth_key_path

    def diagnostics(self) -> dict[str, Any]:
        if shutil.which("curl") is None:
            return PushDiagnostics(
                enabled=False,
                mode="disabled",
                reason="missing_curl",
                provider="apns",
            ).as_dict()
        return PushDiagnostics(
            enabled=True,
            mode="apns",
            reason=None,
            provider="apns",
        ).as_dict()

    def _jwt(self, now: int | None = None) -> str:
        try:
            import jwt  # type: ignore
        except ModuleNotFoundError as exc:  # pragma: no cover - runtime dependency check
            raise RuntimeError("PyJWT is required for APNs push delivery") from exc

        now = int(now or time.time())
        token = jwt.encode(
            {"iss": self.team_id, "iat": now},
            self.auth_key_path.read_text(encoding="utf-8"),
            algorithm="ES256",
            headers={"kid": self.key_id},
        )
        return str(token)

    def send(
        self,
        *,
        device_token: str,
        environment: str,
        payload: dict[str, Any],
        push_type: str = "background",
        priority: int = 5,
        **context: Any,
    ) -> dict[str, Any]:
        del context
        if shutil.which("curl") is None:
            return {
                "ok": False,
                "status": "disabled",
                "enabled": False,
                "mode": "disabled",
                "reason": "missing_curl",
            }
        base_url = "https://api.sandbox.push.apple.com" if environment == "sandbox" else "https://api.push.apple.com"
        jwt = self._jwt()
        body = _json_bytes(payload)
        proc = subprocess.run(
            [
                "curl",
                "--silent",
                "--show-error",
                "--http2",
                "--include",
                "--request",
                "POST",
                "--data-binary",
                "@-",
                "--header",
                f"authorization: bearer {jwt}",
                "--header",
                f"apns-topic: {self.topic}",
                "--header",
                f"apns-push-type: {push_type}",
                "--header",
                f"apns-priority: {priority}",
                "--header",
                "apns-expiration: 0",
                "--write-out",
                "\n__HTTP_STATUS__:%{http_code}",
                f"{base_url}/3/device/{device_token}",
            ],
            input=body,
            capture_output=True,
            check=False,
        )
        stdout = proc.stdout.decode("utf-8", "replace")
        stderr = proc.stderr.decode("utf-8", "replace").strip()
        raw_response = stdout
        http_status = None
        if "\n__HTTP_STATUS__:" in stdout:
            raw_response, http_code = stdout.rsplit("\n__HTTP_STATUS__:", 1)
            try:
                http_status = int(http_code.strip())
            except ValueError:
                http_status = None

        header_text, response_body = _split_http_response(raw_response)
        response_headers = _parse_headers(header_text)
        status = "sent" if http_status and 200 <= http_status < 300 else "failed"
        error_code = None
        apns_id = response_headers.get("apns-id")
        if response_body.strip():
            try:
                response_json = json.loads(response_body)
                if isinstance(response_json, dict):
                    error_code = response_json.get("reason")
            except Exception:
                pass
        if status == "failed" and not error_code and stderr:
            error_code = stderr[:200]
        return {
            "ok": status == "sent",
            "status": status,
            "http_status": http_status,
            "apns_id": apns_id,
            "error_code": error_code,
            "response_body": response_body,
            "stderr": stderr,
        }


def _split_http_response(raw: str) -> tuple[str, str]:
    if "\r\n\r\n" in raw:
        return raw.split("\r\n\r\n", 1)
    if "\n\n" in raw:
        return raw.split("\n\n", 1)
    return "", raw


def _parse_headers(raw_headers: str) -> dict[str, str]:
    headers: dict[str, str] = {}
    for line in raw_headers.splitlines():
        if ":" not in line:
            continue
        name, value = line.split(":", 1)
        headers[name.strip().lower()] = value.strip()
    return headers


def create_push_sender(config: Any) -> Any:
    if not getattr(config, "push_enabled", False):
        return NoopPushSender()
    auth_key_path = getattr(config, "apns_auth_key_path", None)
    topic = str(getattr(config, "apns_topic", "")).strip()
    team_id = str(getattr(config, "apns_team_id", "")).strip()
    key_id = str(getattr(config, "apns_key_id", "")).strip()
    if not auth_key_path or not topic or not team_id or not key_id:
        return NoopPushSender()
    return APNsPushSender(
        topic=topic,
        team_id=team_id,
        key_id=key_id,
        auth_key_path=Path(auth_key_path),
    )
