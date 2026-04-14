from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import json
import mimetypes
import time
import uuid
from pathlib import Path
from typing import Any, Callable

from .config import MobilePluginConfig
from .push import create_push_sender
from .http import error_response, json_response, web
from .runtime import HermesProfileRuntime
from .store import MobileAuthStore


_ALLOWED_UPLOAD_CONTENT_TYPES = {
    "application/json",
    "application/pdf",
    "image/gif",
    "image/jpeg",
    "image/png",
    "image/webp",
    "text/markdown",
    "text/plain",
}


def _bearer_token(request: Any) -> str | None:
    headers = getattr(request, "headers", {}) or {}
    auth_header = headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return None
    token = auth_header[7:].strip()
    return token if token else None


def _extract_session_id(request: Any, suffix: str) -> str | None:
    match_info = getattr(request, "match_info", {}) or {}
    if "session_id" in match_info and match_info["session_id"]:
        return str(match_info["session_id"])

    path = str(getattr(request, "path", ""))
    prefix = "/mobile/sessions/"
    if not path.startswith(prefix) or not path.endswith(suffix):
        return None
    raw = path[len(prefix) : len(path) - len(suffix)]
    if not raw or "/" in raw:
        return None
    return raw


def _extract_device_id(request: Any, suffix: str) -> str | None:
    match_info = getattr(request, "match_info", {}) or {}
    if "device_id" in match_info and match_info["device_id"]:
        return str(match_info["device_id"])

    path = str(getattr(request, "path", ""))
    prefix = "/mobile/devices/"
    if not path.startswith(prefix) or not path.endswith(suffix):
        return None
    raw = path[len(prefix) : len(path) - len(suffix)]
    if not raw or "/" in raw:
        return None
    return raw


async def _json_body(request: Any) -> dict:
    try:
        body = await request.json()
    except Exception:
        return {}
    return body if isinstance(body, dict) else {}


def _guess_upload_content_type(filename: str, content_type: str | None) -> str:
    candidate = str(content_type or "").strip()
    if candidate:
        return candidate
    guessed, _ = mimetypes.guess_type(filename)
    return guessed or "application/octet-stream"


def _is_allowed_upload_content_type(content_type: str) -> bool:
    base = content_type.split(";", 1)[0].strip().lower()
    if base in _ALLOWED_UPLOAD_CONTENT_TYPES:
        return True
    if base.startswith("text/"):
        return True
    return False


def _read_upload_field(field: Any) -> bytes:
    upload_file = getattr(field, "file", None)
    if upload_file is not None and getattr(upload_file, "read", None):
        try:
            upload_file.seek(0)
        except Exception:
            pass
        return upload_file.read()
    if hasattr(field, "read") and callable(field.read):
        return field.read()
    raise TypeError("unsupported upload field")


def _extract_attachment_ids(body: dict[str, Any]) -> list[str] | None:
    raw = body.get("attachment_ids")
    if raw is None:
        return []
    if not isinstance(raw, list):
        return None
    attachment_ids: list[str] = []
    for value in raw:
        item = str(value).strip()
        if not item:
            return None
        attachment_ids.append(item)
    return attachment_ids


async def _write_sse_event(response: Any, event: dict) -> None:
    payload = f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
    await response.write(payload.encode("utf-8"))


class MobileRoutes:
    def __init__(
        self,
        config: MobilePluginConfig,
        store: MobileAuthStore,
        push_sender: Any | None = None,
        profile_runtime: HermesProfileRuntime | None = None,
    ):
        self.config = config
        self.store = store
        self.push_sender = push_sender or create_push_sender(config)
        self.profile_runtime = profile_runtime or HermesProfileRuntime(config.hermes_root)
        self._active_runs: dict[tuple[str, str], dict[str, Any]] = {}
        self._shutdown_requested = False

    def register(self, route_registrar: Callable[[str, str, Callable], None]) -> None:
        route_registrar("GET", "/mobile/capabilities", self.capabilities)
        route_registrar("POST", "/mobile/pair/start", self.pair_start)
        route_registrar("POST", "/mobile/pair/complete", self.pair_complete)
        route_registrar("POST", "/mobile/auth/refresh", self.auth_refresh)
        route_registrar("GET", "/mobile/me", self.me)
        route_registrar("GET", "/mobile/sessions", self.sessions_list)
        route_registrar("POST", "/mobile/sessions", self.session_create)
        route_registrar(
            "GET", "/mobile/sessions/{session_id}/messages", self.session_messages_list
        )
        route_registrar(
            "POST", "/mobile/sessions/{session_id}/messages", self.session_messages_send
        )
        route_registrar("POST", "/mobile/sessions/{session_id}/abort", self.session_abort)
        route_registrar("POST", "/mobile/push/register", self.push_register)
        route_registrar("GET", "/mobile/push/diagnostics", self.push_diagnostics)
        route_registrar("GET", "/mobile/devices", self.devices_list)
        route_registrar("POST", "/mobile/devices/{device_id}/revoke", self.device_revoke)
        route_registrar("POST", "/mobile/uploads", self.uploads_create)

    async def capabilities(self, _request: Any):
        return json_response(
            200,
            {
                "ok": True,
                "profile_name": self.config.profile_name,
                "available_profiles": [],
                "profiles_discoverable": False,
                "features": {
                    "pairing": True,
                    "auth_refresh": True,
                    "sessions_list": True,
                    "sessions_create": True,
                    "messages_list": True,
                    "messages_send": True,
                    "session_abort": True,
                    "push_register": True,
                    "push_diagnostics": True,
                    "devices_list": True,
                    "device_revoke": True,
                    "uploads": True,
                },
                "pairing": {
                    "code_format": "XXXX-XXXX",
                    "install_channel": "stable",
                },
                "scope": {
                    "mode": "profile_state_db",
                    "default_profile": self.config.profile_name,
                },
            },
        )

    async def push_register(self, request: Any):
        auth = self._authorize(request)
        if not auth:
            return error_response(401, "unauthorized", "invalid access token")

        body = await _json_body(request)
        push_token = str(body.get("push_token", "")).strip()
        platform = str(body.get("platform", "ios")).strip().lower() or "ios"
        environment = str(body.get("environment", "sandbox")).strip().lower() or "sandbox"
        app_id = str(body.get("app_id", "")).strip() or None
        if not push_token:
            return error_response(400, "bad_request", "push_token is required")
        if len(push_token) > 4096:
            return error_response(400, "bad_request", "push_token is too long")
        if environment not in {"sandbox", "production"}:
            return error_response(400, "bad_request", "environment must be sandbox or production")

        registration = self.store.upsert_push_registration(
            device_id=auth["device_id"],
            profile_name=auth["profile_name"],
            platform=platform,
            environment=environment,
            push_token=push_token,
            app_id=app_id,
        )
        delivery = self.push_sender.diagnostics()
        return json_response(
            200,
            {
                "ok": True,
                "profile_name": auth["profile_name"],
                "registration": registration,
                "delivery": delivery,
            },
        )

    async def push_diagnostics(self, request: Any):
        auth = self._authorize(request)
        if not auth:
            return error_response(401, "unauthorized", "invalid access token")

        diagnostics = self.push_sender.diagnostics()
        summary = self.store.push_delivery_summary(profile_name=auth["profile_name"])
        registrations = self.store.list_devices(profile_name=auth["profile_name"])
        active_targets = self.store.list_active_push_targets(
            profile_name=auth["profile_name"],
            exclude_device_id=auth["device_id"],
        )
        return json_response(
            200,
            {
                "ok": True,
                "profile_name": auth["profile_name"],
                "enabled": diagnostics.get("enabled", False),
                "mode": diagnostics.get("mode", "disabled"),
                "reason": diagnostics.get("reason"),
                "sender": diagnostics,
                "registrations": {
                    "active": len(active_targets),
                    "total": len(registrations),
                },
                "deliveries": summary,
            },
        )

    async def devices_list(self, request: Any):
        auth = self._authorize(request)
        if not auth:
            return error_response(401, "unauthorized", "invalid access token")
        devices = self.store.list_devices(profile_name=auth["profile_name"])
        payload = []
        for device in devices:
            payload.append(
                {
                    "id": device["id"],
                    "device_name": device["device_name"],
                    "created_at": device["created_at"],
                    "last_seen_at": device["last_seen_at"],
                    "revoked_at": device["revoked_at"],
                    "is_current": device["id"] == auth["device_id"],
                    "push": device["push"],
                }
            )
        return json_response(
            200,
            {
                "ok": True,
                "profile_name": auth["profile_name"],
                "devices": payload,
            },
        )

    async def device_revoke(self, request: Any):
        auth = self._authorize(request)
        if not auth:
            return error_response(401, "unauthorized", "invalid access token")
        device_id = _extract_device_id(request, "/revoke")
        if not device_id:
            return error_response(400, "bad_request", "invalid device path")
        revoked = self.store.revoke_device(
            profile_name=auth["profile_name"],
            device_id=device_id,
        )
        if revoked is None:
            return error_response(404, "not_found", "device not found")
        return json_response(
            200,
            {
                "ok": True,
                "revoked": True,
                "device_id": revoked["device_id"],
                "revoked_at": revoked["revoked_at"],
            },
        )

    async def uploads_create(self, request: Any):
        auth = self._authorize(request)
        if not auth:
            return error_response(401, "unauthorized", "invalid access token")
        headers = getattr(request, "headers", {}) or {}
        content_header = str(headers.get("Content-Type", "")).lower()
        filename = ""
        content_type = "application/octet-stream"
        session_id: str | None = None
        raw: bytes

        form_data: dict[str, Any] | None = None
        if "multipart/form-data" in content_header and hasattr(request, "post"):
            try:
                form_data = await request.post()
            except Exception:
                form_data = None

        if form_data is not None:
            session_id = str(form_data.get("session_id", "")).strip() or None
            file_field = form_data.get("file") or form_data.get("attachment") or form_data.get("upload")
            if file_field is None:
                return error_response(400, "bad_request", "file field is required")
            filename = Path(str(getattr(file_field, "filename", "")).strip()).name
            if not filename:
                return error_response(400, "bad_request", "filename is required")
            content_type = _guess_upload_content_type(
                filename,
                getattr(file_field, "content_type", None),
            )
            if not _is_allowed_upload_content_type(content_type):
                return error_response(
                    415,
                    "unsupported_media_type",
                    f"content_type '{content_type}' is not supported",
                )
            try:
                raw = _read_upload_field(file_field)
            except Exception:
                return error_response(400, "bad_request", "invalid upload file field")
            if not raw:
                return error_response(400, "bad_request", "upload content must not be empty")
        else:
            body = await _json_body(request)
            filename = Path(str(body.get("filename", "")).strip()).name
            content_b64 = str(body.get("content_base64", "")).strip()
            content_type = _guess_upload_content_type(
                filename,
                str(body.get("content_type", "")).strip() or None,
            )
            session_id = str(body.get("session_id", "")).strip() or None
            if not filename:
                return error_response(400, "bad_request", "filename is required")
            if not content_b64:
                return error_response(400, "bad_request", "content_base64 is required")
            try:
                raw = base64.b64decode(content_b64, validate=True)
            except (ValueError, binascii.Error):
                return error_response(400, "bad_request", "content_base64 must be valid base64")
            if not raw:
                return error_response(400, "bad_request", "upload content must not be empty")
            if not _is_allowed_upload_content_type(content_type):
                return error_response(
                    415,
                    "unsupported_media_type",
                    f"content_type '{content_type}' is not supported",
                )
        if len(raw) > self.config.upload_max_bytes:
            return error_response(
                413,
                "payload_too_large",
                f"upload exceeds max size of {self.config.upload_max_bytes} bytes",
            )

        upload_id = str(uuid.uuid4())
        safe_name = f"{upload_id}-{filename}"
        stored_path = self.config.upload_dir / safe_name
        stored_path.write_bytes(raw)
        sha256 = hashlib.sha256(raw).hexdigest()
        upload = self.store.create_upload_record(
            upload_id=upload_id,
            profile_name=auth["profile_name"],
            device_id=auth["device_id"],
            session_id=session_id,
            original_filename=filename,
            stored_path=str(stored_path),
            content_type=content_type or "application/octet-stream",
            byte_size=len(raw),
            sha256=sha256,
        )
        return json_response(
            201,
            {
                "ok": True,
                "upload": upload,
                "limits": {
                    "max_bytes": self.config.upload_max_bytes,
                },
            },
        )

    async def pair_start(self, request: Any):
        return error_response(
            403,
            "pairing_code_generation_disabled",
            "pairing codes must be generated from Hermes using the operator install/pair flow",
        )

    async def pair_complete(self, request: Any):
        body = await _json_body(request)
        pairing_code = str(body.get("pairing_code", "")).strip().upper()
        device_name = str(body.get("device_name", "")).strip()
        device_public_key = str(body.get("device_public_key", "")).strip()
        platform = str(body.get("platform", "")).strip().lower()
        app_version_raw = str(body.get("app_version", "")).strip()
        app_version = app_version_raw or None
        if not pairing_code:
            return error_response(400, "bad_request", "pairing_code is required")
        if not device_name:
            return error_response(400, "bad_request", "device_name is required")
        if not device_public_key:
            return error_response(400, "bad_request", "device_public_key is required")
        if not platform:
            return error_response(400, "bad_request", "platform is required")
        payload = self.store.complete_pairing(
            pairing_code=pairing_code,
            device_name=device_name,
            device_public_key=device_public_key,
            platform=platform,
            app_version=app_version,
            access_ttl_seconds=self.config.access_token_ttl_seconds,
            refresh_ttl_seconds=self.config.refresh_token_ttl_seconds,
        )
        if not payload:
            return error_response(404, "not_found", "pairing code not found or expired")
        return json_response(200, {"ok": True, **payload})

    async def auth_refresh(self, request: Any):
        body = await _json_body(request)
        refresh_token = str(body.get("refresh_token", "")).strip()
        if not refresh_token:
            return error_response(400, "bad_request", "refresh_token is required")
        payload = self.store.refresh_tokens(
            refresh_token=refresh_token,
            access_ttl_seconds=self.config.access_token_ttl_seconds,
            refresh_ttl_seconds=self.config.refresh_token_ttl_seconds,
        )
        if not payload:
            return error_response(401, "unauthorized", "invalid refresh token")
        return json_response(200, {"ok": True, **payload})

    async def me(self, request: Any):
        payload = self._authorize(request)
        if not payload:
            return error_response(401, "unauthorized", "invalid access token")
        return json_response(200, {"ok": True, **payload})

    async def sessions_list(self, request: Any):
        auth = self._authorize(request)
        if not auth:
            return error_response(401, "unauthorized", "invalid access token")
        session_db = self.profile_runtime.session_view(auth["profile_name"])
        if session_db is None:
            return error_response(404, "profile_not_found", "profile does not exist")

        sessions = session_db.list_sessions_rich(limit=100, offset=0)
        payload = [self._session_summary(s) for s in sessions]
        return json_response(
            200,
            {
                "ok": True,
                "profile_name": auth["profile_name"],
                "sessions": payload,
            },
        )

    async def session_create(self, request: Any):
        auth = self._authorize(request)
        if not auth:
            return error_response(401, "unauthorized", "invalid access token")
        session_db = self.profile_runtime.session_view(auth["profile_name"])
        if session_db is None:
            return error_response(404, "profile_not_found", "profile does not exist")
        if not hasattr(session_db, "create_session"):
            return error_response(501, "not_supported", "session creation is unavailable")

        body = await _json_body(request)
        title = str(body.get("title", "")).strip()
        source = str(body.get("source", "mobile")).strip() or "mobile"
        requested_id = str(body.get("session_id", "")).strip() or None

        if requested_id is not None and "/" in requested_id:
            return error_response(400, "bad_request", "session_id is invalid")
        if len(title) > 160:
            return error_response(400, "bad_request", "title is too long")
        if len(source) > 64:
            return error_response(400, "bad_request", "source is too long")

        created = session_db.create_session(
            session_id=requested_id,
            source=source,
            title=title or None,
        )
        if created is None:
            if requested_id:
                return error_response(409, "conflict", "session_id already exists")
            return error_response(500, "runtime_error", "failed to create session")
        return json_response(
            201,
            {
                "ok": True,
                "profile_name": auth["profile_name"],
                "session": self._session_summary(created),
            },
        )

    async def session_messages_list(self, request: Any):
        auth = self._authorize(request)
        if not auth:
            return error_response(401, "unauthorized", "invalid access token")
        session_id = _extract_session_id(request, "/messages")
        if not session_id:
            return error_response(400, "bad_request", "invalid session path")
        session_db = self.profile_runtime.session_view(auth["profile_name"])
        if session_db is None:
            return error_response(404, "profile_not_found", "profile does not exist")

        session = session_db.get_session(session_id)
        if not session:
            return error_response(404, "not_found", "session not found")

        messages = session_db.get_messages(session_id)
        payload = [
            {
                "id": str(m.get("id")),
                "role": m.get("role"),
                "content": m.get("content") or "",
                "status": "completed",
                "created_at": m.get("timestamp"),
            }
            for m in messages
        ]
        return json_response(200, {"ok": True, "session_id": session_id, "messages": payload})

    async def session_messages_send(self, request: Any):
        if self._shutdown_requested:
            return error_response(503, "unavailable", "plugin is shutting down")
        auth = self._authorize(request)
        if not auth:
            return error_response(401, "unauthorized", "invalid access token")
        session_db = self.profile_runtime.session_view(auth["profile_name"])
        if session_db is None:
            return error_response(404, "profile_not_found", "profile does not exist")

        session_id = _extract_session_id(request, "/messages")
        if not session_id:
            return error_response(400, "bad_request", "invalid session path")

        body = await _json_body(request)
        client_message_id = str(body.get("client_message_id", "")).strip()
        content = str(body.get("content", "")).strip()
        defer_completion = bool(body.get("defer_completion", False))
        stream_requested = bool(body.get("stream", False))
        attachment_ids = _extract_attachment_ids(body)
        if not client_message_id:
            return error_response(400, "bad_request", "client_message_id is required")
        if not content:
            return error_response(400, "bad_request", "content is required")
        if attachment_ids is None:
            return error_response(400, "bad_request", "attachment_ids must be a list of ids")
        if self.store.resolve_uploads(
            profile_name=auth["profile_name"],
            attachment_ids=attachment_ids,
            session_id=session_id,
        ) is None:
            return error_response(404, "attachment_not_found", "attachment not found")

        payload_hash = self.store.request_payload_hash(
            content=content,
            defer_completion=defer_completion,
            attachment_ids=attachment_ids,
        )
        existing = self.store.get_message_request(
            session_id=session_id,
            device_id=auth["device_id"],
            client_message_id=client_message_id,
        )
        if existing:
            if existing["payload_hash"] != payload_hash:
                return error_response(
                    409,
                    "idempotency_conflict",
                    "client_message_id was already used with different payload",
                )
            replay = existing["response"] or {"ok": True, "session_id": session_id}
            replay["idempotency_replayed"] = True
            replay_status = existing.get("status", "")
            if stream_requested and replay_status not in ("pending", "running", "streaming"):
                return await self._stream_replay_response(request, replay)
            if replay_status in ("pending", "running", "streaming"):
                return json_response(202, replay)
            return json_response(200, replay)

        request_id = str(uuid.uuid4())
        accepted_event = {
            "id": f"{request_id}:1",
            "type": "message.accepted",
            "request_id": request_id,
            "session_id": session_id,
            "created_at": time.time(),
        }
        initial_response = {
            "ok": True,
            "request_id": request_id,
            "session_id": session_id,
            "client_message_id": client_message_id,
            "idempotency_replayed": False,
            "stream": {
                "transport": "sse",
                "done": False,
                "events": [accepted_event],
            },
        }

        pending_status = "streaming" if defer_completion else "running"
        self.store.create_message_request(
            request_id=request_id,
            session_id=session_id,
            device_id=auth["device_id"],
            client_message_id=client_message_id,
            request_payload_hash=payload_hash,
            status=pending_status,
            response=initial_response,
        )

        system_prompt = self._session_system_prompt(session_db, session_id)
        conversation_history = session_db.get_messages_as_conversation(session_id)

        if defer_completion:
            key = (auth["device_id"], session_id)
            task = asyncio.create_task(
                self._run_and_finalize(
                    profile_name=auth["profile_name"],
                    session_id=session_id,
                    user_message=content,
                    conversation_history=conversation_history,
                    request_id=request_id,
                    accepted_event=accepted_event,
                    system_prompt=system_prompt,
                    source_device_id=auth["device_id"],
                )
            )
            self._active_runs[key] = {
                "request_id": request_id,
                "task": task,
            }
            task.add_done_callback(lambda _t, k=key: self._active_runs.pop(k, None))
            return json_response(202, initial_response)

        if stream_requested:
            return await self._stream_live_response(
                request=request,
                profile_name=auth["profile_name"],
                session_id=session_id,
                user_message=content,
                conversation_history=conversation_history,
                request_id=request_id,
                accepted_event=accepted_event,
                system_prompt=system_prompt,
                device_id=auth["device_id"],
            )

        try:
            run_payload = await self._run_agent_once(
                profile_name=auth["profile_name"],
                session_id=session_id,
                user_message=content,
                conversation_history=conversation_history,
                request_id=request_id,
                accepted_event=accepted_event,
                system_prompt=system_prompt,
            )
        except Exception as exc:
            failed_payload = {
                "ok": False,
                "request_id": request_id,
                "session_id": session_id,
                "idempotency_replayed": False,
                "error": {
                    "code": "runtime_error",
                    "message": str(exc),
                },
            }
            self.store.finalize_message_request(
                request_id=request_id,
                status="failed",
                response=failed_payload,
            )
            return json_response(502, failed_payload)

        self.store.finalize_message_request(
            request_id=request_id,
            status="completed",
            response=run_payload,
        )
        await self._dispatch_push_notifications(
            profile_name=auth["profile_name"],
            source_device_id=auth["device_id"],
            session_id=session_id,
            request_id=request_id,
            event_type="message.completed",
        )
        return json_response(202, run_payload)

    async def _stream_replay_response(self, request: Any, replay_payload: dict):
        response = web.StreamResponse(
            status=200,
            headers={
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            },
        )
        await response.prepare(request)
        events = replay_payload.get("stream", {}).get("events", [])
        for event in events:
            await _write_sse_event(response, event)
        await response.write_eof()
        return response

    async def _dispatch_push_notifications(
        self,
        *,
        profile_name: str,
        source_device_id: str,
        session_id: str,
        request_id: str,
        event_type: str,
    ) -> None:
        diagnostics = getattr(self.push_sender, "diagnostics", None)
        if not callable(diagnostics):
            return
        if not diagnostics().get("enabled", False):
            return
        targets = self.store.list_active_push_targets(
            profile_name=profile_name,
            exclude_device_id=source_device_id or None,
        )
        if not targets:
            return

        payload = {
            "aps": {"content-available": 1},
            "session_id": session_id,
            "event_type": event_type,
            "unread_count": 1,
            "title": "New activity",
        }
        for target in targets:
            try:
                result = self.push_sender.send(
                    device_token=target["push_token"],
                    environment=target["environment"],
                    payload=payload,
                    push_type="background",
                    priority=5,
                    profile_name=profile_name,
                    session_id=session_id,
                    request_id=request_id,
                    event_type=event_type,
                    source_device_id=source_device_id,
                    target_device_id=target["device_id"],
                    push_registration_id=target.get("push_registration_id"),
                )
            except Exception as exc:
                result = {
                    "ok": False,
                    "status": "failed",
                    "http_status": None,
                    "apns_id": None,
                    "error_code": str(exc),
                    "response_body": None,
                }
            self.store.record_push_delivery(
                profile_name=profile_name,
                device_id=target["device_id"],
                push_registration_id=target.get("push_registration_id"),
                session_id=session_id,
                request_id=request_id,
                event_type=event_type,
                push_type="background",
                status=str(result.get("status") or ("sent" if result.get("ok") else "failed")),
                http_status=result.get("http_status"),
                apns_id=result.get("apns_id"),
                error_code=result.get("error_code"),
                response_body=result.get("response_body"),
            )

    async def _stream_live_response(
        self,
        *,
        request: Any,
        profile_name: str,
        session_id: str,
        user_message: str,
        conversation_history: list[dict[str, Any]],
        request_id: str,
        accepted_event: dict,
        system_prompt: str | None,
        device_id: str,
    ):
        response = web.StreamResponse(
            status=200,
            headers={
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            },
        )
        await response.prepare(request)
        event_log: list[dict] = [accepted_event]
        await _write_sse_event(response, accepted_event)
        seq = 1
        handle = await self.profile_runtime.start_run(
            profile_name=profile_name,
            session_id=session_id,
            user_message=user_message,
            conversation_history=conversation_history,
            ephemeral_system_prompt=system_prompt,
        )

        key = (device_id, session_id)
        self._active_runs[key] = {
            "request_id": request_id,
            "handle": handle,
            "task": asyncio.current_task(),
        }

        disconnected = False
        completed = False
        try:
            while True:
                try:
                    worker_event = await asyncio.wait_for(handle.read_event(), timeout=15.0)
                except asyncio.TimeoutError:
                    try:
                        await response.write(b": keepalive\n\n")
                    except Exception:
                        disconnected = True
                        break
                    continue
                if worker_event is None:
                    break

                kind = worker_event.get("event")
                if kind == "delta":
                    seq += 1
                    event = {
                        "id": f"{request_id}:{seq}",
                        "type": "message.delta",
                        "request_id": request_id,
                        "session_id": session_id,
                        "delta": str(worker_event.get("delta") or ""),
                        "created_at": time.time(),
                    }
                    event_log.append(event)
                    try:
                        await _write_sse_event(response, event)
                    except Exception:
                        disconnected = True
                        break
                    continue

                if kind == "tool":
                    seq += 1
                    event = {
                        "id": f"{request_id}:{seq}",
                        "type": str(worker_event.get("type") or "tool.progress"),
                        "request_id": request_id,
                        "session_id": session_id,
                        "tool_name": worker_event.get("tool_name"),
                        "preview": worker_event.get("preview"),
                        "args": worker_event.get("args"),
                        "meta": worker_event.get("meta") or {},
                        "created_at": time.time(),
                    }
                    event_log.append(event)
                    try:
                        await _write_sse_event(response, event)
                    except Exception:
                        disconnected = True
                        break
                    continue

                if kind == "failed":
                    seq += 1
                    failed_event = {
                        "id": f"{request_id}:{seq}",
                        "type": "message.failed",
                        "request_id": request_id,
                        "session_id": session_id,
                        "error": {
                            "code": "runtime_error",
                            "message": str(worker_event.get("message") or "worker failed"),
                        },
                        "created_at": time.time(),
                    }
                    event_log.append(failed_event)
                    await _write_sse_event(response, failed_event)
                    self.store.finalize_message_request(
                        request_id=request_id,
                        status="failed",
                        response=self._payload_from_events(
                            request_id=request_id,
                            session_id=session_id,
                            events=event_log,
                            ok=False,
                        ),
                    )
                    return response

                if kind == "completed":
                    final_text = str(worker_event.get("content") or "")
                    if final_text and not any(e.get("type") == "message.delta" for e in event_log):
                        seq += 1
                        delta_event = {
                            "id": f"{request_id}:{seq}",
                            "type": "message.delta",
                            "request_id": request_id,
                            "session_id": session_id,
                            "delta": final_text,
                            "created_at": time.time(),
                        }
                        event_log.append(delta_event)
                        await _write_sse_event(response, delta_event)
                    seq += 1
                    completed_event = {
                        "id": f"{request_id}:{seq}",
                        "type": "message.completed",
                        "request_id": request_id,
                        "session_id": session_id,
                        "content": final_text,
                        "usage": worker_event.get("usage") or {},
                        "created_at": time.time(),
                    }
                    event_log.append(completed_event)
                    await _write_sse_event(response, completed_event)
                    self.store.finalize_message_request(
                        request_id=request_id,
                        status="completed",
                        response=self._payload_from_events(
                            request_id=request_id,
                            session_id=session_id,
                            events=event_log,
                            ok=True,
                        ),
                    )
                    await self._dispatch_push_notifications(
                        profile_name=profile_name,
                        source_device_id=device_id,
                        session_id=session_id,
                        request_id=request_id,
                        event_type="message.completed",
                    )
                    completed = True
                    break

            if disconnected:
                raise ConnectionResetError("client disconnected")

            if completed is False:
                code, stderr = await handle.wait()
                if code != 0:
                    failed_event = {
                        "id": f"{request_id}:{seq + 1}",
                        "type": "message.failed",
                        "request_id": request_id,
                        "session_id": session_id,
                        "error": {
                            "code": "runtime_error",
                            "message": stderr or "worker failed",
                        },
                        "created_at": time.time(),
                    }
                    event_log.append(failed_event)
                    await _write_sse_event(response, failed_event)
                    self.store.finalize_message_request(
                        request_id=request_id,
                        status="failed",
                        response=self._payload_from_events(
                            request_id=request_id,
                            session_id=session_id,
                            events=event_log,
                            ok=False,
                        ),
                    )
        except (ConnectionResetError, asyncio.CancelledError):
            disconnected = True
            handle.abort()

            abort_payload = {
                "ok": True,
                "request_id": request_id,
                "session_id": session_id,
                "idempotency_replayed": False,
                "stream": {
                    "transport": "sse",
                    "done": True,
                    "events": [
                        *event_log,
                        {
                            "id": f"{request_id}:disconnect",
                            "type": "message.aborted",
                            "request_id": request_id,
                            "session_id": session_id,
                            "reason": "client_disconnect",
                            "created_at": time.time(),
                        },
                    ],
                },
            }
            self.store.abort_request(request_id=request_id, response=abort_payload)
        finally:
            self._active_runs.pop(key, None)
            if not disconnected:
                await response.write_eof()

        return response

    async def session_abort(self, request: Any):
        auth = self._authorize(request)
        if not auth:
            return error_response(401, "unauthorized", "invalid access token")
        session_id = _extract_session_id(request, "/abort")
        if not session_id:
            return error_response(400, "bad_request", "invalid session path")
        key = (auth["device_id"], session_id)
        active = self._active_runs.get(key)
        if active is None:
            return json_response(
                409,
                {
                    "ok": False,
                    "aborted": False,
                    "session_id": session_id,
                    "error": {
                        "code": "no_active_stream",
                        "message": "no active stream to abort",
                    },
                },
            )
        handle = active.get("handle")
        if handle is not None:
            handle.abort()
        task = active.get("task")
        if task is None:
            self._active_runs.pop(key, None)
            return json_response(
                409,
                {
                    "ok": False,
                    "aborted": False,
                    "session_id": session_id,
                    "error": {
                        "code": "no_active_stream",
                        "message": "no active stream to abort",
                    },
                },
            )
        if task.done():
            self._active_runs.pop(key, None)
            return json_response(
                409,
                {
                    "ok": False,
                    "aborted": False,
                    "session_id": session_id,
                    "error": {
                        "code": "no_active_stream",
                        "message": "no active stream to abort",
                    },
                },
            )
        if not task.done():
            task.cancel()
        request_id = active["request_id"]
        abort_response = {
            "ok": True,
            "request_id": request_id,
            "session_id": session_id,
            "idempotency_replayed": False,
            "stream": {
                "transport": "sse",
                "done": True,
                "events": [
                    {
                        "id": f"{request_id}:abort",
                        "type": "message.aborted",
                        "request_id": request_id,
                        "session_id": session_id,
                        "created_at": time.time(),
                    }
                ],
            },
        }
        self.store.abort_request(request_id=request_id, response=abort_response)
        self._active_runs.pop(key, None)
        return json_response(
            200,
            {
                "ok": True,
                "aborted": True,
                "session_id": session_id,
                "request_id": request_id,
            },
        )

    def _session_system_prompt(self, session_db: Any, session_id: str) -> str | None:
        session = session_db.get_session(session_id)
        if not session:
            return None
        value = session.get("system_prompt")
        if value is None:
            return None
        return str(value)

    def _payload_from_events(
        self,
        *,
        request_id: str,
        session_id: str,
        events: list[dict[str, Any]],
        ok: bool,
    ) -> dict[str, Any]:
        return {
            "ok": ok,
            "request_id": request_id,
            "session_id": session_id,
            "idempotency_replayed": False,
            "stream": {
                "transport": "sse",
                "done": True,
                "events": events,
            },
        }

    async def _run_and_finalize(
        self,
        *,
        profile_name: str,
        session_id: str,
        user_message: str,
        conversation_history: list[dict[str, Any]],
        request_id: str,
        accepted_event: dict,
        system_prompt: str | None,
        source_device_id: str,
    ) -> None:
        key = (source_device_id, session_id)
        try:
            payload = await self._run_agent_once(
                profile_name=profile_name,
                session_id=session_id,
                user_message=user_message,
                conversation_history=conversation_history,
                request_id=request_id,
                accepted_event=accepted_event,
                system_prompt=system_prompt,
                key=key,
            )
            self.store.finalize_message_request(
                request_id=request_id,
                status="completed",
                response=payload,
            )
            await self._dispatch_push_notifications(
                profile_name=profile_name,
                source_device_id=source_device_id,
                session_id=session_id,
                request_id=request_id,
                event_type="message.completed",
            )
        except asyncio.CancelledError:
            return
        except Exception as exc:
            self.store.finalize_message_request(
                request_id=request_id,
                status="failed",
                response={
                    "ok": False,
                    "request_id": request_id,
                    "session_id": session_id,
                    "error": {
                        "code": "runtime_error",
                        "message": str(exc),
                    },
                },
            )
        finally:
            self._active_runs.pop(key, None)

    async def _run_agent_once(
        self,
        *,
        profile_name: str,
        session_id: str,
        user_message: str,
        conversation_history: list[dict[str, Any]],
        request_id: str,
        accepted_event: dict,
        system_prompt: str | None,
        key: tuple[str, str] | None = None,
    ) -> dict:
        delta_events: list[dict] = []
        tool_events: list[dict] = []
        seq = 1
        handle = await self.profile_runtime.start_run(
            profile_name=profile_name,
            session_id=session_id,
            user_message=user_message,
            conversation_history=conversation_history,
            ephemeral_system_prompt=system_prompt,
        )
        if key is not None:
            existing = self._active_runs.get(key, {})
            existing["handle"] = handle
            self._active_runs[key] = existing

        final_text = ""
        usage: dict[str, Any] = {}
        while True:
            worker_event = await handle.read_event()
            if worker_event is None:
                break
            kind = worker_event.get("event")
            if kind == "delta":
                seq += 1
                delta_events.append(
                    {
                        "id": f"{request_id}:{seq}",
                        "type": "message.delta",
                        "request_id": request_id,
                        "session_id": session_id,
                        "delta": str(worker_event.get("delta") or ""),
                        "created_at": time.time(),
                    }
                )
                continue
            if kind == "tool":
                seq += 1
                tool_events.append(
                    {
                        "id": f"{request_id}:{seq}",
                        "type": str(worker_event.get("type") or "tool.progress"),
                        "request_id": request_id,
                        "session_id": session_id,
                        "tool_name": worker_event.get("tool_name"),
                        "preview": worker_event.get("preview"),
                        "args": worker_event.get("args"),
                        "meta": worker_event.get("meta") or {},
                        "created_at": time.time(),
                    }
                )
                continue
            if kind == "failed":
                raise RuntimeError(str(worker_event.get("message") or "worker failed"))
            if kind == "completed":
                final_text = str(worker_event.get("content") or "")
                usage = worker_event.get("usage") or {}
                break

        code, stderr = await handle.wait()
        if not final_text and code != 0:
            raise RuntimeError(stderr or "worker failed")
        if not delta_events and final_text:
            seq += 1
            delta_events.append(
                {
                    "id": f"{request_id}:{seq}",
                    "type": "message.delta",
                    "request_id": request_id,
                    "session_id": session_id,
                    "delta": final_text,
                    "created_at": time.time(),
                }
            )

        completed_event = {
            "id": f"{request_id}:{seq + 1}",
            "type": "message.completed",
            "request_id": request_id,
            "session_id": session_id,
            "content": final_text,
            "usage": usage or {},
            "created_at": time.time(),
        }
        events = [accepted_event, *tool_events, *delta_events, completed_event]
        return {
            "ok": True,
            "request_id": request_id,
            "session_id": session_id,
            "idempotency_replayed": False,
            "stream": {
                "transport": "sse",
                "done": True,
                "events": events,
            },
        }

    def _authorize(self, request: Any) -> dict | None:
        token = _bearer_token(request)
        if not token:
            return None
        payload = self.store.resolve_access_token(token)
        if not payload:
            return None
        return payload

    def _session_summary(self, session: dict[str, Any]) -> dict[str, Any]:
        preview_text = str(session.get("preview_text") or session.get("preview") or "").strip()
        title = str(session.get("title") or "").strip()
        title_source = str(session.get("title_source") or "").strip()
        if not title:
            title = "New Chat"
            if not title_source:
                title_source = "fallback_new_chat"
        elif not title_source:
            title_source = "metadata"
        return {
            "id": session.get("id"),
            "source": session.get("source") or "mobile",
            "title": title,
            "title_source": title_source,
            "preview_text": preview_text,
            "preview": preview_text,
            "created_at": session.get("started_at"),
            "updated_at": session.get("last_active") or session.get("started_at"),
            "message_count": session.get("message_count", 0),
            "unread_count": int(session.get("unread_count", 0) or 0),
        }

    async def shutdown(self) -> None:
        self._shutdown_requested = True
        active_items = list(self._active_runs.items())

        for (_device_id, session_id), active in active_items:
            task = active.get("task")
            if task is None or task.done():
                continue

            request_id = active["request_id"]
            handle = active.get("handle")
            if handle is not None:
                handle.abort()
            task.cancel()

            self.store.abort_request(
                request_id=request_id,
                response={
                    "ok": True,
                    "request_id": request_id,
                    "session_id": session_id,
                    "idempotency_replayed": False,
                    "stream": {
                        "transport": "sse",
                        "done": True,
                        "events": [
                            {
                                "id": f"{request_id}:shutdown-abort",
                                "type": "message.aborted",
                                "request_id": request_id,
                                "session_id": session_id,
                                "reason": "plugin_shutdown",
                                "created_at": time.time(),
                            }
                        ],
                    },
                },
            )

        tasks = [
            active["task"]
            for _key, active in active_items
            if active.get("task") is not None and not active["task"].done()
        ]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        self._active_runs.clear()
