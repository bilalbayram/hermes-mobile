from __future__ import annotations

import hashlib
import json
import secrets
import sqlite3
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _hash_payload(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _new_token() -> str:
    return secrets.token_urlsafe(32)


def _new_pairing_code() -> str:
    alphabet = "23456789ABCDEFGHJKMNPQRSTUVWXYZ"
    first = "".join(secrets.choice(alphabet) for _ in range(4))
    second = "".join(secrets.choice(alphabet) for _ in range(4))
    return f"{first}-{second}"


def _safe_json_load(raw: str) -> dict:
    try:
        value = json.loads(raw)
        return value if isinstance(value, dict) else {}
    except Exception:
        return {}


@dataclass(frozen=True)
class SessionTokens:
    access_token: str
    refresh_token: str
    access_expires_at: int
    refresh_expires_at: int


class MobileAuthStore:
    def __init__(
        self,
        conn: sqlite3.Connection,
        clock: Callable[[], float] | None = None,
        token_factory: Callable[[], str] | None = None,
    ):
        self.conn = conn
        self._clock = clock or time.time
        self._token_factory = token_factory or _new_token
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        self._closed = True

    def create_pairing_code(
        self,
        *,
        profile_name: str,
        ttl_seconds: int,
        install_channel: str = "stable",
    ) -> dict:
        if self._closed:
            return {}
        now = int(self._clock())
        code_id = str(uuid.uuid4())
        pairing_code = self._generate_unique_pairing_code()
        expires_at = now + ttl_seconds
        self.conn.execute(
            """
            INSERT INTO mobile_pairing_codes(
                id, pairing_code, profile_name, install_channel, status, issued_at, expires_at, used_at, used_by_device_id
            ) VALUES (?, ?, ?, ?, 'pending', ?, ?, NULL, NULL)
            """,
            (
                code_id,
                pairing_code,
                profile_name,
                install_channel,
                now,
                expires_at,
            ),
        )
        self.conn.commit()
        return {
            "pairing_id": code_id,
            "pairing_code": pairing_code,
            "install_channel": install_channel,
            "profile_name": profile_name,
            "expires_at": expires_at,
        }

    def complete_pairing(
        self,
        pairing_code: str,
        device_name: str,
        device_public_key: str,
        platform: str,
        app_version: str | None,
        access_ttl_seconds: int,
        refresh_ttl_seconds: int,
    ) -> dict | None:
        if self._closed:
            return None
        now = int(self._clock())
        row = self.conn.execute(
            """
            SELECT id, profile_name, install_channel, expires_at, status
            FROM mobile_pairing_codes
            WHERE pairing_code = ?
            """,
            (pairing_code.upper(),),
        ).fetchone()
        if not row:
            return None
        if row["status"] != "pending" or row["expires_at"] < now:
            return None

        device_id = self._upsert_pairing_device(
            profile_name=row["profile_name"],
            device_name=device_name,
            device_public_key=device_public_key,
            platform=platform,
            app_version=app_version,
            now=now,
        )

        tokens = self._issue_tokens(
            device_id=device_id,
            profile_name=row["profile_name"],
            access_ttl_seconds=access_ttl_seconds,
            refresh_ttl_seconds=refresh_ttl_seconds,
        )
        self.conn.execute(
            """
            UPDATE mobile_pairing_codes
            SET status = 'used', used_at = ?, used_by_device_id = ?
            WHERE id = ?
            """,
            (now, device_id, row["id"]),
        )
        self.conn.commit()

        return {
            "device_id": device_id,
            "device_name": device_name,
            "profile_name": row["profile_name"],
            "install_channel": row["install_channel"],
            "platform": platform,
            "app_version": app_version,
            "access_token": tokens.access_token,
            "refresh_token": tokens.refresh_token,
            "access_expires_at": tokens.access_expires_at,
            "refresh_expires_at": tokens.refresh_expires_at,
        }

    def _generate_unique_pairing_code(self) -> str:
        for _ in range(5):
            pairing_code = _new_pairing_code()
            row = self.conn.execute(
                "SELECT 1 FROM mobile_pairing_codes WHERE pairing_code = ?",
                (pairing_code,),
            ).fetchone()
            if not row:
                return pairing_code
        raise RuntimeError("failed to generate unique pairing code")

    def _upsert_pairing_device(
        self,
        *,
        profile_name: str,
        device_name: str,
        device_public_key: str,
        platform: str,
        app_version: str | None,
        now: int,
    ) -> str:
        existing_device = self.conn.execute(
            """
            SELECT id
            FROM mobile_devices
            WHERE profile_name = ? AND device_public_key = ? AND revoked_at IS NULL
            """,
            (profile_name, device_public_key),
        ).fetchone()
        if not existing_device:
            device_id = str(uuid.uuid4())
            self.conn.execute(
                """
                INSERT INTO mobile_devices(
                    id, profile_name, device_name, device_public_key, platform, app_version, created_at, last_seen_at, revoked_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)
                """,
                (
                    device_id,
                    profile_name,
                    device_name,
                    device_public_key,
                    platform,
                    app_version,
                    now,
                    now,
                ),
            )
            return device_id

        device_id = existing_device["id"]
        self.conn.execute(
            """
            UPDATE mobile_devices
            SET device_name = ?, platform = ?, app_version = ?, last_seen_at = ?
            WHERE id = ?
            """,
            (device_name, platform, app_version, now, device_id),
        )
        return device_id

    def refresh_tokens(
        self,
        refresh_token: str,
        access_ttl_seconds: int,
        refresh_ttl_seconds: int,
    ) -> dict | None:
        if self._closed:
            return None
        now = int(self._clock())
        token_hash = _hash_token(refresh_token)
        row = self.conn.execute(
            """
            SELECT id, device_id, profile_name, expires_at, revoked_at
            FROM mobile_refresh_tokens
            WHERE token_hash = ?
            """,
            (token_hash,),
        ).fetchone()
        if not row:
            return None
        if row["revoked_at"] is not None or row["expires_at"] < now:
            return None

        device_row = self.conn.execute(
            """
            SELECT id, device_name, revoked_at
            FROM mobile_devices
            WHERE id = ?
            """,
            (row["device_id"],),
        ).fetchone()
        if not device_row or device_row["revoked_at"] is not None:
            return None

        self.conn.execute(
            "UPDATE mobile_refresh_tokens SET revoked_at = ? WHERE id = ?",
            (now, row["id"]),
        )
        tokens = self._issue_tokens(
            device_id=row["device_id"],
            profile_name=row["profile_name"],
            access_ttl_seconds=access_ttl_seconds,
            refresh_ttl_seconds=refresh_ttl_seconds,
        )
        self.conn.execute(
            "UPDATE mobile_devices SET last_seen_at = ? WHERE id = ?",
            (now, row["device_id"]),
        )
        self.conn.commit()

        return {
            "device_id": row["device_id"],
            "device_name": device_row["device_name"],
            "profile_name": row["profile_name"],
            "access_token": tokens.access_token,
            "refresh_token": tokens.refresh_token,
            "access_expires_at": tokens.access_expires_at,
            "refresh_expires_at": tokens.refresh_expires_at,
        }

    def resolve_access_token(self, access_token: str) -> dict | None:
        if self._closed:
            return None
        now = int(self._clock())
        row = self.conn.execute(
            """
            SELECT t.device_id, t.profile_name, d.device_name, t.expires_at, t.revoked_at, d.revoked_at AS device_revoked_at
            FROM mobile_access_tokens t
            JOIN mobile_devices d ON d.id = t.device_id
            WHERE t.token_hash = ?
            """,
            (_hash_token(access_token),),
        ).fetchone()
        if not row:
            return None
        if row["revoked_at"] is not None or row["expires_at"] < now:
            return None
        if row["device_revoked_at"] is not None:
            return None

        self.conn.execute(
            "UPDATE mobile_devices SET last_seen_at = ? WHERE id = ?",
            (now, row["device_id"]),
        )
        self.conn.commit()
        return {
            "device_id": row["device_id"],
            "device_name": row["device_name"],
            "profile_name": row["profile_name"],
        }

    def get_message_request(
        self,
        *,
        session_id: str,
        device_id: str,
        client_message_id: str,
    ) -> dict:
        if self._closed:
            return {}
        existing = self.conn.execute(
            """
            SELECT id, request_payload_hash, status, response_json
            FROM mobile_message_requests
            WHERE session_id = ? AND device_id = ? AND client_message_id = ?
            """,
            (session_id, device_id, client_message_id),
        ).fetchone()
        if not existing:
            return {}
        return {
            "request_id": existing["id"],
            "payload_hash": existing["request_payload_hash"],
            "status": existing["status"],
            "response": _safe_json_load(existing["response_json"]),
        }

    def create_message_request(
        self,
        *,
        request_id: str,
        session_id: str,
        device_id: str,
        client_message_id: str,
        request_payload_hash: str,
        status: str,
        response: dict,
    ) -> None:
        if self._closed:
            return
        now = int(self._clock())
        try:
            self.conn.execute(
                """
                INSERT INTO mobile_message_requests(
                    id, session_id, device_id, client_message_id, request_payload_hash, status, response_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    request_id,
                    session_id,
                    device_id,
                    client_message_id,
                    request_payload_hash,
                    status,
                    json.dumps(response, sort_keys=True),
                    now,
                    now,
                ),
            )
            self.conn.commit()
        except sqlite3.ProgrammingError:
            return

    def finalize_message_request(
        self,
        *,
        request_id: str,
        status: str,
        response: dict,
    ) -> None:
        if self._closed:
            return
        now = int(self._clock())
        try:
            self.conn.execute(
                """
                UPDATE mobile_message_requests
                SET status = ?, response_json = ?, updated_at = ?
                WHERE id = ?
                """,
                (status, json.dumps(response, sort_keys=True), now, request_id),
            )
            self.conn.commit()
        except sqlite3.ProgrammingError:
            return

    def request_payload_hash(
        self,
        *,
        content: str,
        defer_completion: bool,
        attachment_ids: list[str] | None = None,
    ) -> str:
        normalized_attachments = sorted(
            {str(item) for item in (attachment_ids or []) if str(item)}
        )
        return _hash_payload(
            {
                "attachment_ids": normalized_attachments,
                "content": content,
                "defer_completion": bool(defer_completion),
            }
        )

    def abort_request(self, *, request_id: str, response: dict) -> None:
        if self._closed:
            return
        now = int(self._clock())
        try:
            self.conn.execute(
                """
                UPDATE mobile_message_requests
                SET status = 'aborted', response_json = ?, updated_at = ?
                WHERE id = ?
                """,
                (json.dumps(response, sort_keys=True), now, request_id),
            )
            self.conn.commit()
        except sqlite3.ProgrammingError:
            return

    def upsert_push_registration(
        self,
        *,
        device_id: str,
        profile_name: str,
        platform: str,
        environment: str,
        push_token: str,
        app_id: str | None,
    ) -> dict:
        if self._closed:
            return {}
        now = int(self._clock())
        token_hash = _hash_token(push_token)
        row = self.conn.execute(
            """
            SELECT id FROM mobile_push_registrations
            WHERE device_id = ?
            """,
            (device_id,),
        ).fetchone()
        if row:
            registration_id = row["id"]
            self.conn.execute(
                """
                UPDATE mobile_push_registrations
                SET profile_name = ?, platform = ?, environment = ?, push_token = ?,
                    push_token_hash = ?, app_id = ?, updated_at = ?, revoked_at = NULL
                WHERE id = ?
                """,
                (
                    profile_name,
                    platform,
                    environment,
                    push_token,
                    token_hash,
                    app_id,
                    now,
                    registration_id,
                ),
            )
        else:
            registration_id = str(uuid.uuid4())
            self.conn.execute(
                """
                INSERT INTO mobile_push_registrations(
                    id, device_id, profile_name, platform, environment, push_token,
                    push_token_hash, app_id, created_at, updated_at, revoked_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
                """,
                (
                    registration_id,
                    device_id,
                    profile_name,
                    platform,
                    environment,
                    push_token,
                    token_hash,
                    app_id,
                    now,
                    now,
                ),
            )
        self.conn.commit()
        return {
            "id": registration_id,
            "device_id": device_id,
            "platform": platform,
            "environment": environment,
            "app_id": app_id or "",
            "updated_at": now,
        }

    def list_devices(self, *, profile_name: str) -> list[dict]:
        if self._closed:
            return []
        rows = self.conn.execute(
            """
            SELECT
                d.id,
                d.device_name,
                d.device_public_key,
                d.created_at,
                d.last_seen_at,
                d.revoked_at,
                p.platform,
                p.environment,
                p.app_id,
                p.updated_at AS push_updated_at,
                p.revoked_at AS push_revoked_at
            FROM mobile_devices d
            LEFT JOIN mobile_push_registrations p ON p.device_id = d.id
            WHERE d.profile_name = ?
            ORDER BY d.created_at DESC
            """,
            (profile_name,),
        ).fetchall()
        out: list[dict] = []
        for row in rows:
            out.append(
                {
                    "id": row["id"],
                    "device_name": row["device_name"],
                    "device_public_key": row["device_public_key"],
                    "created_at": row["created_at"],
                    "last_seen_at": row["last_seen_at"],
                    "revoked_at": row["revoked_at"],
                    "push": {
                        "registered": bool(row["platform"]) and row["push_revoked_at"] is None,
                        "platform": row["platform"] or "",
                        "environment": row["environment"] or "",
                        "app_id": row["app_id"] or "",
                        "updated_at": row["push_updated_at"],
                    },
                }
            )
        return out

    def revoke_device(self, *, profile_name: str, device_id: str) -> dict | None:
        if self._closed:
            return None
        row = self.conn.execute(
            """
            SELECT id, revoked_at
            FROM mobile_devices
            WHERE profile_name = ? AND id = ?
            """,
            (profile_name, device_id),
        ).fetchone()
        if not row:
            return None
        now = int(self._clock())
        self.conn.execute(
            """
            UPDATE mobile_devices
            SET revoked_at = COALESCE(revoked_at, ?)
            WHERE id = ?
            """,
            (now, device_id),
        )
        self.conn.execute(
            """
            UPDATE mobile_access_tokens
            SET revoked_at = COALESCE(revoked_at, ?)
            WHERE device_id = ?
            """,
            (now, device_id),
        )
        self.conn.execute(
            """
            UPDATE mobile_refresh_tokens
            SET revoked_at = COALESCE(revoked_at, ?)
            WHERE device_id = ?
            """,
            (now, device_id),
        )
        self.conn.execute(
            """
            UPDATE mobile_push_registrations
            SET revoked_at = COALESCE(revoked_at, ?)
            WHERE device_id = ?
            """,
            (now, device_id),
        )
        self.conn.commit()
        revoked_at = row["revoked_at"] if row["revoked_at"] is not None else now
        return {
            "device_id": device_id,
            "revoked_at": revoked_at,
        }

    def create_upload_record(
        self,
        *,
        upload_id: str,
        profile_name: str,
        device_id: str,
        session_id: str | None,
        original_filename: str,
        stored_path: str,
        content_type: str,
        byte_size: int,
        sha256: str,
    ) -> dict:
        if self._closed:
            return {}
        now = int(self._clock())
        self.conn.execute(
            """
            INSERT INTO mobile_uploads(
                id, profile_name, device_id, session_id, original_filename, stored_path,
                content_type, byte_size, sha256, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                upload_id,
                profile_name,
                device_id,
                session_id,
                original_filename,
                stored_path,
                content_type,
                byte_size,
                sha256,
                now,
            ),
        )
        self.conn.commit()
        return {
            "id": upload_id,
            "profile_name": profile_name,
            "device_id": device_id,
            "session_id": session_id,
            "original_filename": original_filename,
            "stored_path": stored_path,
            "content_type": content_type,
            "byte_size": byte_size,
            "sha256": sha256,
            "created_at": now,
        }

    def resolve_uploads(
        self,
        *,
        profile_name: str,
        attachment_ids: list[str],
        session_id: str | None = None,
    ) -> list[dict] | None:
        if self._closed:
            return None
        if not attachment_ids:
            return []
        placeholders = ",".join("?" for _ in attachment_ids)
        rows = self.conn.execute(
            f"""
            SELECT id, profile_name, device_id, session_id, original_filename, stored_path,
                   content_type, byte_size, sha256, created_at
            FROM mobile_uploads
            WHERE profile_name = ? AND id IN ({placeholders})
            """,
            [profile_name, *attachment_ids],
        ).fetchall()
        if len(rows) != len(set(attachment_ids)):
            return None
        out: list[dict] = []
        by_id = {row["id"]: dict(row) for row in rows}
        for attachment_id in attachment_ids:
            row = by_id.get(attachment_id)
            if row is None:
                return None
            if row.get("session_id") not in (None, "", session_id):
                return None
            out.append(row)
        return out

    def list_active_push_targets(
        self,
        *,
        profile_name: str,
        exclude_device_id: str | None = None,
    ) -> list[dict]:
        if self._closed:
            return []
        params: list[Any] = [profile_name]
        exclude_clause = ""
        if exclude_device_id:
            exclude_clause = "AND d.id != ?"
            params.append(exclude_device_id)
        rows = self.conn.execute(
            f"""
            SELECT
                d.id AS device_id,
                d.device_name,
                p.id AS push_registration_id,
                p.platform,
                p.environment,
                p.push_token,
                p.app_id,
                p.updated_at
            FROM mobile_devices d
            JOIN mobile_push_registrations p ON p.device_id = d.id
            WHERE d.profile_name = ?
              AND d.revoked_at IS NULL
              AND p.revoked_at IS NULL
              AND p.push_token <> ''
              {exclude_clause}
            ORDER BY p.updated_at DESC
            """,
            params,
        ).fetchall()
        return [dict(row) for row in rows]

    def record_push_delivery(
        self,
        *,
        profile_name: str,
        device_id: str,
        push_registration_id: str | None,
        session_id: str,
        request_id: str | None,
        event_type: str,
        push_type: str,
        status: str,
        http_status: int | None,
        apns_id: str | None,
        error_code: str | None,
        response_body: str | dict | None,
    ) -> dict:
        if self._closed:
            return {}
        now = int(self._clock())
        delivery_id = str(uuid.uuid4())
        body = response_body
        if isinstance(body, dict):
            body = json.dumps(body, sort_keys=True)
        self.conn.execute(
            """
            INSERT INTO mobile_push_deliveries(
                id, profile_name, device_id, push_registration_id, session_id,
                request_id, event_type, push_type, status, http_status, apns_id,
                error_code, response_body, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                delivery_id,
                profile_name,
                device_id,
                push_registration_id,
                session_id,
                request_id,
                event_type,
                push_type,
                status,
                http_status,
                apns_id,
                error_code,
                body,
                now,
            ),
        )
        self.conn.commit()
        return {
            "id": delivery_id,
            "profile_name": profile_name,
            "device_id": device_id,
            "push_registration_id": push_registration_id,
            "session_id": session_id,
            "request_id": request_id,
            "event_type": event_type,
            "push_type": push_type,
            "status": status,
            "http_status": http_status,
            "apns_id": apns_id,
            "error_code": error_code,
            "response_body": body,
            "created_at": now,
        }

    def push_delivery_summary(self, *, profile_name: str) -> dict:
        if self._closed:
            return {
                "total": 0,
                "sent": 0,
                "failed": 0,
                "last_delivery_at": None,
                "last_error": None,
            }
        row = self.conn.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN status = 'sent' THEN 1 ELSE 0 END) AS sent,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed,
                MAX(created_at) AS last_delivery_at
            FROM mobile_push_deliveries
            WHERE profile_name = ?
            """,
            (profile_name,),
        ).fetchone()
        last_error = self.conn.execute(
            """
            SELECT status, error_code, response_body, created_at
            FROM mobile_push_deliveries
            WHERE profile_name = ? AND status != 'sent'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (profile_name,),
        ).fetchone()
        return {
            "total": row["total"] if row else 0,
            "sent": row["sent"] if row and row["sent"] is not None else 0,
            "failed": row["failed"] if row and row["failed"] is not None else 0,
            "last_delivery_at": row["last_delivery_at"] if row else None,
            "last_error": dict(last_error) if last_error else None,
        }

    def _issue_tokens(
        self,
        device_id: str,
        profile_name: str,
        access_ttl_seconds: int,
        refresh_ttl_seconds: int,
    ) -> SessionTokens:
        now = int(self._clock())
        access_token = self._token_factory()
        refresh_token = self._token_factory()
        access_expires_at = now + access_ttl_seconds
        refresh_expires_at = now + refresh_ttl_seconds
        self.conn.execute(
            """
            INSERT INTO mobile_access_tokens(
                id, device_id, profile_name, token_hash, issued_at, expires_at, revoked_at
            ) VALUES (?, ?, ?, ?, ?, ?, NULL)
            """,
            (
                str(uuid.uuid4()),
                device_id,
                profile_name,
                _hash_token(access_token),
                now,
                access_expires_at,
            ),
        )
        self.conn.execute(
            """
            INSERT INTO mobile_refresh_tokens(
                id, device_id, profile_name, token_hash, issued_at, expires_at, revoked_at
            ) VALUES (?, ?, ?, ?, ?, ?, NULL)
            """,
            (
                str(uuid.uuid4()),
                device_id,
                profile_name,
                _hash_token(refresh_token),
                now,
                refresh_expires_at,
            ),
        )
        return SessionTokens(
            access_token=access_token,
            refresh_token=refresh_token,
            access_expires_at=access_expires_at,
            refresh_expires_at=refresh_expires_at,
        )
