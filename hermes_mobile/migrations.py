from __future__ import annotations

import sqlite3

MOBILE_SCHEMA_VERSION = 6


def run_migrations(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS mobile_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )
    version = _current_version(conn)
    for target_version, migrate in (
        (1, _migrate_v1),
        (2, _migrate_v2),
        (3, _migrate_v3),
        (4, _migrate_v4),
        (5, _migrate_v5),
        (6, _migrate_v6),
    ):
        if version >= target_version:
            continue
        migrate(conn)
        version = target_version

    _set_version(conn, version)
    conn.commit()


def _current_version(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT value FROM mobile_meta WHERE key = 'schema_version'"
    ).fetchone()
    if not row:
        return 0
    return int(row["value"] if isinstance(row, sqlite3.Row) else row[0])


def _set_version(conn: sqlite3.Connection, version: int) -> None:
    conn.execute(
        """
        INSERT INTO mobile_meta(key, value)
        VALUES ('schema_version', ?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """,
        (str(version),),
    )


def _migrate_v1(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS mobile_pairing_sessions (
            id TEXT PRIMARY KEY,
            pairing_code TEXT NOT NULL UNIQUE,
            device_name TEXT NOT NULL,
            device_public_key TEXT NOT NULL,
            profile_name TEXT NOT NULL,
            status TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            expires_at INTEGER NOT NULL,
            completed_at INTEGER
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS mobile_devices (
            id TEXT PRIMARY KEY,
            profile_name TEXT NOT NULL,
            device_name TEXT NOT NULL,
            device_public_key TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            last_seen_at INTEGER NOT NULL,
            revoked_at INTEGER
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS mobile_access_tokens (
            id TEXT PRIMARY KEY,
            device_id TEXT NOT NULL,
            profile_name TEXT NOT NULL,
            token_hash TEXT NOT NULL UNIQUE,
            issued_at INTEGER NOT NULL,
            expires_at INTEGER NOT NULL,
            revoked_at INTEGER,
            FOREIGN KEY(device_id) REFERENCES mobile_devices(id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS mobile_refresh_tokens (
            id TEXT PRIMARY KEY,
            device_id TEXT NOT NULL,
            profile_name TEXT NOT NULL,
            token_hash TEXT NOT NULL UNIQUE,
            issued_at INTEGER NOT NULL,
            expires_at INTEGER NOT NULL,
            revoked_at INTEGER,
            FOREIGN KEY(device_id) REFERENCES mobile_devices(id)
        )
        """
    )


def _migrate_v2(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS mobile_message_requests (
            id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            device_id TEXT NOT NULL,
            client_message_id TEXT NOT NULL,
            request_payload_hash TEXT NOT NULL,
            status TEXT NOT NULL,
            response_json TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            UNIQUE(session_id, device_id, client_message_id),
            FOREIGN KEY(device_id) REFERENCES mobile_devices(id)
        )
        """
    )


def _migrate_v3(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS mobile_push_registrations (
            id TEXT PRIMARY KEY,
            device_id TEXT NOT NULL UNIQUE,
            profile_name TEXT NOT NULL,
            platform TEXT NOT NULL,
            environment TEXT NOT NULL,
            push_token TEXT NOT NULL,
            push_token_hash TEXT NOT NULL,
            app_id TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            revoked_at INTEGER,
            FOREIGN KEY(device_id) REFERENCES mobile_devices(id)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_mobile_push_profile
        ON mobile_push_registrations(profile_name, updated_at DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_mobile_push_token_hash
        ON mobile_push_registrations(push_token_hash)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS mobile_uploads (
            id TEXT PRIMARY KEY,
            profile_name TEXT NOT NULL,
            device_id TEXT NOT NULL,
            original_filename TEXT NOT NULL,
            stored_path TEXT NOT NULL,
            content_type TEXT NOT NULL,
            byte_size INTEGER NOT NULL,
            sha256 TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            FOREIGN KEY(device_id) REFERENCES mobile_devices(id)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_mobile_uploads_profile_created
        ON mobile_uploads(profile_name, created_at DESC)
        """
    )


def _ensure_column(
    conn: sqlite3.Connection,
    table: str,
    column: str,
    definition: str,
) -> None:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    existing = {row[1] for row in rows}
    if column in existing:
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {definition}")


def _migrate_v4(conn: sqlite3.Connection) -> None:
    _ensure_column(conn, "mobile_uploads", "session_id", "session_id TEXT")
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_mobile_uploads_session_created
        ON mobile_uploads(session_id, created_at DESC)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS mobile_push_deliveries (
            id TEXT PRIMARY KEY,
            profile_name TEXT NOT NULL,
            device_id TEXT NOT NULL,
            push_registration_id TEXT,
            session_id TEXT NOT NULL,
            request_id TEXT,
            event_type TEXT NOT NULL,
            push_type TEXT NOT NULL,
            status TEXT NOT NULL,
            http_status INTEGER,
            apns_id TEXT,
            error_code TEXT,
            response_body TEXT,
            created_at INTEGER NOT NULL,
            FOREIGN KEY(device_id) REFERENCES mobile_devices(id)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_mobile_push_deliveries_profile_created
        ON mobile_push_deliveries(profile_name, created_at DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_mobile_push_deliveries_device_created
        ON mobile_push_deliveries(device_id, created_at DESC)
        """
    )


def _migrate_v5(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS mobile_pairing_codes (
            id TEXT PRIMARY KEY,
            pairing_code TEXT NOT NULL UNIQUE,
            profile_name TEXT NOT NULL,
            install_channel TEXT NOT NULL,
            status TEXT NOT NULL,
            issued_at INTEGER NOT NULL,
            expires_at INTEGER NOT NULL,
            used_at INTEGER,
            used_by_device_id TEXT,
            FOREIGN KEY(used_by_device_id) REFERENCES mobile_devices(id)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_mobile_pairing_codes_profile_issued
        ON mobile_pairing_codes(profile_name, issued_at DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_mobile_pairing_codes_status_expires
        ON mobile_pairing_codes(status, expires_at DESC)
        """
    )
    _ensure_column(conn, "mobile_devices", "platform", "platform TEXT")
    _ensure_column(conn, "mobile_devices", "app_version", "app_version TEXT")


def _migrate_v6(conn: sqlite3.Connection) -> None:
    _ensure_column(conn, "mobile_message_requests", "profile_name", "profile_name TEXT")
    conn.execute(
        """
        UPDATE mobile_message_requests
        SET profile_name = (
            SELECT d.profile_name
            FROM mobile_devices d
            WHERE d.id = mobile_message_requests.device_id
        )
        WHERE profile_name IS NULL OR TRIM(profile_name) = ''
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_mobile_message_requests_profile_session_updated
        ON mobile_message_requests(profile_name, session_id, updated_at DESC)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS mobile_notification_policies (
            profile_name TEXT NOT NULL,
            device_scope TEXT NOT NULL,
            event_type TEXT NOT NULL,
            delivery_mode TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY(profile_name, device_scope, event_type)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_mobile_notification_policies_profile_updated
        ON mobile_notification_policies(profile_name, updated_at DESC)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS mobile_inbox_items (
            id TEXT PRIMARY KEY,
            profile_name TEXT NOT NULL,
            device_scope TEXT NOT NULL,
            session_id TEXT,
            kind TEXT NOT NULL,
            title TEXT NOT NULL,
            body TEXT NOT NULL,
            body_preview TEXT NOT NULL,
            deep_link_target TEXT,
            created_at INTEGER NOT NULL,
            read_at INTEGER,
            archived_at INTEGER
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_mobile_inbox_items_profile_created
        ON mobile_inbox_items(profile_name, created_at DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_mobile_inbox_items_device_created
        ON mobile_inbox_items(profile_name, device_scope, created_at DESC)
        """
    )
