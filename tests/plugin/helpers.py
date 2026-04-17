from __future__ import annotations

import asyncio
import io
import os
import sqlite3
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from hermes_mobile.http import parse_json_response


def _normalize_text(value: str) -> str:
    if not value:
        return ""
    return " ".join(value.replace("\r", " ").replace("\n", " ").split()).strip()


def _truncate_text(value: str, limit: int) -> str:
    normalized = _normalize_text(value)
    if len(normalized) <= limit:
        return normalized
    return normalized[: max(0, limit - 3)].rstrip() + "..."


def create_hermes_state_db(path: str) -> None:
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_version (
            version INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sessions (
            id TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            user_id TEXT,
            model TEXT,
            model_config TEXT,
            system_prompt TEXT,
            parent_session_id TEXT,
            started_at REAL NOT NULL,
            ended_at REAL,
            end_reason TEXT,
            message_count INTEGER DEFAULT 0,
            tool_call_count INTEGER DEFAULT 0,
            input_tokens INTEGER DEFAULT 0,
            output_tokens INTEGER DEFAULT 0,
            cache_read_tokens INTEGER DEFAULT 0,
            cache_write_tokens INTEGER DEFAULT 0,
            reasoning_tokens INTEGER DEFAULT 0,
            billing_provider TEXT,
            billing_base_url TEXT,
            billing_mode TEXT,
            estimated_cost_usd REAL,
            actual_cost_usd REAL,
            cost_status TEXT,
            cost_source TEXT,
            pricing_version TEXT,
            title TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            role TEXT NOT NULL,
            content TEXT,
            tool_call_id TEXT,
            tool_calls TEXT,
            tool_name TEXT,
            timestamp REAL NOT NULL,
            token_count INTEGER,
            finish_reason TEXT,
            reasoning TEXT,
            reasoning_details TEXT,
            codex_reasoning_items TEXT
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_sessions_started ON sessions(started_at DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id, timestamp)"
    )
    conn.commit()
    conn.close()


class FakeContext:
    def __init__(self):
        self.routes: dict[tuple[str, str], Any] = {}
        self.startup_callbacks: list[Any] = []
        self.shutdown_callbacks: list[Any] = []
        self.tools: dict[str, dict[str, Any]] = {}
        self.cli_commands: dict[str, dict[str, Any]] = {}

    def register_http_route(self, method: str, path: str, handler: Any) -> None:
        self.routes[(method.upper(), path)] = handler

    def register_startup_callback(self, callback: Any) -> None:
        self.startup_callbacks.append(callback)

    def register_shutdown_callback(self, callback: Any) -> None:
        self.shutdown_callbacks.append(callback)

    def register_tool(
        self,
        name: str,
        toolset: str,
        schema: dict[str, Any],
        handler: Any,
        **kwargs: Any,
    ) -> None:
        self.tools[name] = {
            "name": name,
            "toolset": toolset,
            "schema": schema,
            "handler": handler,
            **kwargs,
        }

    def register_cli_command(
        self,
        name: str,
        help: str,
        setup_fn: Any,
        handler_fn: Any | None = None,
        description: str = "",
    ) -> None:
        self.cli_commands[name] = {
            "name": name,
            "help": help,
            "setup_fn": setup_fn,
            "handler_fn": handler_fn,
            "description": description,
        }


@dataclass
class FakeRequest:
    method: str
    path: str
    headers: dict[str, str]
    body: dict
    app: dict[Any, Any]
    match_info: dict[str, str]
    post_data: dict[str, Any] | None = None
    ws_messages: list[Any] | None = None

    async def json(self) -> dict:
        return self.body

    async def post(self) -> dict[str, Any]:
        return self.post_data if self.post_data is not None else self.body


@dataclass
class FakeUploadField:
    filename: str
    content: bytes
    content_type: str = "application/octet-stream"

    def __post_init__(self):
        self.file = io.BytesIO(self.content)

    def read(self) -> bytes:
        return self.content


def response_json(response: Any) -> tuple[int, dict]:
    return response.status, parse_json_response(response)


class FakeSessionDB:
    def __init__(self, db_path: str):
        self._db_path = db_path

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def list_sessions_rich(self, limit: int = 20, offset: int = 0, **_kwargs: Any):
        conn = self._conn()
        rows = conn.execute(
            """
            SELECT
                s.*,
                COALESCE(
                    (SELECT m.content
                     FROM messages m
                     WHERE m.session_id = s.id
                       AND m.content IS NOT NULL
                       AND TRIM(m.content) <> ''
                     ORDER BY m.timestamp DESC, m.id DESC LIMIT 1),
                    ''
                ) AS _latest_message_content,
                COALESCE(
                    (SELECT m.content
                     FROM messages m
                     WHERE m.session_id = s.id
                       AND m.role = 'user'
                       AND m.content IS NOT NULL
                       AND TRIM(m.content) <> ''
                     ORDER BY m.timestamp, m.id LIMIT 1),
                    ''
                ) AS _first_user_message,
                COALESCE(
                    (SELECT MAX(m2.timestamp) FROM messages m2 WHERE m2.session_id = s.id),
                    s.started_at
                ) AS last_active
            FROM sessions s
            WHERE s.parent_session_id IS NULL
            ORDER BY s.started_at DESC
            LIMIT ? OFFSET ?
            """,
            (limit, offset),
        ).fetchall()
        conn.close()
        out = []
        for row in rows:
            item = dict(row)
            latest = _normalize_text(str(item.pop("_latest_message_content", "") or ""))
            first_user = _normalize_text(str(item.pop("_first_user_message", "") or ""))
            title = _normalize_text(str(item.get("title") or ""))
            title_source = "metadata"
            if not title:
                if first_user:
                    title = _truncate_text(first_user, 72)
                    title_source = "derived_first_user"
                elif latest:
                    title = _truncate_text(latest, 72)
                    title_source = "derived_latest_message"
                else:
                    title = "New Chat"
                    title_source = "fallback_new_chat"
            preview_text = _truncate_text(latest, 140)
            item["title"] = title
            item["title_source"] = title_source
            item["preview_text"] = preview_text
            item["preview"] = preview_text
            out.append(item)
        return out

    def create_session(
        self,
        *,
        session_id: str | None = None,
        source: str = "mobile",
        title: str | None = None,
    ) -> dict | None:
        resolved_session_id = str(session_id or "").strip() or str(uuid.uuid4())
        if "/" in resolved_session_id:
            return None
        now = time.time()
        requested_title = _normalize_text(str(title or ""))
        persisted_title = requested_title or None
        if requested_title:
            title_source = "metadata"
        else:
            requested_title = "New Chat"
            title_source = "fallback_new_chat"
        conn = self._conn()
        try:
            conn.execute(
                """
                INSERT INTO sessions(id, source, started_at, message_count, title)
                VALUES (?, ?, ?, 0, ?)
                """,
                (
                    resolved_session_id,
                    source,
                    now,
                    persisted_title,
                ),
            )
            conn.commit()
        except sqlite3.IntegrityError:
            return None
        finally:
            conn.close()
        return {
            "id": resolved_session_id,
            "source": source,
            "title": requested_title,
            "title_source": title_source,
            "preview_text": "",
            "preview": "",
            "started_at": now,
            "last_active": now,
            "message_count": 0,
            "unread_count": 0,
        }

    def get_session(self, session_id: str) -> dict | None:
        conn = self._conn()
        row = conn.execute("SELECT * FROM sessions WHERE id = ?", (session_id,)).fetchone()
        conn.close()
        return dict(row) if row else None

    def get_messages(self, session_id: str) -> list[dict]:
        conn = self._conn()
        rows = conn.execute(
            "SELECT * FROM messages WHERE session_id = ? ORDER BY timestamp, id",
            (session_id,),
        ).fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def get_messages_as_conversation(self, session_id: str) -> list[dict]:
        conn = self._conn()
        rows = conn.execute(
            "SELECT role, content, tool_call_id, tool_calls, tool_name FROM messages WHERE session_id = ? ORDER BY timestamp, id",
            (session_id,),
        ).fetchall()
        conn.close()
        out = []
        for row in rows:
            msg = {"role": row["role"], "content": row["content"]}
            if row["tool_call_id"]:
                msg["tool_call_id"] = row["tool_call_id"]
            if row["tool_name"]:
                msg["tool_name"] = row["tool_name"]
            out.append(msg)
        return out


class _InterruptibleAgent:
    def __init__(self):
        self._interrupted = False

    def interrupt(self) -> None:
        self._interrupted = True

    @property
    def interrupted(self) -> bool:
        return self._interrupted


class FakeAPIServerAdapter:
    def __init__(self, db_path: str):
        self._db_path = db_path
        self._session_db = FakeSessionDB(db_path)
        self.run_call_count = 0

    def _ensure_session_db(self) -> FakeSessionDB:
        return self._session_db

    async def _run_agent(
        self,
        *,
        user_message: str,
        conversation_history: list[dict],
        ephemeral_system_prompt: str | None = None,
        session_id: str | None = None,
        stream_delta_callback=None,
        tool_progress_callback=None,
        agent_ref=None,
    ):
        del conversation_history, tool_progress_callback
        self.run_call_count += 1
        sid = session_id or f"session-{int(time.time())}"
        agent = _InterruptibleAgent()
        if agent_ref is not None:
            agent_ref[0] = agent

        if "Long running" in user_message:
            for _ in range(50):
                if agent.interrupted:
                    return {"error": "aborted"}, {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}
                await asyncio.sleep(0.02)
        elif "Slow sync response" in user_message:
            for _ in range(10):
                if agent.interrupted:
                    return {"error": "aborted"}, {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}
                await asyncio.sleep(0.02)
        elif "Explode stream" in user_message:
            raise RuntimeError("simulated stream failure")

        first = "Hermes "
        second = f"response: {user_message}"
        if stream_delta_callback:
            stream_delta_callback(first)
            stream_delta_callback(second)
            stream_delta_callback(None)

        final_text = first + second
        now = time.time()
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        conn.execute(
            """
            INSERT INTO sessions(id, source, started_at, message_count, title, system_prompt)
            VALUES (?, 'api_server', ?, 0, ?, ?)
            ON CONFLICT(id) DO NOTHING
            """,
            (sid, now, user_message[:32], ephemeral_system_prompt),
        )
        conn.execute(
            """
            INSERT INTO messages(session_id, role, content, timestamp)
            VALUES (?, 'user', ?, ?)
            """,
            (sid, user_message, now),
        )
        conn.execute(
            """
            INSERT INTO messages(session_id, role, content, timestamp)
            VALUES (?, 'assistant', ?, ?)
            """,
            (sid, final_text, now + 0.001),
        )
        conn.execute(
            """
            UPDATE sessions
            SET message_count = (
                SELECT COUNT(*) FROM messages WHERE session_id = ?
            )
            WHERE id = ?
            """,
            (sid, sid),
        )
        conn.commit()
        conn.close()
        return (
            {"final_response": final_text},
            {"input_tokens": 10, "output_tokens": 11, "total_tokens": 21},
        )


class FakeWorkerHandle:
    def __init__(
        self,
        events: list[dict[str, Any]],
        *,
        exit_code: int = 0,
        stderr: str = "",
    ):
        self._events = list(events)
        self._aborted = False
        self._exit_code = exit_code
        self._stderr = stderr

    async def read_event(self) -> dict[str, Any] | None:
        if self._aborted:
            return None
        if not self._events:
            return None
        event = self._events.pop(0)
        delay = float(event.pop("_delay", 0))
        if delay > 0:
            await asyncio.sleep(delay)
        return event

    async def wait(self) -> tuple[int, str]:
        return (self._exit_code, self._stderr)

    def abort(self) -> None:
        self._aborted = True


class FakeProfileRuntime:
    def __init__(self, hermes_root: str):
        self.hermes_root = Path(hermes_root)
        self.run_call_count = 0

    def list_profiles(self) -> list[str]:
        profiles = ["default"]
        profiles_root = self.hermes_root / "profiles"
        if profiles_root.exists():
            profiles.extend(sorted(child.name for child in profiles_root.iterdir() if child.is_dir()))
        return profiles

    def resolve_profile_home(self, profile_name: str) -> Path | None:
        if profile_name == "default":
            home = self.hermes_root
        else:
            home = self.hermes_root / "profiles" / profile_name
        return home if home.is_dir() else None

    def session_view(self, profile_name: str) -> FakeSessionDB | None:
        home = self.resolve_profile_home(profile_name)
        if home is None:
            return None
        return FakeSessionDB(str(home / "state.db"))

    async def start_run(
        self,
        *,
        profile_name: str,
        session_id: str,
        user_message: str,
        conversation_history: list[dict[str, Any]],
        ephemeral_system_prompt: str | None,
    ) -> FakeWorkerHandle:
        del conversation_history
        self.run_call_count += 1
        home = self.resolve_profile_home(profile_name)
        if home is None:
            raise RuntimeError("profile not found")

        if "Explode stream" in user_message:
            return FakeWorkerHandle(
                [
                    {
                        "_delay": 0.01,
                        "event": "failed",
                        "message": "simulated stream failure",
                    }
                ],
                exit_code=1,
                stderr="simulated stream failure",
            )

        first = "Hermes "
        second = f"response: {user_message}"
        final_text = first + second
        delay = 0.0
        if "Long running" in user_message:
            delay = 0.5
        elif "Slow sync response" in user_message:
            delay = 0.2
        waiting_prompt = None
        runtime_events: list[dict[str, Any]] = []
        if "Needs answer" in user_message:
            waiting_prompt = "What should Hermes do next?"
            runtime_events = [
                {
                    "_delay": delay,
                    "event": "tool",
                    "type": "tool.started",
                    "tool_name": "web_search",
                    "preview": "Searching deployment options",
                },
                {
                    "event": "tool",
                    "type": "tool.completed",
                    "tool_name": "web_search",
                    "preview": "Found multiple deployment options",
                },
                {
                    "event": "waiting",
                    "reason": "human_input",
                    "prompt": waiting_prompt,
                },
            ]
        elif "Resume answer" in user_message:
            runtime_events = [
                {
                    "_delay": delay,
                    "event": "tool",
                    "type": "tool.started",
                    "tool_name": "planner",
                    "preview": "Collecting requirements",
                },
                {
                    "event": "waiting",
                    "reason": "human_input",
                    "prompt": "Which environment should I target?",
                },
                {"event": "resumed"},
                {"event": "delta", "delta": first},
                {"event": "delta", "delta": second},
                {
                    "event": "completed",
                    "content": final_text,
                    "usage": {"input_tokens": 10, "output_tokens": 11, "total_tokens": 21},
                },
            ]
        now = time.time()
        conn = sqlite3.connect(str(home / "state.db"))
        conn.row_factory = sqlite3.Row
        conn.execute(
            """
            INSERT INTO sessions(id, source, started_at, message_count, title, system_prompt)
            VALUES (?, 'api_server', ?, 0, ?, ?)
            ON CONFLICT(id) DO NOTHING
            """,
            (session_id, now, user_message[:32], ephemeral_system_prompt),
        )
        conn.execute(
            """
            INSERT INTO messages(session_id, role, content, timestamp)
            VALUES (?, 'user', ?, ?)
            """,
            (session_id, user_message, now),
        )
        if waiting_prompt is None:
            conn.execute(
                """
                INSERT INTO messages(session_id, role, content, timestamp)
                VALUES (?, 'assistant', ?, ?)
                """,
                (session_id, final_text, now + 0.001),
            )
        conn.execute(
            """
            UPDATE sessions
            SET message_count = (
                SELECT COUNT(*) FROM messages WHERE session_id = ?
            )
            WHERE id = ?
            """,
            (session_id, session_id),
        )
        conn.commit()
        conn.close()

        if runtime_events:
            return FakeWorkerHandle(runtime_events)

        return FakeWorkerHandle(
            [
                {"_delay": delay, "event": "delta", "delta": first},
                {"event": "delta", "delta": second},
                {
                    "event": "completed",
                    "content": final_text,
                    "usage": {"input_tokens": 10, "output_tokens": 11, "total_tokens": 21},
                },
            ]
        )


class EnvHarness:
    def __init__(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.hermes_home = self._tmp.name
        self.db_path = os.path.join(self.hermes_home, "state.db")
        self._prev_env: dict[str, str | None] = {}

    def set_up(self) -> None:
        keys = [
            "HERMES_HOME",
            "HERMES_MOBILE_DB_PATH",
            "HERMES_MOBILE_PROFILE_NAME",
            "HERMES_MOBILE_PAIRING_CODE_TTL_SECONDS",
            "HERMES_MOBILE_ACCESS_TOKEN_TTL_SECONDS",
            "HERMES_MOBILE_REFRESH_TOKEN_TTL_SECONDS",
            "HERMES_MOBILE_PUSH_ENABLED",
            "HERMES_MOBILE_APNS_TOPIC",
            "HERMES_MOBILE_APNS_AUTH_KEY_PATH",
            "HERMES_MOBILE_APNS_TEAM_ID",
            "HERMES_MOBILE_APNS_KEY_ID",
            "HERMES_MOBILE_UPLOAD_MAX_BYTES",
            "HERMES_MOBILE_UPLOAD_DIR",
        ]
        for key in keys:
            self._prev_env[key] = os.environ.get(key)
        os.environ["HERMES_HOME"] = self.hermes_home
        os.environ.pop("HERMES_MOBILE_DB_PATH", None)
        os.environ.pop("HERMES_MOBILE_PROFILE_NAME", None)
        os.environ.pop("HERMES_MOBILE_PAIRING_CODE_TTL_SECONDS", None)
        os.environ.pop("HERMES_MOBILE_ACCESS_TOKEN_TTL_SECONDS", None)
        os.environ.pop("HERMES_MOBILE_REFRESH_TOKEN_TTL_SECONDS", None)
        create_hermes_state_db(self.db_path)

    def create_profile(self, name: str) -> str:
        profile_dir = os.path.join(self.hermes_home, "profiles", name)
        os.makedirs(profile_dir, exist_ok=True)
        create_hermes_state_db(os.path.join(profile_dir, "state.db"))
        return profile_dir

    def tear_down(self) -> None:
        for key, value in self._prev_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        self._tmp.cleanup()
