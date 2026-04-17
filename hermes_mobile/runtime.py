from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def _normalize_text(value: str) -> str:
    if not value:
        return ""
    compact = " ".join(value.replace("\r", " ").replace("\n", " ").split())
    return compact.strip()


def _truncate_text(value: str, limit: int) -> str:
    normalized = _normalize_text(value)
    if len(normalized) <= limit:
        return normalized
    return normalized[: max(0, limit - 3)].rstrip() + "..."


class SQLiteSessionView:
    def __init__(self, db_path: Path):
        self.db_path = db_path

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def list_sessions_rich(self, limit: int = 20, offset: int = 0, **_kwargs: Any) -> list[dict[str, Any]]:
        conn = self._connect()
        try:
            try:
                rows = conn.execute(
                    """
                    SELECT
                        s.*,
                        COALESCE(
                            (
                                SELECT m.content
                                FROM messages m
                                WHERE m.session_id = s.id
                                  AND m.content IS NOT NULL
                                  AND TRIM(m.content) <> ''
                                ORDER BY m.timestamp DESC, m.id DESC
                                LIMIT 1
                            ),
                            ''
                        ) AS _latest_message_content,
                        COALESCE(
                            (
                                SELECT m.content
                                FROM messages m
                                WHERE m.session_id = s.id
                                  AND m.role = 'user'
                                  AND m.content IS NOT NULL
                                  AND TRIM(m.content) <> ''
                                ORDER BY m.timestamp ASC, m.id ASC
                                LIMIT 1
                            ),
                            ''
                        ) AS _first_user_message,
                        COALESCE(
                            (
                                SELECT MAX(m.timestamp)
                                FROM messages m
                                WHERE m.session_id = s.id
                            ),
                            s.started_at
                        ) AS last_active
                    FROM sessions s
                    WHERE s.parent_session_id IS NULL
                    ORDER BY last_active DESC
                    LIMIT ? OFFSET ?
                    """,
                    (limit, offset),
                ).fetchall()
            except sqlite3.OperationalError:
                return []
            sessions: list[dict[str, Any]] = []
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
                sessions.append(item)
            return sessions
        finally:
            conn.close()

    def create_session(
        self,
        *,
        session_id: str | None = None,
        source: str = "mobile",
        title: str | None = None,
    ) -> dict[str, Any] | None:
        now = time.time()
        resolved_session_id = str(session_id or "").strip() or str(uuid.uuid4())
        if "/" in resolved_session_id:
            return None

        requested_title = _normalize_text(str(title or ""))
        persisted_title = requested_title or None
        if requested_title:
            title_source = "metadata"
        else:
            requested_title = "New Chat"
            title_source = "fallback_new_chat"
        conn = self._connect()
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
        except sqlite3.OperationalError:
            return None
        finally:
            conn.close()
        return {
            "id": resolved_session_id,
            "source": source,
            "title": requested_title,
            "preview_text": "",
            "last_active": now,
            "started_at": now,
            "message_count": 0,
            "title_source": title_source,
        }

    def get_session(self, session_id: str) -> dict[str, Any] | None:
        conn = self._connect()
        try:
            try:
                row = conn.execute(
                    "SELECT * FROM sessions WHERE id = ?",
                    (session_id,),
                ).fetchone()
            except sqlite3.OperationalError:
                return None
            return dict(row) if row else None
        finally:
            conn.close()

    def get_messages(self, session_id: str) -> list[dict[str, Any]]:
        conn = self._connect()
        try:
            try:
                rows = conn.execute(
                    """
                    SELECT *
                    FROM messages
                    WHERE session_id = ?
                    ORDER BY timestamp ASC, id ASC
                    """,
                    (session_id,),
                ).fetchall()
            except sqlite3.OperationalError:
                return []
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def get_messages_as_conversation(self, session_id: str) -> list[dict[str, Any]]:
        rows = self.get_messages(session_id)
        conversation: list[dict[str, Any]] = []
        for row in rows:
            item = {
                "role": row.get("role"),
                "content": row.get("content"),
            }
            if row.get("tool_call_id"):
                item["tool_call_id"] = row["tool_call_id"]
            if row.get("tool_calls"):
                item["tool_calls"] = row["tool_calls"]
            if row.get("tool_name"):
                item["tool_name"] = row["tool_name"]
            conversation.append(item)
        return conversation


class WorkerRunHandle:
    def __init__(self, process: asyncio.subprocess.Process):
        self.process = process

    async def read_event(self) -> dict[str, Any] | None:
        if self.process.stdout is None:
            return None
        line = await self.process.stdout.readline()
        if not line:
            return None
        return json.loads(line.decode("utf-8"))

    async def wait(self) -> tuple[int, str]:
        stderr = ""
        if self.process.stderr is not None:
            data = await self.process.stderr.read()
            stderr = data.decode("utf-8", "replace").strip()
        code = await self.process.wait()
        return code, stderr

    def abort(self) -> None:
        if self.process.returncode is not None:
            return
        self.process.terminate()


@dataclass(frozen=True)
class HermesProfileRuntime:
    hermes_root: Path

    def list_profiles(self) -> list[str]:
        profiles = ["default"]
        profiles_root = self.hermes_root / "profiles"
        if not profiles_root.exists():
            return profiles
        for child in sorted(profiles_root.iterdir()):
            if child.is_dir():
                profiles.append(child.name)
        return profiles

    def resolve_profile_home(self, profile_name: str) -> Path | None:
        name = str(profile_name or "").strip()
        if not name:
            return None
        if name == "default":
            home = self.hermes_root
        else:
            home = self.hermes_root / "profiles" / name
        if not home.is_dir():
            return None
        return home

    def session_view(self, profile_name: str) -> SQLiteSessionView | None:
        home = self.resolve_profile_home(profile_name)
        if home is None:
            return None
        return SQLiteSessionView(home / "state.db")

    async def start_run(
        self,
        *,
        profile_name: str,
        session_id: str,
        user_message: str,
        conversation_history: list[dict[str, Any]],
        ephemeral_system_prompt: str | None,
    ) -> WorkerRunHandle:
        profile_home = self.resolve_profile_home(profile_name)
        if profile_home is None:
            raise RuntimeError(f"profile not found: {profile_name}")

        try:
            import hermes_constants  # type: ignore

            hermes_root = str(Path(hermes_constants.__file__).resolve().parent)
        except Exception as exc:  # pragma: no cover - production dependency
            raise RuntimeError("hermes runtime is unavailable") from exc

        env = dict(os.environ)
        env["HERMES_HOME"] = str(profile_home)
        pythonpath = env.get("PYTHONPATH", "").strip()
        env["PYTHONPATH"] = (
            os.pathsep.join(filter(None, [hermes_root, pythonpath]))
            if pythonpath
            else hermes_root
        )
        worker_path = Path(__file__).with_name("profile_worker.py")
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            str(worker_path),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
            cwd=hermes_root,
        )
        payload = {
            "session_id": session_id,
            "user_message": user_message,
            "conversation_history": conversation_history,
            "ephemeral_system_prompt": ephemeral_system_prompt,
        }
        if process.stdin is None:
            raise RuntimeError("worker stdin unavailable")
        process.stdin.write(json.dumps(payload).encode("utf-8") + b"\n")
        await process.stdin.drain()
        process.stdin.close()
        return WorkerRunHandle(process)
