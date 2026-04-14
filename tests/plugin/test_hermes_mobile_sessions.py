import unittest
import inspect
import asyncio
import sqlite3
import json

from hermes_mobile import register
from helpers import (
    EnvHarness,
    FakeContext,
    FakeProfileRuntime,
    FakeRequest,
    response_json,
)


def parse_sse_events(text: str) -> list[dict]:
    events: list[dict] = []
    for line in text.splitlines():
        if not line.startswith("data: "):
            continue
        payload = line[len("data: ") :].strip()
        if not payload:
            continue
        events.append(json.loads(payload))
    return events


class HermesMobileSessionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.env = EnvHarness()
        self.env.set_up()
        self.ctx = FakeContext()
        self.profile_runtime = FakeProfileRuntime(self.env.hermes_home)
        self.ctx.create_profile_runtime = lambda _config: self.profile_runtime
        register(self.ctx)
        for callback in self.ctx.startup_callbacks:
            callback()
        self.access_token = await self._pair_device_and_get_access_token()

    async def asyncTearDown(self):
        for callback in self.ctx.shutdown_callbacks:
            result = callback()
            if inspect.isawaitable(result):
                await result
        self.env.tear_down()

    def route(self, method: str, path: str):
        return self.ctx.routes[(method.upper(), path)]

    def request(
        self,
        method: str,
        path: str,
        *,
        body=None,
        headers=None,
        session_id: str | None = None,
    ) -> FakeRequest:
        match_info = {"session_id": session_id} if session_id else {}
        return FakeRequest(
            method=method.upper(),
            path=path,
            headers=headers or {},
            body=body or {},
            app={},
            match_info=match_info,
        )

    def auth_headers(self):
        return {"Authorization": f"Bearer {self.access_token}"}

    def issue_pairing_code(self, *, profile_name: str = "default") -> dict:
        routes = self.route("GET", "/mobile/capabilities").__self__
        return routes.store.create_pairing_code(
            profile_name=profile_name,
            ttl_seconds=routes.config.pairing_code_ttl_seconds,
        )

    async def _pair_device_and_get_access_token(self, profile_name: str | None = None) -> str:
        start_payload = self.issue_pairing_code(profile_name=profile_name or "default")
        pair_complete = self.route("POST", "/mobile/pair/complete")
        complete_status, complete_payload = response_json(
            await pair_complete(
                self.request(
                    "POST",
                    "/mobile/pair/complete",
                    body={
                        "pairing_code": start_payload["pairing_code"],
                        "device_name": "Bayram iPhone",
                        "device_public_key": "public-key-data",
                        "platform": "ios",
                    },
                )
            )
        )
        self.assertEqual(complete_status, 200)
        return complete_payload["access_token"]

    async def test_named_profile_reads_and_writes_isolated_sessions(self):
        self.env.create_profile("work")
        work_token = await self._pair_device_and_get_access_token(profile_name="work")
        send = self.route("POST", "/mobile/sessions/{session_id}/messages")
        send_status, _ = response_json(
            await send(
                self.request(
                    "POST",
                    "/mobile/sessions/work-session/messages",
                    headers={"Authorization": f"Bearer {work_token}"},
                    session_id="work-session",
                    body={
                        "client_message_id": "work-1",
                        "content": "hello from work",
                    },
                )
            )
        )
        self.assertEqual(send_status, 202)

        sessions = self.route("GET", "/mobile/sessions")
        default_status, default_payload = response_json(
            await sessions(
                self.request(
                    "GET",
                    "/mobile/sessions",
                    headers=self.auth_headers(),
                )
            )
        )
        work_status, work_payload = response_json(
            await sessions(
                self.request(
                    "GET",
                    "/mobile/sessions",
                    headers={"Authorization": f"Bearer {work_token}"},
                )
            )
        )
        self.assertEqual(default_status, 200)
        self.assertEqual(default_payload["sessions"], [])
        self.assertEqual(work_status, 200)
        self.assertEqual(len(work_payload["sessions"]), 1)
        self.assertEqual(work_payload["sessions"][0]["id"], "work-session")

    async def test_sessions_and_messages_require_auth(self):
        sessions = self.route("GET", "/mobile/sessions")
        create_session = self.route("POST", "/mobile/sessions")
        messages = self.route("GET", "/mobile/sessions/{session_id}/messages")
        send = self.route("POST", "/mobile/sessions/{session_id}/messages")
        abort = self.route("POST", "/mobile/sessions/{session_id}/abort")

        s_status, _ = response_json(await sessions(self.request("GET", "/mobile/sessions")))
        self.assertEqual(s_status, 401)
        create_status, _ = response_json(
            await create_session(
                self.request(
                    "POST",
                    "/mobile/sessions",
                    body={"title": "New Chat"},
                )
            )
        )
        self.assertEqual(create_status, 401)
        m_status, _ = response_json(
            await messages(
                self.request(
                    "GET",
                    "/mobile/sessions/s1/messages",
                    session_id="s1",
                )
            )
        )
        self.assertEqual(m_status, 401)
        send_status, _ = response_json(
            await send(
                self.request(
                    "POST",
                    "/mobile/sessions/s1/messages",
                    session_id="s1",
                    body={"client_message_id": "c1", "content": "hello"},
                )
            )
        )
        self.assertEqual(send_status, 401)
        abort_status, _ = response_json(
            await abort(
                self.request(
                    "POST",
                    "/mobile/sessions/s1/abort",
                    session_id="s1",
                )
            )
        )
        self.assertEqual(abort_status, 401)

    async def test_send_stream_and_read_history_from_canonical_state_db(self):
        send = self.route("POST", "/mobile/sessions/{session_id}/messages")
        session_id = "session-alpha"
        send_status, send_payload = response_json(
            await send(
                self.request(
                    "POST",
                    f"/mobile/sessions/{session_id}/messages",
                    session_id=session_id,
                    headers=self.auth_headers(),
                    body={
                        "client_message_id": "client-1",
                        "content": "Hello Hermes",
                    },
                )
            )
        )
        self.assertEqual(send_status, 202)
        self.assertEqual(send_payload["ok"], True)
        self.assertEqual(send_payload["session_id"], session_id)
        self.assertEqual(send_payload["stream"]["transport"], "sse")
        self.assertEqual(send_payload["stream"]["done"], True)
        event_types = [event["type"] for event in send_payload["stream"]["events"]]
        self.assertIn("message.delta", event_types)
        self.assertIn("message.completed", event_types)

        sessions = self.route("GET", "/mobile/sessions")
        sessions_status, sessions_payload = response_json(
            await sessions(
                self.request(
                    "GET",
                    "/mobile/sessions",
                    headers=self.auth_headers(),
                )
            )
        )
        self.assertEqual(sessions_status, 200)
        self.assertEqual(len(sessions_payload["sessions"]), 1)
        self.assertEqual(sessions_payload["sessions"][0]["id"], session_id)
        self.assertEqual(sessions_payload["sessions"][0]["message_count"], 2)
        self.assertTrue(bool(sessions_payload["sessions"][0]["title"]))
        self.assertIn("preview_text", sessions_payload["sessions"][0])
        self.assertIn("Hermes response", sessions_payload["sessions"][0]["preview_text"])
        self.assertEqual(
            sessions_payload["sessions"][0]["preview"],
            sessions_payload["sessions"][0]["preview_text"],
        )

        messages = self.route("GET", "/mobile/sessions/{session_id}/messages")
        messages_status, messages_payload = response_json(
            await messages(
                self.request(
                    "GET",
                    f"/mobile/sessions/{session_id}/messages",
                    session_id=session_id,
                    headers=self.auth_headers(),
                )
            )
        )
        self.assertEqual(messages_status, 200)
        self.assertEqual(len(messages_payload["messages"]), 2)
        self.assertEqual(messages_payload["messages"][0]["role"], "user")
        self.assertEqual(messages_payload["messages"][1]["role"], "assistant")
        self.assertIn("Hermes response", messages_payload["messages"][1]["content"])

    async def test_create_session_returns_shell_and_lists_it(self):
        create_session = self.route("POST", "/mobile/sessions")
        sessions = self.route("GET", "/mobile/sessions")

        create_status, create_payload = response_json(
            await create_session(
                self.request(
                    "POST",
                    "/mobile/sessions",
                    headers=self.auth_headers(),
                    body={"title": "Roadmap discussion"},
                )
            )
        )
        self.assertEqual(create_status, 201)
        self.assertEqual(create_payload["ok"], True)
        created = create_payload["session"]
        self.assertTrue(bool(created["id"]))
        self.assertEqual(created["title"], "Roadmap discussion")
        self.assertEqual(created["preview_text"], "")
        self.assertEqual(created["preview"], "")
        self.assertEqual(created["message_count"], 0)
        self.assertEqual(created["unread_count"], 0)
        self.assertEqual(created["source"], "mobile")

        sessions_status, sessions_payload = response_json(
            await sessions(
                self.request(
                    "GET",
                    "/mobile/sessions",
                    headers=self.auth_headers(),
                )
            )
        )
        self.assertEqual(sessions_status, 200)
        self.assertEqual(len(sessions_payload["sessions"]), 1)
        listed = sessions_payload["sessions"][0]
        self.assertEqual(listed["id"], created["id"])
        self.assertEqual(listed["title"], "Roadmap discussion")
        self.assertEqual(listed["preview_text"], "")

    async def test_create_session_with_blank_title_returns_fallback_title(self):
        create_session = self.route("POST", "/mobile/sessions")

        create_status, create_payload = response_json(
            await create_session(
                self.request(
                    "POST",
                    "/mobile/sessions",
                    headers=self.auth_headers(),
                    body={"title": "   "},
                )
            )
        )
        self.assertEqual(create_status, 201)
        self.assertEqual(create_payload["ok"], True)
        created = create_payload["session"]
        self.assertEqual(created["title"], "New Chat")
        self.assertEqual(created["title_source"], "fallback_new_chat")

    async def test_send_with_stream_true_returns_sse_response(self):
        send = self.route("POST", "/mobile/sessions/{session_id}/messages")
        session_id = "session-sse-live"
        response = await send(
            self.request(
                "POST",
                f"/mobile/sessions/{session_id}/messages",
                session_id=session_id,
                headers=self.auth_headers(),
                body={
                    "client_message_id": "sse-1",
                    "content": "Stream me",
                    "stream": True,
                },
            )
        )

        self.assertEqual(response.status, 200)
        self.assertEqual(response.content_type, "text/event-stream")
        events = parse_sse_events(response.text)
        self.assertGreaterEqual(len(events), 3)
        self.assertEqual(events[0]["type"], "message.accepted")
        event_types = [event["type"] for event in events]
        self.assertIn("message.delta", event_types)
        self.assertEqual(events[-1]["type"], "message.completed")

    async def test_stream_replay_uses_sse_and_does_not_rerun_agent(self):
        send = self.route("POST", "/mobile/sessions/{session_id}/messages")
        session_id = "session-sse-replay"
        first = await send(
            self.request(
                "POST",
                f"/mobile/sessions/{session_id}/messages",
                session_id=session_id,
                headers=self.auth_headers(),
                body={
                    "client_message_id": "sse-replay-1",
                    "content": "Replay stream",
                    "stream": True,
                },
            )
        )
        second = await send(
            self.request(
                "POST",
                f"/mobile/sessions/{session_id}/messages",
                session_id=session_id,
                headers=self.auth_headers(),
                body={
                    "client_message_id": "sse-replay-1",
                    "content": "Replay stream",
                    "stream": True,
                },
            )
        )
        self.assertEqual(first.status, 200)
        self.assertEqual(second.status, 200)
        self.assertEqual(first.content_type, "text/event-stream")
        self.assertEqual(second.content_type, "text/event-stream")
        self.assertEqual(self.profile_runtime.run_call_count, 1)

    async def test_stream_runtime_error_emits_failure_event_and_persists_failed_request(self):
        send = self.route("POST", "/mobile/sessions/{session_id}/messages")
        session_id = "session-sse-error"
        response = await send(
            self.request(
                "POST",
                f"/mobile/sessions/{session_id}/messages",
                session_id=session_id,
                headers=self.auth_headers(),
                body={
                    "client_message_id": "sse-error-1",
                    "content": "Explode stream",
                    "stream": True,
                },
            )
        )

        self.assertEqual(response.status, 200)
        self.assertEqual(response.content_type, "text/event-stream")
        events = parse_sse_events(response.text)
        self.assertGreaterEqual(len(events), 2)
        self.assertEqual(events[0]["type"], "message.accepted")
        self.assertEqual(events[-1]["type"], "message.failed")
        self.assertEqual(events[-1]["error"]["code"], "runtime_error")

        conn = sqlite3.connect(self.env.db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT status, response_json FROM mobile_message_requests WHERE session_id = ?",
            (session_id,),
        ).fetchone()
        conn.close()
        self.assertIsNotNone(row)
        self.assertEqual(row["status"], "failed")
        stored_payload = json.loads(row["response_json"])
        self.assertEqual(stored_payload["stream"]["events"][-1]["type"], "message.failed")

    async def test_send_idempotency_replay_and_conflict(self):
        send = self.route("POST", "/mobile/sessions/{session_id}/messages")
        session_id = "session-idempotent"

        first_status, first_payload = response_json(
            await send(
                self.request(
                    "POST",
                    f"/mobile/sessions/{session_id}/messages",
                    session_id=session_id,
                    headers=self.auth_headers(),
                    body={
                        "client_message_id": "idem-1",
                        "content": "Repeat me",
                    },
                )
            )
        )
        second_status, second_payload = response_json(
            await send(
                self.request(
                    "POST",
                    f"/mobile/sessions/{session_id}/messages",
                    session_id=session_id,
                    headers=self.auth_headers(),
                    body={
                        "client_message_id": "idem-1",
                        "content": "Repeat me",
                    },
                )
            )
        )
        self.assertEqual(first_status, 202)
        self.assertEqual(second_status, 200)
        self.assertEqual(second_payload["idempotency_replayed"], True)
        self.assertEqual(first_payload["request_id"], second_payload["request_id"])

        conflict_status, _ = response_json(
            await send(
                self.request(
                    "POST",
                    f"/mobile/sessions/{session_id}/messages",
                    session_id=session_id,
                    headers=self.auth_headers(),
                    body={
                        "client_message_id": "idem-1",
                        "content": "Different content",
                    },
                )
            )
        )
        self.assertEqual(conflict_status, 409)

    async def test_sync_send_is_idempotent_while_first_call_is_running(self):
        send = self.route("POST", "/mobile/sessions/{session_id}/messages")
        session_id = "session-slow-sync"

        first_task = asyncio.create_task(
            send(
                self.request(
                    "POST",
                    f"/mobile/sessions/{session_id}/messages",
                    session_id=session_id,
                    headers=self.auth_headers(),
                    body={
                        "client_message_id": "sync-inflight-1",
                        "content": "Slow sync response",
                    },
                )
            )
        )
        await asyncio.sleep(0.05)

        replay_status, replay_payload = response_json(
            await send(
                self.request(
                    "POST",
                    f"/mobile/sessions/{session_id}/messages",
                    session_id=session_id,
                    headers=self.auth_headers(),
                    body={
                        "client_message_id": "sync-inflight-1",
                        "content": "Slow sync response",
                    },
                )
            )
        )
        first_status, first_payload = response_json(await first_task)

        self.assertEqual(replay_status, 202)
        self.assertEqual(first_status, 202)
        self.assertEqual(replay_payload["idempotency_replayed"], True)
        self.assertEqual(replay_payload["request_id"], first_payload["request_id"])
        self.assertEqual(self.profile_runtime.run_call_count, 1)

        conn = sqlite3.connect(self.env.db_path)
        count = conn.execute(
            "SELECT COUNT(*) FROM messages WHERE session_id = ?",
            (session_id,),
        ).fetchone()[0]
        conn.close()
        self.assertEqual(count, 2)

    async def test_abort_active_stream(self):
        send = self.route("POST", "/mobile/sessions/{session_id}/messages")
        abort = self.route("POST", "/mobile/sessions/{session_id}/abort")
        session_id = "session-abort"
        send_status, send_payload = response_json(
            await send(
                self.request(
                    "POST",
                    f"/mobile/sessions/{session_id}/messages",
                    session_id=session_id,
                    headers=self.auth_headers(),
                    body={
                        "client_message_id": "client-abort-1",
                        "content": "Long running response",
                        "defer_completion": True,
                    },
                )
            )
        )
        self.assertEqual(send_status, 202)
        self.assertEqual(send_payload["stream"]["done"], False)

        abort_status, abort_payload = response_json(
            await abort(
                self.request(
                    "POST",
                    f"/mobile/sessions/{session_id}/abort",
                    session_id=session_id,
                    headers=self.auth_headers(),
                )
            )
        )
        self.assertEqual(abort_status, 200)
        self.assertEqual(abort_payload["aborted"], True)
        self.assertEqual(abort_payload["session_id"], session_id)

        abort2_status, abort2_payload = response_json(
            await abort(
                self.request(
                    "POST",
                    f"/mobile/sessions/{session_id}/abort",
                    session_id=session_id,
                    headers=self.auth_headers(),
                )
            )
        )
        self.assertEqual(abort2_status, 409)
        self.assertEqual(abort2_payload["aborted"], False)

    async def test_abort_ignores_completed_run_left_in_active_map(self):
        send = self.route("POST", "/mobile/sessions/{session_id}/messages")
        abort = self.route("POST", "/mobile/sessions/{session_id}/abort")
        route_owner = abort.__self__
        session_id = "session-completed-race"
        send_status, send_payload = response_json(
            await send(
                self.request(
                    "POST",
                    f"/mobile/sessions/{session_id}/messages",
                    session_id=session_id,
                    headers=self.auth_headers(),
                    body={
                        "client_message_id": "client-completed-race-1",
                        "content": "Completed response",
                    },
                )
            )
        )
        self.assertEqual(send_status, 202)
        device_id = route_owner.store.resolve_access_token(self.access_token)["device_id"]

        done_task = asyncio.create_task(asyncio.sleep(0))
        await done_task
        route_owner._active_runs[(device_id, session_id)] = {
            "request_id": send_payload["request_id"],
            "agent_ref": [None],
            "task": done_task,
        }

        abort_status, abort_payload = response_json(
            await abort(
                self.request(
                    "POST",
                    f"/mobile/sessions/{session_id}/abort",
                    session_id=session_id,
                    headers=self.auth_headers(),
                )
            )
        )
        self.assertEqual(abort_status, 409)
        self.assertEqual(abort_payload["aborted"], False)

        conn = sqlite3.connect(self.env.db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT status FROM mobile_message_requests WHERE id = ?",
            (send_payload["request_id"],),
        ).fetchone()
        conn.close()
        self.assertIsNotNone(row)
        self.assertEqual(row["status"], "completed")

    async def test_shutdown_cancels_background_runs_before_db_close(self):
        send = self.route("POST", "/mobile/sessions/{session_id}/messages")
        session_id = "session-shutdown"
        send_status, send_payload = response_json(
            await send(
                self.request(
                    "POST",
                    f"/mobile/sessions/{session_id}/messages",
                    session_id=session_id,
                    headers=self.auth_headers(),
                    body={
                        "client_message_id": "shutdown-1",
                        "content": "Long running response",
                        "defer_completion": True,
                    },
                )
            )
        )
        self.assertEqual(send_status, 202)
        request_id = send_payload["request_id"]

        for callback in self.ctx.shutdown_callbacks:
            result = callback()
            if inspect.isawaitable(result):
                await result

        conn = sqlite3.connect(self.env.db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT status FROM mobile_message_requests WHERE id = ?",
            (request_id,),
        ).fetchone()
        conn.close()
        self.assertIsNotNone(row)
        self.assertEqual(row["status"], "aborted")


if __name__ == "__main__":
    unittest.main()
