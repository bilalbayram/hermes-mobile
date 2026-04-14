import base64
import inspect
import os
import sqlite3
import subprocess
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

from plugins.hermes_mobile import register
from plugins.hermes_mobile.push import APNsPushSender
from helpers import (
    EnvHarness,
    FakeContext,
    FakeProfileRuntime,
    FakeRequest,
    FakeUploadField,
    response_json,
)


class FakePushSender:
    def __init__(self):
        self.calls: list[dict] = []

    def diagnostics(self) -> dict:
        return {
            "enabled": True,
            "mode": "apns",
            "reason": None,
            "provider": "fake",
        }

    def send(self, **kwargs):
        self.calls.append(kwargs)
        return {
            "ok": True,
            "status": "sent",
            "http_status": 200,
            "response_body": {},
            "apns_id": "apns-fake-id",
        }


class HermesMobileUploadContractTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.env = EnvHarness()
        self.env.set_up()
        self.ctx = FakeContext()
        self.profile_runtime = FakeProfileRuntime(self.env.hermes_home)
        self.ctx.create_profile_runtime = lambda _config: self.profile_runtime
        register(self.ctx)
        for callback in self.ctx.startup_callbacks:
            callback()
        self.access_token = await self._pair_device()

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
        post_data=None,
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
            post_data=post_data,
        )

    def auth_headers(self):
        return {"Authorization": f"Bearer {self.access_token}"}

    def issue_pairing_code(self) -> dict:
        routes = self.route("GET", "/mobile/capabilities").__self__
        return routes.store.create_pairing_code(
            profile_name="default",
            ttl_seconds=routes.config.pairing_code_ttl_seconds,
        )

    async def _pair_device(self) -> str:
        start_payload = self.issue_pairing_code()
        pair_complete = self.route("POST", "/mobile/pair/complete")
        complete_status, complete_payload = response_json(
            await pair_complete(
                self.request(
                    "POST",
                    "/mobile/pair/complete",
                    body={
                        "pairing_code": start_payload["pairing_code"],
                        "device_name": "iPhone",
                        "device_public_key": "public-key",
                        "platform": "ios",
                    },
                )
            )
        )
        self.assertEqual(complete_status, 200)
        return complete_payload["access_token"]

    async def test_default_upload_limit_is_20mb(self):
        uploads = self.route("POST", "/mobile/uploads")
        status, payload = response_json(
            await uploads(
                self.request(
                    "POST",
                    "/mobile/uploads",
                    headers=self.auth_headers(),
                    body={
                        "filename": "note.txt",
                        "content_type": "text/plain",
                        "content_base64": base64.b64encode(b"hello").decode("ascii"),
                    },
                )
            )
        )
        self.assertEqual(status, 201)
        self.assertEqual(payload["limits"]["max_bytes"], 20 * 1024 * 1024)

    async def test_multipart_upload_persists_session_and_enforces_mime_types(self):
        uploads = self.route("POST", "/mobile/uploads")
        ok_status, ok_payload = response_json(
            await uploads(
                self.request(
                    "POST",
                    "/mobile/uploads",
                    headers={
                        **self.auth_headers(),
                        "Content-Type": "multipart/form-data; boundary=test",
                    },
                    post_data={
                        "session_id": "session-123",
                        "file": FakeUploadField(
                            filename="clip.txt",
                            content=b"hello attachment",
                            content_type="text/plain",
                        ),
                    },
                )
            )
        )
        self.assertEqual(ok_status, 201)
        self.assertEqual(ok_payload["upload"]["session_id"], "session-123")
        self.assertEqual(ok_payload["upload"]["content_type"], "text/plain")
        self.assertTrue(Path(ok_payload["upload"]["stored_path"]).exists())

        conn = sqlite3.connect(self.env.db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT session_id, content_type, byte_size
            FROM mobile_uploads
            WHERE id = ?
            """,
            (ok_payload["upload"]["id"],),
        ).fetchone()
        conn.close()
        self.assertIsNotNone(row)
        self.assertEqual(row["session_id"], "session-123")
        self.assertEqual(row["content_type"], "text/plain")
        self.assertEqual(row["byte_size"], len(b"hello attachment"))

        rejected_status, rejected_payload = response_json(
            await uploads(
                self.request(
                    "POST",
                    "/mobile/uploads",
                    headers={
                        **self.auth_headers(),
                        "Content-Type": "multipart/form-data; boundary=test",
                    },
                    post_data={
                        "file": FakeUploadField(
                            filename="evil.exe",
                            content=b"MZ",
                            content_type="application/x-msdownload",
                        ),
                    },
                )
            )
        )
        self.assertEqual(rejected_status, 415)
        self.assertEqual(rejected_payload["error"]["code"], "unsupported_media_type")

        empty_status, empty_payload = response_json(
            await uploads(
                self.request(
                    "POST",
                    "/mobile/uploads",
                    headers={
                        **self.auth_headers(),
                        "Content-Type": "multipart/form-data; boundary=test",
                    },
                    post_data={
                        "file": FakeUploadField(
                            filename="empty.txt",
                            content=b"",
                            content_type="text/plain",
                        ),
                    },
                )
            )
        )
        self.assertEqual(empty_status, 400)
        self.assertEqual(empty_payload["error"]["code"], "bad_request")

    async def test_send_validates_attachment_ids(self):
        uploads = self.route("POST", "/mobile/uploads")
        upload_status, upload_payload = response_json(
            await uploads(
                self.request(
                    "POST",
                    "/mobile/uploads",
                    headers=self.auth_headers(),
                    body={
                        "filename": "note.txt",
                        "content_type": "text/plain",
                        "content_base64": base64.b64encode(b"hello").decode("ascii"),
                    },
                )
            )
        )
        self.assertEqual(upload_status, 201)

        send = self.route("POST", "/mobile/sessions/{session_id}/messages")
        missing_status, missing_payload = response_json(
            await send(
                self.request(
                    "POST",
                    "/mobile/sessions/session-attach/messages",
                    headers=self.auth_headers(),
                    session_id="session-attach",
                    body={
                        "client_message_id": "c1",
                        "content": "use attachment",
                        "attachment_ids": ["missing-attachment"],
                    },
                )
            )
        )
        self.assertEqual(missing_status, 404)
        self.assertEqual(missing_payload["error"]["code"], "attachment_not_found")

        ok_status, ok_payload = response_json(
            await send(
                self.request(
                    "POST",
                    "/mobile/sessions/session-attach/messages",
                    headers=self.auth_headers(),
                    session_id="session-attach",
                    body={
                        "client_message_id": "c2",
                        "content": "use attachment",
                        "attachment_ids": [upload_payload["upload"]["id"]],
                    },
                )
            )
        )
        self.assertEqual(ok_status, 202)
        self.assertEqual(ok_payload["ok"], True)
        self.assertEqual(ok_payload["session_id"], "session-attach")


class HermesMobilePushDeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.env = EnvHarness()
        self.env.set_up()
        self.key_dir = tempfile.TemporaryDirectory()
        self.key_path = Path(self.key_dir.name) / "AuthKey_ABC123.p8"
        self.key_path.write_text("-----BEGIN PRIVATE KEY-----\nMIIB\n-----END PRIVATE KEY-----\n")
        os.environ["HERMES_MOBILE_APNS_TOPIC"] = "sh.talaria.ios"
        os.environ["HERMES_MOBILE_APNS_TEAM_ID"] = "TEAM12345"
        os.environ["HERMES_MOBILE_APNS_KEY_ID"] = "ABC123"
        os.environ["HERMES_MOBILE_APNS_AUTH_KEY_PATH"] = str(self.key_path)
        os.environ["HERMES_MOBILE_PUSH_ENABLED"] = "true"
        self.ctx = FakeContext()
        self.profile_runtime = FakeProfileRuntime(self.env.hermes_home)
        self.ctx.create_profile_runtime = lambda _config: self.profile_runtime
        self.fake_sender = FakePushSender()
        self.ctx.create_push_sender = lambda config, store: self.fake_sender
        register(self.ctx)
        for callback in self.ctx.startup_callbacks:
            callback()
        self.device1 = await self._pair_device(name="Device One")
        self.device2 = await self._pair_device(name="Device Two")

    async def asyncTearDown(self):
        for callback in self.ctx.shutdown_callbacks:
            result = callback()
            if inspect.isawaitable(result):
                await result
        self.key_dir.cleanup()
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
        session_id=None,
    ):
        return FakeRequest(
            method=method.upper(),
            path=path,
            headers=headers or {},
            body=body or {},
            app={},
            match_info={"session_id": session_id} if session_id else {},
        )

    def auth_headers(self, access_token: str):
        return {"Authorization": f"Bearer {access_token}"}

    def issue_pairing_code(self) -> dict:
        routes = self.route("GET", "/mobile/capabilities").__self__
        return routes.store.create_pairing_code(
            profile_name="default",
            ttl_seconds=routes.config.pairing_code_ttl_seconds,
        )

    async def _pair_device(self, *, name: str) -> dict:
        start_payload = self.issue_pairing_code()
        pair_complete = self.route("POST", "/mobile/pair/complete")
        complete_status, complete_payload = response_json(
            await pair_complete(
                self.request(
                    "POST",
                    "/mobile/pair/complete",
                    body={
                        "pairing_code": start_payload["pairing_code"],
                        "device_name": name,
                        "device_public_key": f"{name}-public-key",
                        "platform": "ios",
                    },
                )
            )
        )
        self.assertEqual(complete_status, 200)
        return complete_payload

    async def test_push_register_and_delivery_logging_are_enabled(self):
        push_register = self.route("POST", "/mobile/push/register")
        reg_status, reg_payload = response_json(
            await push_register(
                self.request(
                    "POST",
                    "/mobile/push/register",
                    headers=self.auth_headers(self.device2["access_token"]),
                    body={
                        "push_token": "token-device-two",
                        "platform": "ios",
                        "environment": "sandbox",
                        "app_id": "sh.talaria.ios",
                    },
                )
            )
        )
        self.assertEqual(reg_status, 200)
        self.assertTrue(reg_payload["delivery"]["enabled"])
        self.assertEqual(reg_payload["delivery"]["mode"], "apns")

        send = self.route("POST", "/mobile/sessions/{session_id}/messages")
        send_status, send_payload = response_json(
            await send(
                self.request(
                    "POST",
                    "/mobile/sessions/session-push/messages",
                    headers=self.auth_headers(self.device1["access_token"]),
                    session_id="session-push",
                    body={
                        "client_message_id": "push-1",
                        "content": "trigger push",
                    },
                )
            )
        )
        self.assertEqual(send_status, 202)
        self.assertEqual(send_payload["ok"], True)
        self.assertEqual(len(self.fake_sender.calls), 1)
        call = self.fake_sender.calls[0]
        self.assertEqual(call["event_type"], "message.completed")
        self.assertEqual(call["session_id"], "session-push")
        self.assertEqual(call["source_device_id"], self.device1["device_id"])
        self.assertEqual(call["target_device_id"], self.device2["device_id"])

        conn = sqlite3.connect(self.env.db_path)
        conn.row_factory = sqlite3.Row
        delivery = conn.execute(
            """
            SELECT profile_name, device_id, session_id, event_type, status, http_status
            FROM mobile_push_deliveries
            ORDER BY created_at DESC
            LIMIT 1
            """
        ).fetchone()
        conn.close()
        self.assertIsNotNone(delivery)
        self.assertEqual(delivery["device_id"], self.device2["device_id"])
        self.assertEqual(delivery["session_id"], "session-push")
        self.assertEqual(delivery["status"], "sent")
        self.assertEqual(delivery["http_status"], 200)

        diagnostics = self.route("GET", "/mobile/push/diagnostics")
        diag_status, diag_payload = response_json(
            await diagnostics(
                self.request(
                    "GET",
                    "/mobile/push/diagnostics",
                    headers=self.auth_headers(self.device1["access_token"]),
                )
            )
        )
        self.assertEqual(diag_status, 200)
        self.assertTrue(diag_payload["enabled"])
        self.assertGreaterEqual(diag_payload["deliveries"]["total"], 1)
        self.assertGreaterEqual(diag_payload["registrations"]["active"], 1)

    async def test_revoked_devices_are_not_targeted_for_push(self):
        revoke = self.route("POST", "/mobile/devices/{device_id}/revoke")
        revoke_status, _ = response_json(
            await revoke(
                self.request(
                    "POST",
                    f"/mobile/devices/{self.device2['device_id']}/revoke",
                    headers=self.auth_headers(self.device1["access_token"]),
                    session_id=None,
                )
            )
        )
        self.assertEqual(revoke_status, 200)

        send = self.route("POST", "/mobile/sessions/{session_id}/messages")
        send_status, _ = response_json(
            await send(
                self.request(
                    "POST",
                    "/mobile/sessions/session-revoke/messages",
                    headers=self.auth_headers(self.device1["access_token"]),
                    session_id="session-revoke",
                    body={
                        "client_message_id": "push-2",
                        "content": "trigger push",
                    },
                )
            )
        )
        self.assertEqual(send_status, 202)
        self.assertEqual(len(self.fake_sender.calls), 0)

    def test_apns_sender_uses_jwt_and_reads_apns_id_from_headers(self):
        sender = APNsPushSender(
            topic="sh.talaria.ios",
            team_id="TEAM12345",
            key_id="ABC123",
            auth_key_path=self.key_path,
        )

        fake_jwt = types.SimpleNamespace(
            encode=lambda payload, key, algorithm, headers: "jwt-token"
        )
        completed = subprocess.CompletedProcess(
            args=["curl"],
            returncode=0,
            stdout=(
                b"HTTP/2 200 \r\n"
                b"apns-id: apns-header-id\r\n"
                b"content-type: application/json\r\n\r\n"
                b"{}\n__HTTP_STATUS__:200"
            ),
            stderr=b"",
        )

        with mock.patch.dict(sys.modules, {"jwt": fake_jwt}):
            with mock.patch("plugins.hermes_mobile.push.shutil.which", return_value="/usr/bin/curl"):
                with mock.patch("plugins.hermes_mobile.push.subprocess.run", return_value=completed) as run:
                    result = sender.send(
                        device_token="push-token-1",
                        environment="sandbox",
                        payload={"aps": {"content-available": 1}},
                    )

        self.assertTrue(result["ok"])
        self.assertEqual(result["apns_id"], "apns-header-id")
        command = run.call_args.args[0]
        self.assertIn("authorization: bearer jwt-token", command)


if __name__ == "__main__":
    unittest.main()
