import base64
import inspect
import os
import sqlite3
import unittest

from plugins.hermes_mobile import register
from helpers import EnvHarness, FakeContext, FakeRequest, response_json


class HermesMobileDevicePushUploadTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.env = EnvHarness()
        self.env.set_up()
        os.environ["HERMES_MOBILE_UPLOAD_MAX_BYTES"] = "8"
        os.environ.pop("HERMES_MOBILE_PUSH_ENABLED", None)
        self.ctx = FakeContext()
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
        self.env.tear_down()

    def route(self, method: str, path: str):
        return self.ctx.routes[(method.upper(), path)]

    def request(self, method: str, path: str, body=None, headers=None, match_info=None):
        return FakeRequest(
            method=method.upper(),
            path=path,
            headers=headers or {},
            body=body or {},
            app={},
            match_info=match_info or {},
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

    async def test_push_register_allows_disabled_mode_and_persists_registration(self):
        push_register = self.route("POST", "/mobile/push/register")
        status, payload = response_json(
            await push_register(
                self.request(
                    "POST",
                    "/mobile/push/register",
                    headers=self.auth_headers(self.device1["access_token"]),
                    body={
                        "push_token": "push-token-abc",
                        "platform": "ios",
                        "environment": "sandbox",
                        "app_id": "sh.talaria.ios",
                    },
                )
            )
        )
        self.assertEqual(status, 200)
        self.assertEqual(payload["ok"], True)
        self.assertEqual(payload["delivery"]["enabled"], False)
        self.assertEqual(payload["delivery"]["reason"], "missing_credentials")
        self.assertEqual(payload["registration"]["device_id"], self.device1["device_id"])

        conn = sqlite3.connect(self.env.db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT device_id, platform, environment, app_id
            FROM mobile_push_registrations
            WHERE device_id = ?
            """,
            (self.device1["device_id"],),
        ).fetchone()
        conn.close()
        self.assertIsNotNone(row)
        self.assertEqual(row["platform"], "ios")

    async def test_devices_list_and_revoke_invalidates_tokens(self):
        list_devices = self.route("GET", "/mobile/devices")
        list_status, list_payload = response_json(
            await list_devices(
                self.request(
                    "GET",
                    "/mobile/devices",
                    headers=self.auth_headers(self.device1["access_token"]),
                )
            )
        )
        self.assertEqual(list_status, 200)
        self.assertEqual(len(list_payload["devices"]), 2)
        self.assertTrue(
            any(device["id"] == self.device1["device_id"] for device in list_payload["devices"])
        )
        self.assertTrue(
            any(device["id"] == self.device2["device_id"] for device in list_payload["devices"])
        )

        revoke = self.route("POST", "/mobile/devices/{device_id}/revoke")
        revoke_status, revoke_payload = response_json(
            await revoke(
                self.request(
                    "POST",
                    f"/mobile/devices/{self.device2['device_id']}/revoke",
                    headers=self.auth_headers(self.device1["access_token"]),
                    match_info={"device_id": self.device2["device_id"]},
                )
            )
        )
        self.assertEqual(revoke_status, 200)
        self.assertEqual(revoke_payload["revoked"], True)
        self.assertEqual(revoke_payload["device_id"], self.device2["device_id"])

        me = self.route("GET", "/mobile/me")
        me_status, _ = response_json(
            await me(
                self.request(
                    "GET",
                    "/mobile/me",
                    headers=self.auth_headers(self.device2["access_token"]),
                )
            )
        )
        self.assertEqual(me_status, 401)

        refresh = self.route("POST", "/mobile/auth/refresh")
        refresh_status, _ = response_json(
            await refresh(
                self.request(
                    "POST",
                    "/mobile/auth/refresh",
                    body={"refresh_token": self.device2["refresh_token"]},
                )
            )
        )
        self.assertEqual(refresh_status, 401)

    async def test_uploads_persist_and_enforce_explicit_limit(self):
        uploads = self.route("POST", "/mobile/uploads")
        small_content = base64.b64encode(b"hello").decode("ascii")
        ok_status, ok_payload = response_json(
            await uploads(
                self.request(
                    "POST",
                    "/mobile/uploads",
                    headers=self.auth_headers(self.device1["access_token"]),
                    body={
                        "filename": "note.txt",
                        "content_type": "text/plain",
                        "content_base64": small_content,
                    },
                )
            )
        )
        self.assertEqual(ok_status, 201)
        self.assertEqual(ok_payload["ok"], True)
        self.assertEqual(ok_payload["upload"]["byte_size"], 5)
        self.assertTrue(os.path.exists(ok_payload["upload"]["stored_path"]))

        conn = sqlite3.connect(self.env.db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT id, original_filename, byte_size
            FROM mobile_uploads
            WHERE id = ?
            """,
            (ok_payload["upload"]["id"],),
        ).fetchone()
        conn.close()
        self.assertIsNotNone(row)
        self.assertEqual(row["original_filename"], "note.txt")
        self.assertEqual(row["byte_size"], 5)

        too_big = base64.b64encode(b"123456789").decode("ascii")
        too_big_status, too_big_payload = response_json(
            await uploads(
                self.request(
                    "POST",
                    "/mobile/uploads",
                    headers=self.auth_headers(self.device1["access_token"]),
                    body={
                        "filename": "big.txt",
                        "content_base64": too_big,
                    },
                )
            )
        )
        self.assertEqual(too_big_status, 413)
        self.assertEqual(too_big_payload["error"]["code"], "payload_too_large")


if __name__ == "__main__":
    unittest.main()
