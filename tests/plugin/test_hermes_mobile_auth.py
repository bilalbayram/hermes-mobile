import sqlite3
import unittest
import inspect

from hermes_mobile import register
from helpers import EnvHarness, FakeContext, FakeProfileRuntime, FakeRequest, response_json


class HermesMobileAuthTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.env = EnvHarness()
        self.env.set_up()
        self.ctx = FakeContext()
        self.ctx.create_profile_runtime = lambda config: FakeProfileRuntime(str(config.hermes_root))
        register(self.ctx)
        for callback in self.ctx.startup_callbacks:
            callback()

    async def asyncTearDown(self):
        for callback in self.ctx.shutdown_callbacks:
            result = callback()
            if inspect.isawaitable(result):
                await result
        self.env.tear_down()

    def route(self, method: str, path: str):
        return self.ctx.routes[(method.upper(), path)]

    def request(self, method: str, path: str, body=None, headers=None, app=None):
        return FakeRequest(
            method=method.upper(),
            path=path,
            headers=headers or {},
            body=body or {},
            app=app or {},
            match_info={},
        )

    def mobile_routes(self):
        return self.route("GET", "/mobile/capabilities").__self__

    def issue_pairing_code(self, *, profile_name: str = "default", ttl_seconds: int | None = None):
        routes = self.mobile_routes()
        return routes.store.create_pairing_code(
            profile_name=profile_name,
            ttl_seconds=ttl_seconds or routes.config.pairing_code_ttl_seconds,
        )

    async def test_registers_routes_and_startup(self):
        self.assertGreaterEqual(len(self.ctx.startup_callbacks), 1)
        self.assertIn(("GET", "/mobile/capabilities"), self.ctx.routes)
        self.assertIn(("GET", "/mobile/ws"), self.ctx.routes)
        self.assertIn(("POST", "/mobile/pair/start"), self.ctx.routes)
        self.assertIn(("POST", "/mobile/pair/complete"), self.ctx.routes)
        self.assertIn(("POST", "/mobile/auth/refresh"), self.ctx.routes)
        self.assertIn(("GET", "/mobile/me"), self.ctx.routes)
        self.assertIn(("GET", "/mobile/sessions"), self.ctx.routes)
        self.assertIn(("POST", "/mobile/sessions"), self.ctx.routes)
        self.assertIn(("GET", "/mobile/sessions/{session_id}/messages"), self.ctx.routes)
        self.assertIn(("POST", "/mobile/sessions/{session_id}/messages"), self.ctx.routes)
        self.assertIn(("POST", "/mobile/sessions/{session_id}/abort"), self.ctx.routes)
        self.assertIn(("POST", "/mobile/push/register"), self.ctx.routes)
        self.assertIn(("GET", "/mobile/devices"), self.ctx.routes)
        self.assertIn(("POST", "/mobile/devices/{device_id}/revoke"), self.ctx.routes)
        self.assertIn(("POST", "/mobile/uploads"), self.ctx.routes)

    async def test_capabilities(self):
        handler = self.route("GET", "/mobile/capabilities")
        status, payload = response_json(await handler(self.request("GET", "/mobile/capabilities")))
        self.assertEqual(status, 200)
        self.assertEqual(payload["ok"], True)
        self.assertEqual(payload["profile_name"], "default")
        self.assertEqual(payload["available_profiles"], [])
        self.assertEqual(payload["profiles_discoverable"], False)
        self.assertEqual(payload["features"]["pairing"], True)
        self.assertEqual(payload["features"]["auth_refresh"], True)
        self.assertEqual(payload["features"]["sessions_list"], True)
        self.assertEqual(payload["features"]["sessions_create"], True)
        self.assertEqual(payload["features"]["messages_send"], True)
        self.assertEqual(payload["features"]["realtime_ws"], True)
        self.assertEqual(payload["pairing"]["code_format"], "XXXX-XXXX")
        self.assertEqual(payload["pairing"]["install_channel"], "stable")
        self.assertEqual(payload["realtime"]["transport"], "websocket")
        self.assertEqual(payload["realtime"]["path"], "/mobile/ws")
        self.assertEqual(payload["realtime"]["protocol_version"], 1)
        self.assertEqual(payload["scope"]["mode"], "profile_state_db")
        self.assertEqual(payload["scope"]["default_profile"], "default")

    async def test_pairing_and_me_and_refresh(self):
        start_payload = self.issue_pairing_code(profile_name="default")
        pairing_code = start_payload["pairing_code"]
        self.assertEqual(start_payload["install_channel"], "stable")

        pair_complete = self.route("POST", "/mobile/pair/complete")
        complete_status, complete_payload = response_json(
            await pair_complete(
                self.request(
                    "POST",
                    "/mobile/pair/complete",
                    body={
                        "pairing_code": pairing_code,
                        "device_name": "Bayram iPhone",
                        "device_public_key": "public-key-data",
                        "platform": "ios",
                        "app_version": "1.0",
                    },
                )
            )
        )
        self.assertEqual(complete_status, 200)
        access_token = complete_payload["access_token"]
        refresh_token = complete_payload["refresh_token"]

        me_handler = self.route("GET", "/mobile/me")
        unauthorized_status, _ = response_json(await me_handler(self.request("GET", "/mobile/me")))
        self.assertEqual(unauthorized_status, 401)

        me_status, me_payload = response_json(
            await me_handler(
                self.request(
                    "GET",
                    "/mobile/me",
                    headers={"Authorization": f"Bearer {access_token}"},
                )
            )
        )
        self.assertEqual(me_status, 200)
        self.assertEqual(me_payload["device_name"], "Bayram iPhone")

        refresh_handler = self.route("POST", "/mobile/auth/refresh")
        refresh_status, refresh_payload = response_json(
            await refresh_handler(
                self.request(
                    "POST",
                    "/mobile/auth/refresh",
                    body={"refresh_token": refresh_token},
                )
            )
        )
        self.assertEqual(refresh_status, 200)
        self.assertNotEqual(refresh_payload["refresh_token"], refresh_token)

        stale_status, _ = response_json(
            await refresh_handler(
                self.request(
                    "POST",
                    "/mobile/auth/refresh",
                    body={"refresh_token": refresh_token},
                )
            )
        )
        self.assertEqual(stale_status, 401)

    async def test_pair_start_validation_and_migrations(self):
        pair_start = self.route("POST", "/mobile/pair/start")
        missing_status, missing_payload = response_json(
            await pair_start(
                self.request(
                    "POST",
                    "/mobile/pair/start",
                    body={"profile_name": "   "},
                )
            )
        )
        self.assertEqual(missing_status, 403)
        self.assertEqual(missing_payload["error"]["code"], "pairing_code_generation_disabled")

        self.env.create_profile("other")
        other_payload = self.issue_pairing_code(profile_name="other")

        pair_complete = self.route("POST", "/mobile/pair/complete")
        missing_payload_status, _ = response_json(
            await pair_complete(
                self.request(
                    "POST",
                    "/mobile/pair/complete",
                    body={"pairing_code": other_payload["pairing_code"]},
                )
            )
        )
        self.assertEqual(missing_payload_status, 400)

        complete_status, complete_payload = response_json(
            await pair_complete(
                self.request(
                    "POST",
                    "/mobile/pair/complete",
                    body={
                        "pairing_code": other_payload["pairing_code"],
                        "device_name": "Other Phone",
                        "device_public_key": "other-pk",
                        "platform": "ios",
                    },
                )
            )
        )
        self.assertEqual(complete_status, 200)
        self.assertEqual(complete_payload["profile_name"], "other")

        reuse_status, _ = response_json(
            await pair_complete(
                self.request(
                    "POST",
                    "/mobile/pair/complete",
                    body={
                        "pairing_code": other_payload["pairing_code"],
                        "device_name": "Other Phone",
                        "device_public_key": "other-pk",
                        "platform": "ios",
                    },
                )
            )
        )
        self.assertEqual(reuse_status, 404)

        conn = sqlite3.connect(self.env.db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT value FROM mobile_meta WHERE key = 'schema_version'"
        ).fetchone()
        device_row = conn.execute(
            """
            SELECT platform, app_version
            FROM mobile_devices
            WHERE id = ?
            """,
            (complete_payload["device_id"],),
        ).fetchone()
        conn.close()
        self.assertIsNotNone(row)
        self.assertEqual(int(row["value"]), 6)
        self.assertEqual(device_row["platform"], "ios")
        self.assertIsNone(device_row["app_version"])

    async def test_pairing_code_format_and_expiry(self):
        payload = self.issue_pairing_code(profile_name="default", ttl_seconds=1)
        self.assertRegex(payload["pairing_code"], r"^[23456789ABCDEFGHJKMNPQRSTUVWXYZ]{4}-[23456789ABCDEFGHJKMNPQRSTUVWXYZ]{4}$")

        conn = sqlite3.connect(self.env.db_path)
        conn.execute(
            "UPDATE mobile_pairing_codes SET expires_at = 0 WHERE pairing_code = ?",
            (payload["pairing_code"],),
        )
        conn.commit()
        conn.close()

        pair_complete = self.route("POST", "/mobile/pair/complete")
        expired_status, expired_payload = response_json(
            await pair_complete(
                self.request(
                    "POST",
                    "/mobile/pair/complete",
                    body={
                        "pairing_code": payload["pairing_code"],
                        "device_name": "Expired Phone",
                        "device_public_key": "expired-pk",
                        "platform": "ios",
                    },
                )
            )
        )
        self.assertEqual(expired_status, 404)
        self.assertEqual(expired_payload["error"]["code"], "not_found")


if __name__ == "__main__":
    unittest.main()
