from __future__ import annotations

import argparse
import contextlib
import inspect
import io
import json
import unittest

from hermes_mobile import register
from hermes_mobile.config import MobilePluginConfig
from hermes_mobile.operator_surface import MobileOperatorSurface
from helpers import EnvHarness, FakeContext, FakeProfileRuntime


class FakePairingCodeStore:
    def __init__(self):
        self.calls: list[dict[str, object]] = []

    def create_pairing_code(
        self,
        *,
        profile_name: str,
        ttl_seconds: int,
        install_channel: str,
    ) -> dict[str, object]:
        self.calls.append(
            {
                "profile_name": profile_name,
                "ttl_seconds": ttl_seconds,
                "install_channel": install_channel,
            }
        )
        return {
            "pairing_code": "ABCD-WXYZ",
            "install_channel": install_channel,
            "expires_at": 1_700_000_000,
        }


class HermesMobileCommandSurfaceTests(unittest.IsolatedAsyncioTestCase):
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

    def test_registers_mobile_tools_and_cli_command(self):
        self.assertIn("mobile_generate_pairing_code", self.ctx.tools)
        self.assertIn("mobile_install_or_verify", self.ctx.tools)
        self.assertIn("mobile_prepare_connection_bundle", self.ctx.tools)
        self.assertIn("mobile_get_notification_policy", self.ctx.tools)
        self.assertIn("mobile_set_notification_policy", self.ctx.tools)
        self.assertIn("mobile_send_inbox_item", self.ctx.tools)
        self.assertIn("mobile_list_inbox_items", self.ctx.tools)
        self.assertIn("mobile_notify", self.ctx.tools)
        self.assertIn("mobile", self.ctx.cli_commands)
        self.assertIn("talaria-mobile", self.ctx.cli_commands)

    def test_install_or_verify_reports_stable_channel_and_plugin_identity(self):
        handler = self.ctx.tools["mobile_install_or_verify"]["handler"]

        payload = handler({})

        self.assertEqual(payload["ok"], True)
        self.assertEqual(payload["status"], "verified")
        self.assertEqual(payload["install_channel"], "stable")
        self.assertEqual(payload["repository"], "github.com/bilalbayram/hermes-mobile")
        self.assertEqual(payload["version"], "0.2.1")

    def test_install_or_verify_rejects_non_stable_channel(self):
        handler = self.ctx.tools["mobile_install_or_verify"]["handler"]

        payload = handler({"channel": "beta"})

        self.assertEqual(
            payload,
            {
                "ok": False,
                "error": "unsupported_channel",
                "message": "channel 'beta' is not supported",
            },
        )

    def test_generate_pairing_code_uses_store_pairing_code_path(self):
        handler = self.ctx.tools["mobile_generate_pairing_code"]["handler"]

        payload = handler({})

        self.assertEqual(payload["ok"], True)
        self.assertEqual(payload["profile_name"], "default")
        self.assertRegex(payload["pairing_code"], r"^[A-Z2-9]{4}-[A-Z2-9]{4}$")
        self.assertEqual(payload["install_channel"], "stable")
        self.assertGreater(payload["expires_at"], payload["created_at"])

    def test_generate_pairing_code_uses_real_issuer_when_available(self):
        store = FakePairingCodeStore()
        surface = MobileOperatorSurface(
            config=MobilePluginConfig(),
            store=store,  # type: ignore[arg-type]
            profile_runtime=FakeProfileRuntime(str(self.env.hermes_home)),
        )

        payload = surface.generate_pairing_code()

        self.assertEqual(payload["ok"], True)
        self.assertEqual(payload["profile_name"], "default")
        self.assertEqual(payload["pairing_code"], "ABCD-WXYZ")
        self.assertEqual(payload["install_channel"], "stable")
        self.assertEqual(payload["created_at"], 1_699_999_400)
        self.assertEqual(payload["expires_at"], 1_700_000_000)
        self.assertEqual(
            store.calls,
            [
                {
                    "profile_name": "default",
                    "ttl_seconds": 600,
                    "install_channel": "stable",
                }
            ],
        )

    def test_generate_pairing_code_rejects_unknown_profile(self):
        surface = MobileOperatorSurface(
            config=MobilePluginConfig(),
            store=FakePairingCodeStore(),  # type: ignore[arg-type]
            profile_runtime=FakeProfileRuntime(str(self.env.hermes_home)),
        )

        payload = surface.generate_pairing_code("missing")

        self.assertEqual(
            payload,
            {
                "ok": False,
                "error": "profile_not_found",
                "message": "profile 'missing' does not exist",
            },
        )

    def test_prepare_talaria_connection_bundle_formats_block_and_json(self):
        surface = MobileOperatorSurface(
            config=MobilePluginConfig(),
            store=FakePairingCodeStore(),  # type: ignore[arg-type]
            profile_runtime=FakeProfileRuntime(str(self.env.hermes_home)),
        )
        surface.install_or_verify = lambda channel=None: {
            "ok": True,
            "status": "verified",
            "install_channel": channel or "stable",
            "repository": "github.com/bilalbayram/hermes-mobile",
            "version": "0.2.1",
        }
        surface.generate_pairing_code = lambda profile_name=None: {
            "ok": True,
            "profile_name": profile_name or "default",
            "pairing_code": "ABCD-WXYZ",
            "install_channel": "stable",
            "created_at": 1_699_999_400,
            "expires_at": 1_700_000_000,
        }

        payload = surface.prepare_talaria_connection_bundle(
            server_name="Home Hermes",
            base_url="https://hermes.example.ts.net/",
            profile_name="work",
            connection_mode="tailscale",
        )

        self.assertEqual(payload["ok"], True)
        self.assertEqual(payload["install"]["status"], "verified")
        self.assertEqual(payload["bundle"]["server_name"], "Home Hermes")
        self.assertEqual(payload["bundle"]["base_url"], "https://hermes.example.ts.net")
        self.assertEqual(payload["bundle"]["profile_name"], "work")
        self.assertEqual(payload["bundle"]["pairing_code"], "ABCD-WXYZ")
        self.assertEqual(payload["bundle"]["connection_mode"], "tailscale")
        self.assertEqual(payload["bundle"]["expires_at"], 1_700_000_000)
        self.assertIn("TALARIA-CONNECT", payload["bundle_text"])
        self.assertIn("Server: Home Hermes", payload["bundle_text"])
        self.assertIn("URL: https://hermes.example.ts.net", payload["bundle_text"])
        self.assertIn("Profile: work", payload["bundle_text"])
        self.assertIn("Code: ABCD-WXYZ", payload["bundle_text"])
        self.assertEqual(payload["bundle_json"], payload["bundle"])

    def test_prepare_talaria_connection_bundle_rejects_non_https_urls(self):
        surface = MobileOperatorSurface(
            config=MobilePluginConfig(),
            store=FakePairingCodeStore(),  # type: ignore[arg-type]
            profile_runtime=FakeProfileRuntime(str(self.env.hermes_home)),
        )

        payload = surface.prepare_talaria_connection_bundle(
            base_url="http://127.0.0.1:8642",
        )

        self.assertEqual(
            payload,
            {
                "ok": False,
                "error": "invalid_base_url",
                "message": "base_url must be an https URL",
            },
        )

    def test_prepare_connection_bundle_returns_text_and_json_bundle(self):
        handler = self.ctx.tools["mobile_prepare_connection_bundle"]["handler"]

        payload = handler(
            {
                "base_url": "https://hermes.example.com:8642",
                "server_name": "Home Hermes",
                "profile_name": "default",
                "connection_mode": "tailscale",
            }
        )

        self.assertEqual(payload["ok"], True)
        self.assertEqual(payload["bundle_format"], "talaria-connect-v1")
        self.assertEqual(payload["server_name"], "Home Hermes")
        self.assertEqual(payload["base_url"], "https://hermes.example.com:8642")
        self.assertEqual(payload["profile_name"], "default")
        self.assertRegex(payload["pairing_code"], r"^[A-Z2-9]{4}-[A-Z2-9]{4}$")
        self.assertEqual(payload["connection_mode"], "tailscale")
        self.assertIn("TALARIA-CONNECT", payload["bundle_text"])
        self.assertEqual(payload["bundle_json"]["server_name"], "Home Hermes")
        self.assertEqual(payload["bundle_json"]["base_url"], "https://hermes.example.com:8642")
        self.assertEqual(payload["bundle_json"]["profile_name"], "default")
        self.assertEqual(payload["bundle_json"]["pairing_code"], payload["pairing_code"])

    def test_prepare_connection_bundle_rejects_non_https_url(self):
        handler = self.ctx.tools["mobile_prepare_connection_bundle"]["handler"]

        payload = handler(
            {
                "base_url": "http://192.168.1.20:8642",
            }
        )

        self.assertEqual(
            payload,
            {
                "ok": False,
                "error": "invalid_base_url",
                "message": "base_url must be an https URL",
            },
        )

    def test_prepare_connection_bundle_rejects_urls_with_credentials(self):
        handler = self.ctx.tools["mobile_prepare_connection_bundle"]["handler"]

        payload = handler(
            {
                "base_url": "https://user:pass@hermes.example.com",
            }
        )

        self.assertEqual(
            payload,
            {
                "ok": False,
                "error": "invalid_base_url",
                "message": "base_url must be an https URL",
            },
        )

    def test_notification_policy_round_trip_uses_store(self):
        set_handler = self.ctx.tools["mobile_set_notification_policy"]["handler"]
        get_handler = self.ctx.tools["mobile_get_notification_policy"]["handler"]

        set_payload = set_handler(
            {
                "event_type": "run.waiting",
                "delivery_mode": "alert_and_inbox",
                "enabled": True,
            }
        )
        get_payload = get_handler({})

        self.assertEqual(set_payload["ok"], True)
        self.assertEqual(set_payload["policy"]["event_type"], "run.waiting")
        self.assertEqual(set_payload["policy"]["delivery_mode"], "alert_and_inbox")
        self.assertEqual(get_payload["ok"], True)
        self.assertEqual(len(get_payload["policies"]), 1)
        self.assertEqual(get_payload["policies"][0]["event_type"], "run.waiting")

    def test_send_inbox_item_creates_durable_item(self):
        send_handler = self.ctx.tools["mobile_send_inbox_item"]["handler"]
        list_handler = self.ctx.tools["mobile_list_inbox_items"]["handler"]

        send_payload = send_handler(
            {
                "title": "Need your answer",
                "body": "Which deployment target should I use?",
                "kind": "run.waiting",
                "session_id": "session-1",
                "deep_link_target": "session:session-1",
            }
        )
        list_payload = list_handler({})

        self.assertEqual(send_payload["ok"], True)
        self.assertEqual(send_payload["item"]["kind"], "run.waiting")
        self.assertEqual(send_payload["item"]["session_id"], "session-1")
        self.assertEqual(list_payload["ok"], True)
        self.assertEqual(len(list_payload["items"]), 1)
        self.assertEqual(list_payload["items"][0]["title"], "Need your answer")

    def test_cli_command_emits_json_for_pairing_code(self):
        entry = self.ctx.cli_commands["mobile"]
        parser = argparse.ArgumentParser()
        entry["setup_fn"](parser)
        args = parser.parse_args(["generate-pairing-code", "--target-profile", "default"])

        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            code = entry["handler_fn"](args)

        self.assertEqual(code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["ok"], True)
        self.assertEqual(payload["profile_name"], "default")
        self.assertEqual(payload["install_channel"], "stable")

    def test_cli_command_emits_json_for_install_or_verify(self):
        entry = self.ctx.cli_commands["mobile"]
        parser = argparse.ArgumentParser()
        entry["setup_fn"](parser)
        args = parser.parse_args(["install-or-verify", "--channel", "stable"])

        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            code = entry["handler_fn"](args)

        self.assertEqual(code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["ok"], True)
        self.assertEqual(payload["status"], "verified")
        self.assertEqual(payload["install_channel"], "stable")
        self.assertEqual(payload["repository"], "github.com/bilalbayram/hermes-mobile")

    def test_cli_command_emits_json_for_connection_bundle(self):
        entry = self.ctx.cli_commands["mobile"]
        parser = argparse.ArgumentParser()
        entry["setup_fn"](parser)
        args = parser.parse_args(
            [
                "prepare-connection-bundle",
                "--base-url",
                "https://hermes.example.com",
                "--server-name",
                "Home Hermes",
                "--target-profile",
                "default",
                "--connection-mode",
                "tailscale",
            ]
        )

        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            code = entry["handler_fn"](args)

        self.assertEqual(code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["ok"], True)
        self.assertEqual(payload["server_name"], "Home Hermes")
        self.assertEqual(payload["base_url"], "https://hermes.example.com")
        self.assertEqual(payload["profile_name"], "default")
        self.assertEqual(payload["connection_mode"], "tailscale")
        self.assertIn("TALARIA-CONNECT", payload["bundle_text"])

    def test_cli_command_emits_json_for_talaria_connection_bundle(self):
        entry = self.ctx.cli_commands["mobile"]
        parser = argparse.ArgumentParser()
        entry["setup_fn"](parser)
        args = parser.parse_args(
            [
                "prepare-talaria-connection-bundle",
                "--server-name",
                "Home Hermes",
                "--base-url",
                "https://hermes.example.ts.net",
                "--target-profile",
                "default",
            ]
        )

        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            code = entry["handler_fn"](args)

        self.assertEqual(code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["ok"], True)
        self.assertEqual(payload["bundle"]["server_name"], "Home Hermes")
        self.assertEqual(payload["bundle"]["base_url"], "https://hermes.example.ts.net")
        self.assertEqual(payload["bundle"]["profile_name"], "default")


if __name__ == "__main__":
    unittest.main()
