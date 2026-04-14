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
        self.assertIn("mobile", self.ctx.cli_commands)

    def test_install_or_verify_reports_stable_channel_and_plugin_identity(self):
        handler = self.ctx.tools["mobile_install_or_verify"]["handler"]

        payload = handler({})

        self.assertEqual(payload["ok"], True)
        self.assertEqual(payload["status"], "verified")
        self.assertEqual(payload["install_channel"], "stable")
        self.assertEqual(payload["repository"], "github.com/bilalbayram/hermes-mobile")
        self.assertEqual(payload["version"], "0.1.0")

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


if __name__ == "__main__":
    unittest.main()
