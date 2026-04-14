from __future__ import annotations

import argparse
import json
from typing import Any

from .config import MobilePluginConfig
from .runtime import HermesProfileRuntime
from .store import MobileAuthStore

PLUGIN_VERSION = "0.1.0"
PLUGIN_TOOLSET = "plugin_hermes_mobile"
STABLE_CHANNEL = "stable"
PLUGIN_REPOSITORY = "github.com/bilalbayram/hermes-mobile"


def _error_payload(error: str, message: str) -> dict[str, Any]:
    return {
        "ok": False,
        "error": error,
        "message": message,
    }


def _display_pairing_code(value: str) -> str:
    compact = str(value or "").strip().replace("-", "").upper()
    if len(compact) != 8:
        return compact
    return f"{compact[:4]}-{compact[4:]}"


class MobileOperatorSurface:
    def __init__(
        self,
        *,
        config: MobilePluginConfig,
        store: MobileAuthStore,
        profile_runtime: HermesProfileRuntime,
    ):
        self.config = config
        self.store = store
        self.profile_runtime = profile_runtime

    def generate_pairing_code(self, profile_name: str | None = None) -> dict[str, Any]:
        resolved_profile = str(profile_name or "").strip() or self.config.profile_name
        profiles = self.profile_runtime.list_profiles()
        if resolved_profile not in profiles:
            return _error_payload(
                "profile_not_found",
                f"profile '{resolved_profile}' does not exist",
            )

        created = self.store.create_pairing_code(
            profile_name=resolved_profile,
            ttl_seconds=self.config.pairing_code_ttl_seconds,
            install_channel=STABLE_CHANNEL,
        )
        if not created:
            return _error_payload(
                "pairing_code_unavailable",
                "failed to generate pairing code",
            )
        pairing_code = str(created.get("pairing_code", "")).strip()
        expires_at = int(created.get("expires_at") or 0)
        created_at = max(0, expires_at - int(self.config.pairing_code_ttl_seconds))
        return {
            "ok": True,
            "profile_name": resolved_profile,
            "pairing_code": _display_pairing_code(pairing_code),
            "install_channel": str(created.get("install_channel") or STABLE_CHANNEL),
            "created_at": created_at,
            "expires_at": expires_at,
        }

    def install_or_verify(self, channel: str | None = None) -> dict[str, Any]:
        resolved_channel = str(channel or STABLE_CHANNEL).strip() or STABLE_CHANNEL
        if resolved_channel != STABLE_CHANNEL:
            return _error_payload(
                "unsupported_channel",
                f"channel '{resolved_channel}' is not supported",
            )
        return {
            "ok": True,
            "status": "verified",
            "install_channel": STABLE_CHANNEL,
            "repository": PLUGIN_REPOSITORY,
            "version": PLUGIN_VERSION,
        }


def _pairing_tool_schema() -> dict[str, Any]:
    return {
        "name": "mobile_generate_pairing_code",
        "description": "Generate a one-time Talaria pairing code for a Hermes profile.",
        "parameters": {
            "type": "object",
            "properties": {
                "profile_name": {
                    "type": "string",
                    "description": "Hermes profile to pair against.",
                }
            },
        },
    }


def _install_or_verify_tool_schema() -> dict[str, Any]:
    return {
        "name": "mobile_install_or_verify",
        "description": "Verify the loaded hermes-mobile plugin on the stable channel.",
        "parameters": {
            "type": "object",
            "properties": {
                "channel": {
                    "type": "string",
                    "description": "Install channel to verify.",
                    "enum": [STABLE_CHANNEL],
                }
            },
        },
    }


def _register_cli(parser: argparse.ArgumentParser) -> None:
    subparsers = parser.add_subparsers(dest="mobile_command")
    install = subparsers.add_parser(
        "install-or-verify",
        help="Verify hermes-mobile on the stable channel",
    )
    install.add_argument("--channel", default=STABLE_CHANNEL)
    pairing = subparsers.add_parser(
        "generate-pairing-code",
        help="Generate a one-time Talaria pairing code",
    )
    pairing.add_argument("--target-profile", default="default")


def _execute_cli_command(
    surface: MobileOperatorSurface,
    command: str | None,
    args: argparse.Namespace,
) -> dict[str, Any]:
    if command == "install-or-verify":
        return surface.install_or_verify(channel=getattr(args, "channel", STABLE_CHANNEL))
    if command == "generate-pairing-code":
        return surface.generate_pairing_code(
            profile_name=getattr(args, "target_profile", "default")
        )
    return _error_payload("unknown_command", "mobile command is required")


def _cli_handler(surface: MobileOperatorSurface, args: argparse.Namespace) -> int:
    payload = _execute_cli_command(surface, getattr(args, "mobile_command", None), args)
    print(json.dumps(payload))
    return 0 if payload.get("ok") else 1


def register_operator_surface(context: Any, surface: MobileOperatorSurface) -> None:
    register_tool = getattr(context, "register_tool", None)
    if callable(register_tool):
        register_tool(
            name="mobile_install_or_verify",
            toolset=PLUGIN_TOOLSET,
            schema=_install_or_verify_tool_schema(),
            handler=lambda args, **_kwargs: surface.install_or_verify(
                channel=(args or {}).get("channel")
            ),
            description="Verify the loaded hermes-mobile plugin on the stable channel.",
        )
        register_tool(
            name="mobile_generate_pairing_code",
            toolset=PLUGIN_TOOLSET,
            schema=_pairing_tool_schema(),
            handler=lambda args, **_kwargs: surface.generate_pairing_code(
                profile_name=(args or {}).get("profile_name")
            ),
            description="Generate a one-time Talaria pairing code.",
        )

    register_cli_command = getattr(context, "register_cli_command", None)
    if callable(register_cli_command):
        register_cli_command(
            name="mobile",
            help="Manage hermes-mobile install state and pairing codes",
            setup_fn=_register_cli,
            handler_fn=lambda args: _cli_handler(surface, args),
            description="Operator-facing hermes-mobile commands.",
        )
