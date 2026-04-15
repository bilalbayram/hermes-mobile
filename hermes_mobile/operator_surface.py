from __future__ import annotations

import argparse
import json
from urllib.parse import urlparse
from typing import Any

from .config import MobilePluginConfig
from .runtime import HermesProfileRuntime
from .store import MobileAuthStore

PLUGIN_VERSION = "0.1.0"
PLUGIN_TOOLSET = "plugin_hermes_mobile"
STABLE_CHANNEL = "stable"
PLUGIN_REPOSITORY = "github.com/bilalbayram/hermes-mobile"
BUNDLE_FORMAT = "talaria-connect-v1"
BUNDLE_PREFIX = "TALARIA-CONNECT"


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


def _normalize_https_url(value: str) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    parsed = urlparse(raw)
    if parsed.scheme.lower() != "https" or not parsed.netloc:
        return None
    normalized = f"https://{parsed.netloc}{parsed.path or ''}"
    return normalized.rstrip("/")


def _infer_connection_mode(base_url: str, explicit_mode: str | None = None) -> str | None:
    resolved_mode = str(explicit_mode or "").strip()
    if resolved_mode:
        return resolved_mode
    hostname = (urlparse(base_url).hostname or "").lower()
    if hostname.endswith(".ts.net"):
        return "tailscale"
    if hostname.endswith(".trycloudflare.com") or "cloudflared" in hostname:
        return "cloudflared"
    return None


def _render_connection_bundle(bundle: dict[str, Any]) -> str:
    lines = [
        BUNDLE_PREFIX,
        f"Server: {bundle['server_name']}",
        f"URL: {bundle['base_url']}",
        f"Profile: {bundle['profile_name']}",
        f"Code: {bundle['pairing_code']}",
    ]
    connection_mode = str(bundle.get("connection_mode", "")).strip()
    if connection_mode:
        lines.append(f"Mode: {connection_mode}")
    expires_at = bundle.get("expires_at")
    if expires_at:
        lines.append(f"Expires: {expires_at}")
    return "\n".join(lines)


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

    def prepare_connection_bundle(
        self,
        *,
        base_url: str,
        server_name: str | None = None,
        profile_name: str | None = None,
        connection_mode: str | None = None,
        channel: str | None = None,
    ) -> dict[str, Any]:
        normalized_base_url = _normalize_https_url(base_url)
        if normalized_base_url is None:
            return _error_payload(
                "invalid_base_url",
                "base_url must be an https URL",
            )

        install_payload = self.install_or_verify(channel=channel)
        if install_payload.get("ok") is not True:
            return install_payload

        pairing_payload = self.generate_pairing_code(profile_name=profile_name)
        if pairing_payload.get("ok") is not True:
            return pairing_payload

        resolved_profile = str(pairing_payload.get("profile_name") or self.config.profile_name)
        resolved_server_name = str(server_name or "").strip() or "Hermes"
        resolved_connection_mode = _infer_connection_mode(normalized_base_url, connection_mode)
        bundle = {
            "server_name": resolved_server_name,
            "base_url": normalized_base_url,
            "profile_name": resolved_profile,
            "pairing_code": str(pairing_payload.get("pairing_code") or ""),
            "expires_at": pairing_payload.get("expires_at"),
        }
        if resolved_connection_mode:
            bundle["connection_mode"] = resolved_connection_mode
        bundle_text = _render_connection_bundle(bundle)
        return {
            "ok": True,
            "bundle_format": BUNDLE_FORMAT,
            "server_name": resolved_server_name,
            "base_url": normalized_base_url,
            "profile_name": resolved_profile,
            "pairing_code": bundle["pairing_code"],
            "connection_mode": resolved_connection_mode,
            "expires_at": bundle["expires_at"],
            "install": install_payload,
            "bundle": bundle,
            "bundle_json": bundle,
            "bundle_text": bundle_text,
        }

    def prepare_talaria_connection_bundle(
        self,
        *,
        base_url: str,
        server_name: str | None = None,
        profile_name: str | None = None,
        connection_mode: str | None = None,
        channel: str | None = None,
    ) -> dict[str, Any]:
        return self.prepare_connection_bundle(
            base_url=base_url,
            server_name=server_name,
            profile_name=profile_name,
            connection_mode=connection_mode,
            channel=channel,
        )


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


def _connection_bundle_tool_schema() -> dict[str, Any]:
    return {
        "name": "mobile_prepare_connection_bundle",
        "description": "Verify hermes-mobile, generate a pairing code, and return a pasteable Talaria connection bundle.",
        "parameters": {
            "type": "object",
            "properties": {
                "base_url": {
                    "type": "string",
                    "description": "HTTPS base URL the iPhone should use for Hermes.",
                },
                "server_name": {
                    "type": "string",
                    "description": "User-facing server name to embed in the bundle.",
                },
                "profile_name": {
                    "type": "string",
                    "description": "Hermes profile to pair against.",
                },
                "connection_mode": {
                    "type": "string",
                    "description": "Connection style label such as tailscale, public_https, or tunnel.",
                },
                "channel": {
                    "type": "string",
                    "description": "Install channel to verify.",
                    "enum": [STABLE_CHANNEL],
                },
            },
            "required": ["base_url"],
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
    for command_name in ("prepare-connection-bundle", "prepare-talaria-connection-bundle"):
        prepare_bundle = subparsers.add_parser(
            command_name,
            help="Verify hermes-mobile and emit a Talaria connection bundle",
        )
        prepare_bundle.add_argument("--base-url", required=True)
        prepare_bundle.add_argument("--server-name", default="Hermes")
        prepare_bundle.add_argument("--target-profile", default="default")
        prepare_bundle.add_argument("--connection-mode", default="")
        prepare_bundle.add_argument("--channel", default=STABLE_CHANNEL)


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
    if command in {"prepare-connection-bundle", "prepare-talaria-connection-bundle"}:
        return surface.prepare_connection_bundle(
            base_url=getattr(args, "base_url", ""),
            server_name=getattr(args, "server_name", "Hermes"),
            profile_name=getattr(args, "target_profile", "default"),
            connection_mode=getattr(args, "connection_mode", ""),
            channel=getattr(args, "channel", STABLE_CHANNEL),
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
        register_tool(
            name="mobile_prepare_connection_bundle",
            toolset=PLUGIN_TOOLSET,
            schema=_connection_bundle_tool_schema(),
            handler=lambda args, **_kwargs: surface.prepare_connection_bundle(
                base_url=(args or {}).get("base_url", ""),
                server_name=(args or {}).get("server_name"),
                profile_name=(args or {}).get("profile_name"),
                connection_mode=(args or {}).get("connection_mode"),
                channel=(args or {}).get("channel"),
            ),
            description="Verify hermes-mobile, generate a pairing code, and return a pasteable Talaria connection bundle.",
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
