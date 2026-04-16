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
    if parsed.username or parsed.password:
        return None
    host = parsed.hostname or ""
    if not host:
        return None
    netloc = host
    if parsed.port:
        netloc = f"{host}:{parsed.port}"
    normalized = f"https://{netloc}{parsed.path or ''}"
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

    def get_notification_policy(
        self,
        *,
        profile_name: str | None = None,
        device_id: str | None = None,
    ) -> dict[str, Any]:
        resolved_profile = str(profile_name or "").strip() or self.config.profile_name
        return {
            "ok": True,
            "profile_name": resolved_profile,
            "device_id": str(device_id or "").strip() or None,
            "policies": self.store.list_notification_policies(
                profile_name=resolved_profile,
                device_id=device_id,
            ),
        }

    def set_notification_policy(
        self,
        *,
        event_type: str,
        delivery_mode: str,
        enabled: bool = True,
        profile_name: str | None = None,
        device_id: str | None = None,
    ) -> dict[str, Any]:
        resolved_profile = str(profile_name or "").strip() or self.config.profile_name
        normalized_event_type = str(event_type or "").strip()
        normalized_delivery_mode = str(delivery_mode or "").strip()
        if not normalized_event_type:
            return _error_payload("bad_request", "event_type is required")
        if not normalized_delivery_mode:
            return _error_payload("bad_request", "delivery_mode is required")
        policy = self.store.set_notification_policy(
            profile_name=resolved_profile,
            event_type=normalized_event_type,
            delivery_mode=normalized_delivery_mode,
            enabled=bool(enabled),
            device_id=device_id,
        )
        return {
            "ok": True,
            "policy": policy,
        }

    def send_inbox_item(
        self,
        *,
        title: str,
        body: str,
        kind: str = "agent.message",
        session_id: str | None = None,
        deep_link_target: str | None = None,
        profile_name: str | None = None,
        device_id: str | None = None,
    ) -> dict[str, Any]:
        resolved_profile = str(profile_name or "").strip() or self.config.profile_name
        normalized_title = str(title or "").strip()
        normalized_body = str(body or "").strip()
        if not normalized_title:
            return _error_payload("bad_request", "title is required")
        if not normalized_body:
            return _error_payload("bad_request", "body is required")
        item = self.store.create_inbox_item(
            profile_name=resolved_profile,
            kind=str(kind or "agent.message").strip() or "agent.message",
            title=normalized_title,
            body=normalized_body,
            session_id=str(session_id or "").strip() or None,
            deep_link_target=str(deep_link_target or "").strip() or None,
            device_id=str(device_id or "").strip() or None,
        )
        return {
            "ok": True,
            "item": item,
        }

    def list_inbox_items(
        self,
        *,
        profile_name: str | None = None,
        device_id: str | None = None,
        unread_only: bool = False,
    ) -> dict[str, Any]:
        resolved_profile = str(profile_name or "").strip() or self.config.profile_name
        return {
            "ok": True,
            "profile_name": resolved_profile,
            "device_id": str(device_id or "").strip() or None,
            "items": self.store.list_inbox_items(
                profile_name=resolved_profile,
                device_id=device_id,
                unread_only=bool(unread_only),
            ),
        }

    def notify(
        self,
        *,
        title: str,
        body: str,
        kind: str = "agent.message",
        session_id: str | None = None,
        deep_link_target: str | None = None,
        profile_name: str | None = None,
        device_id: str | None = None,
    ) -> dict[str, Any]:
        return self.send_inbox_item(
            title=title,
            body=body,
            kind=kind,
            session_id=session_id,
            deep_link_target=deep_link_target,
            profile_name=profile_name,
            device_id=device_id,
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


def _notification_policy_get_tool_schema() -> dict[str, Any]:
    return {
        "name": "mobile_get_notification_policy",
        "description": "List hermes-mobile notification policy rows for a profile or device.",
        "parameters": {
            "type": "object",
            "properties": {
                "profile_name": {"type": "string"},
                "device_id": {"type": "string"},
            },
        },
    }


def _notification_policy_set_tool_schema() -> dict[str, Any]:
    return {
        "name": "mobile_set_notification_policy",
        "description": "Set a hermes-mobile notification delivery rule for an event type.",
        "parameters": {
            "type": "object",
            "properties": {
                "profile_name": {"type": "string"},
                "device_id": {"type": "string"},
                "event_type": {"type": "string"},
                "delivery_mode": {"type": "string"},
                "enabled": {"type": "boolean"},
            },
            "required": ["event_type", "delivery_mode"],
        },
    }


def _send_inbox_item_tool_schema() -> dict[str, Any]:
    return {
        "name": "mobile_send_inbox_item",
        "description": "Create a durable Talaria inbox item for a profile or device.",
        "parameters": {
            "type": "object",
            "properties": {
                "profile_name": {"type": "string"},
                "device_id": {"type": "string"},
                "session_id": {"type": "string"},
                "deep_link_target": {"type": "string"},
                "kind": {"type": "string"},
                "title": {"type": "string"},
                "body": {"type": "string"},
            },
            "required": ["title", "body"],
        },
    }


def _list_inbox_items_tool_schema() -> dict[str, Any]:
    return {
        "name": "mobile_list_inbox_items",
        "description": "List durable Talaria inbox items.",
        "parameters": {
            "type": "object",
            "properties": {
                "profile_name": {"type": "string"},
                "device_id": {"type": "string"},
                "unread_only": {"type": "boolean"},
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
        register_tool(
            name="mobile_get_notification_policy",
            toolset=PLUGIN_TOOLSET,
            schema=_notification_policy_get_tool_schema(),
            handler=lambda args, **_kwargs: surface.get_notification_policy(
                profile_name=(args or {}).get("profile_name"),
                device_id=(args or {}).get("device_id"),
            ),
            description="List hermes-mobile notification policy rows.",
        )
        register_tool(
            name="mobile_set_notification_policy",
            toolset=PLUGIN_TOOLSET,
            schema=_notification_policy_set_tool_schema(),
            handler=lambda args, **_kwargs: surface.set_notification_policy(
                profile_name=(args or {}).get("profile_name"),
                device_id=(args or {}).get("device_id"),
                event_type=(args or {}).get("event_type", ""),
                delivery_mode=(args or {}).get("delivery_mode", ""),
                enabled=bool((args or {}).get("enabled", True)),
            ),
            description="Set a hermes-mobile notification rule.",
        )
        register_tool(
            name="mobile_send_inbox_item",
            toolset=PLUGIN_TOOLSET,
            schema=_send_inbox_item_tool_schema(),
            handler=lambda args, **_kwargs: surface.send_inbox_item(
                profile_name=(args or {}).get("profile_name"),
                device_id=(args or {}).get("device_id"),
                session_id=(args or {}).get("session_id"),
                deep_link_target=(args or {}).get("deep_link_target"),
                kind=(args or {}).get("kind", "agent.message"),
                title=(args or {}).get("title", ""),
                body=(args or {}).get("body", ""),
            ),
            description="Create a durable Talaria inbox item.",
        )
        register_tool(
            name="mobile_list_inbox_items",
            toolset=PLUGIN_TOOLSET,
            schema=_list_inbox_items_tool_schema(),
            handler=lambda args, **_kwargs: surface.list_inbox_items(
                profile_name=(args or {}).get("profile_name"),
                device_id=(args or {}).get("device_id"),
                unread_only=bool((args or {}).get("unread_only", False)),
            ),
            description="List durable Talaria inbox items.",
        )
        register_tool(
            name="mobile_notify",
            toolset=PLUGIN_TOOLSET,
            schema=_send_inbox_item_tool_schema(),
            handler=lambda args, **_kwargs: surface.notify(
                profile_name=(args or {}).get("profile_name"),
                device_id=(args or {}).get("device_id"),
                session_id=(args or {}).get("session_id"),
                deep_link_target=(args or {}).get("deep_link_target"),
                kind=(args or {}).get("kind", "agent.message"),
                title=(args or {}).get("title", ""),
                body=(args or {}).get("body", ""),
            ),
            description="Create a durable Talaria inbox item for agent notification use.",
        )

    register_cli_command = getattr(context, "register_cli_command", None)
    if callable(register_cli_command):
        for command_name in ("mobile", "talaria-mobile"):
            register_cli_command(
                name=command_name,
                help="Manage hermes-mobile install state and pairing codes",
                setup_fn=_register_cli,
                handler_fn=lambda args: _cli_handler(surface, args),
                description="Operator-facing hermes-mobile commands.",
            )
