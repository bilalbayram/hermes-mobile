from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _get_hermes_home() -> Path:
    try:
        from hermes_constants import get_hermes_home  # type: ignore

        return get_hermes_home()
    except Exception:
        return Path(os.getenv("HERMES_HOME", Path.home() / ".hermes"))


def _get_hermes_root() -> Path:
    try:
        from hermes_constants import get_default_hermes_root  # type: ignore

        return get_default_hermes_root()
    except Exception:
        home = _get_hermes_home()
        if home.parent.name == "profiles":
            return home.parent.parent
        return home


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    return int(raw)


def _bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name, "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class MobilePluginConfig:
    profile_name: str = "default"
    hermes_root: Path = Path(".")
    pairing_code_ttl_seconds: int = 600
    access_token_ttl_seconds: int = 3600
    refresh_token_ttl_seconds: int = 60 * 60 * 24 * 30
    state_db_path: Path = Path("state.db")
    push_enabled: bool = False
    apns_topic: str = ""
    apns_team_id: str = ""
    apns_key_id: str = ""
    apns_auth_key_path: Path | None = None
    upload_max_bytes: int = 20 * 1024 * 1024
    upload_dir: Path = Path("mobile_uploads")

    @classmethod
    def from_env(cls) -> "MobilePluginConfig":
        hermes_home = _get_hermes_home()
        hermes_root = _get_hermes_root()
        explicit_db_path = os.getenv("HERMES_MOBILE_DB_PATH", "").strip()
        state_db_path = Path(explicit_db_path) if explicit_db_path else hermes_home / "state.db"
        apns_topic = os.getenv("HERMES_MOBILE_APNS_TOPIC", "").strip()
        apns_team_id = os.getenv("HERMES_MOBILE_APNS_TEAM_ID", "").strip()
        apns_key_id = os.getenv("HERMES_MOBILE_APNS_KEY_ID", "").strip()
        apns_auth_key_path = os.getenv("HERMES_MOBILE_APNS_AUTH_KEY_PATH", "").strip()
        apns_auth_key = Path(apns_auth_key_path) if apns_auth_key_path else None
        creds_present = bool(
            apns_topic
            and apns_team_id
            and apns_key_id
            and apns_auth_key is not None
            and apns_auth_key.exists()
        )
        push_env = os.getenv("HERMES_MOBILE_PUSH_ENABLED", "").strip()
        if push_env:
            push_enabled = _bool_env("HERMES_MOBILE_PUSH_ENABLED", creds_present) and creds_present
        else:
            push_enabled = creds_present
        explicit_upload_dir = os.getenv("HERMES_MOBILE_UPLOAD_DIR", "").strip()
        upload_dir = Path(explicit_upload_dir) if explicit_upload_dir else hermes_home / "mobile_uploads"
        return cls(
            profile_name=os.getenv("HERMES_MOBILE_PROFILE_NAME", "default").strip() or "default",
            hermes_root=hermes_root,
            pairing_code_ttl_seconds=_int_env("HERMES_MOBILE_PAIRING_CODE_TTL_SECONDS", 600),
            access_token_ttl_seconds=_int_env("HERMES_MOBILE_ACCESS_TOKEN_TTL_SECONDS", 3600),
            refresh_token_ttl_seconds=_int_env(
                "HERMES_MOBILE_REFRESH_TOKEN_TTL_SECONDS",
                60 * 60 * 24 * 30,
            ),
            state_db_path=state_db_path,
            push_enabled=push_enabled,
            apns_topic=apns_topic,
            apns_team_id=apns_team_id,
            apns_key_id=apns_key_id,
            apns_auth_key_path=apns_auth_key,
            upload_max_bytes=max(1, _int_env("HERMES_MOBILE_UPLOAD_MAX_BYTES", 20 * 1024 * 1024)),
            upload_dir=upload_dir,
        )
