from __future__ import annotations

import sqlite3
from typing import Any, Callable, Protocol

from .config import MobilePluginConfig
from .migrations import run_migrations
from .operator_surface import PLUGIN_VERSION, MobileOperatorSurface, register_operator_surface
from .push import create_push_sender
from .runtime import HermesProfileRuntime
from .routes import MobileRoutes
from .store import MobileAuthStore


class PluginContext(Protocol):
    def register_http_route(
        self, method: str, path: str, handler: Callable[..., Any]
    ) -> None: ...

    def register_tool(
        self,
        name: str,
        toolset: str,
        schema: dict[str, Any],
        handler: Callable[..., Any],
        **kwargs: Any,
    ) -> None: ...

    def register_cli_command(
        self,
        name: str,
        help: str,
        setup_fn: Callable[..., Any],
        handler_fn: Callable[..., Any] | None = None,
        description: str = "",
    ) -> None: ...

    def register_startup_callback(self, callback: Callable[[], None]) -> None: ...

    def register_shutdown_callback(self, callback: Callable[[], None]) -> None: ...


def _open_connection(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, check_same_thread=False, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def register(context: PluginContext) -> dict[str, str]:
    config = MobilePluginConfig.from_env()
    config.state_db_path.parent.mkdir(parents=True, exist_ok=True)
    config.upload_dir.mkdir(parents=True, exist_ok=True)
    conn = _open_connection(str(config.state_db_path))
    store = MobileAuthStore(conn=conn)
    sender_factory = getattr(context, "create_push_sender", None)
    push_sender = sender_factory(config, store) if callable(sender_factory) else create_push_sender(config)
    runtime_factory = getattr(context, "create_profile_runtime", None)
    profile_runtime = runtime_factory(config) if callable(runtime_factory) else HermesProfileRuntime(config.hermes_root)
    routes = MobileRoutes(
        config=config,
        store=store,
        push_sender=push_sender,
        profile_runtime=profile_runtime,
    )
    operator_surface = MobileOperatorSurface(
        config=config,
        store=store,
        profile_runtime=profile_runtime,
    )

    def startup_callback() -> None:
        run_migrations(conn)

    async def shutdown_callback() -> None:
        await routes.shutdown()
        store.close()
        conn.close()

    context.register_startup_callback(startup_callback)
    if hasattr(context, "register_shutdown_callback"):
        context.register_shutdown_callback(shutdown_callback)
    routes.register(context.register_http_route)
    register_operator_surface(context, operator_surface)
    return {
        "name": "hermes_mobile",
        "version": PLUGIN_VERSION,
        "state_db_path": str(config.state_db_path),
    }
