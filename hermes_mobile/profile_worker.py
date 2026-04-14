from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

_SCRIPT_DIR = Path(__file__).resolve().parent
sys.path = [
    entry
    for entry in sys.path
    if Path(entry or ".").resolve() != _SCRIPT_DIR
]


def _emit(event: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(event, ensure_ascii=False) + "\n")
    sys.stdout.flush()


def _load_request() -> dict[str, Any]:
    raw = sys.stdin.readline()
    if not raw:
        raise RuntimeError("worker request missing")
    payload = json.loads(raw)
    return payload if isinstance(payload, dict) else {}


def _resolve_gateway_model(config: dict[str, Any]) -> str:
    model_cfg = config.get("model", {})
    if isinstance(model_cfg, str) and model_cfg.strip():
        return model_cfg.strip()
    if isinstance(model_cfg, dict):
        value = model_cfg.get("default") or model_cfg.get("model")
        if isinstance(value, str) and value.strip():
            return value.strip()
    return "hermes-agent"


def main() -> int:
    request = _load_request()

    from run_agent import AIAgent  # type: ignore
    from gateway.run import _load_gateway_config, _resolve_runtime_agent_kwargs, GatewayRunner  # type: ignore
    from hermes_cli.tools_config import _get_platform_tools  # type: ignore
    from hermes_constants import get_hermes_home  # type: ignore
    from hermes_state import SessionDB  # type: ignore

    session_id = str(request.get("session_id", "")).strip()
    user_message = str(request.get("user_message", ""))
    conversation_history = request.get("conversation_history") or []
    ephemeral_system_prompt = request.get("ephemeral_system_prompt")
    if not session_id or not user_message:
        raise RuntimeError("worker request is missing session_id or user_message")

    user_config = _load_gateway_config()
    runtime_kwargs = _resolve_runtime_agent_kwargs()
    enabled_toolsets = sorted(_get_platform_tools(user_config, "api_server"))
    model = _resolve_gateway_model(user_config)
    fallback_model = GatewayRunner._load_fallback_model()
    session_db = SessionDB(db_path=get_hermes_home() / "state.db")

    def on_delta(delta: Any) -> None:
        if delta is None:
            return
        _emit({"event": "delta", "delta": str(delta)})

    def on_tool_progress(
        event_type: str,
        tool_name: str = None,
        preview: str = None,
        args: Any = None,
        **kwargs: Any,
    ) -> None:
        _emit(
            {
                "event": "tool",
                "type": event_type,
                "tool_name": tool_name,
                "preview": preview,
                "args": args,
                "meta": kwargs,
            }
        )

    agent = AIAgent(
        model=model,
        **runtime_kwargs,
        max_iterations=int(user_config.get("agent", {}).get("max_iterations", 90)),
        quiet_mode=True,
        verbose_logging=False,
        ephemeral_system_prompt=ephemeral_system_prompt or None,
        enabled_toolsets=enabled_toolsets,
        session_id=session_id,
        platform="api_server",
        stream_delta_callback=on_delta,
        tool_progress_callback=on_tool_progress,
        session_db=session_db,
        fallback_model=fallback_model,
    )
    try:
        result = agent.run_conversation(
            user_message=user_message,
            conversation_history=conversation_history,
            task_id="default",
        )
    except BaseException as exc:
        _emit({"event": "failed", "message": str(exc)})
        return 1

    usage = {
        "input_tokens": getattr(agent, "session_prompt_tokens", 0) or 0,
        "output_tokens": getattr(agent, "session_completion_tokens", 0) or 0,
        "total_tokens": getattr(agent, "session_total_tokens", 0) or 0,
    }
    final_text = ""
    if isinstance(result, dict):
        final_text = str(result.get("final_response") or result.get("error") or "")
    _emit({"event": "completed", "content": final_text, "usage": usage})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
