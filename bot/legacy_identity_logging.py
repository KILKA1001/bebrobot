import logging
from typing import Any


def _stringify(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _emit(logger: logging.Logger, level: str, event: str, **fields: Any) -> None:
    parts = [event]
    for key, value in fields.items():
        if value is None:
            continue
        parts.append(f"{key}={_stringify(value)}")
    getattr(logger, level)(" ".join(parts))


def log_transport_identity_error(
    logger: logging.Logger,
    *,
    module: str,
    handler: str,
    field: str,
    action: str,
    continue_execution: bool = False,
    **context: Any,
) -> None:
    _emit(
        logger,
        "error",
        "transport identity error",
        module=module,
        handler=handler,
        field=field,
        action=action,
        continue_execution=continue_execution,
        **context,
    )


def log_identity_resolve_error(
    logger: logging.Logger,
    *,
    module: str,
    handler: str,
    field: str,
    action: str,
    continue_execution: bool = False,
    **context: Any,
) -> None:
    _emit(
        logger,
        "error",
        "identity resolve error",
        module=module,
        handler=handler,
        field=field,
        action=action,
        continue_execution=continue_execution,
        **context,
    )


def log_legacy_identity_path_detected(
    logger: logging.Logger,
    *,
    module: str,
    handler: str,
    field: str,
    action: str,
    continue_execution: bool,
    **context: Any,
) -> None:
    _emit(
        logger,
        "warning",
        "legacy identity path detected",
        module=module,
        handler=handler,
        field=field,
        action=action,
        continue_execution=continue_execution,
        **context,
    )


def log_legacy_identity_fallback_used(
    logger: logging.Logger,
    *,
    module: str,
    handler: str,
    field: str,
    action: str,
    continue_execution: bool,
    **context: Any,
) -> None:
    _emit(
        logger,
        "warning",
        "legacy identity fallback used",
        module=module,
        handler=handler,
        field=field,
        action=action,
        continue_execution=continue_execution,
        **context,
    )


def log_legacy_schema_fallback(
    logger: logging.Logger,
    *,
    module: str,
    table: str,
    field: str,
    action: str,
    continue_execution: bool,
    **context: Any,
) -> None:
    _emit(
        logger,
        "warning",
        "legacy schema fallback",
        module=module,
        table=table,
        field=field,
        action=action,
        continue_execution=continue_execution,
        **context,
    )


def log_runtime_dependency_missing(
    logger: logging.Logger,
    *,
    module: str,
    handler: str,
    field: str,
    action: str,
    continue_execution: bool,
    **context: Any,
) -> None:
    _emit(
        logger,
        "error",
        "runtime dependency missing",
        module=module,
        handler=handler,
        field=field,
        action=action,
        continue_execution=continue_execution,
        **context,
    )
