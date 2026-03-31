"""
Назначение: модуль "auth" реализует продуктовый контур в зоне общая логика.
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: общая логика.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

AssignmentSource = Literal["custom", "discord", "telegram", "system"]
PermissionEffect = Literal["allow", "deny"]


@dataclass(slots=True)
class Permission:
    name: str
    effect: PermissionEffect = "allow"


@dataclass(slots=True)
class Role:
    name: str
    permissions: list[Permission] = field(default_factory=list)


@dataclass(slots=True)
class UserRoleAssignment:
    role_name: str
    source: AssignmentSource
    external_id: str | None = None
    expires_at: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    origin_label: str | None = None
    synced_at: datetime | None = None
