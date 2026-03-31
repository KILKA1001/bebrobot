"""
Назначение: модуль "app" реализует продуктовый контур в зоне общая логика (Admin API).
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: общая логика (Admin API).
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

from flask import Blueprint, Flask, jsonify, render_template_string, request

from bot.data import db
from bot.services.accounts_service import AccountsService
from bot.services.auth.role_resolver import RoleResolver
from bot.services.authority_service import AuthorityService
from bot.services.role_management_service import RoleManagementService

logger = logging.getLogger(__name__)

admin_api_bp = Blueprint("admin_api", __name__)


def _resolve_account_id(provider: str, provider_user_id: str) -> str | None:
    try:
        return AccountsService.resolve_account_id(provider, str(provider_user_id))
    except Exception:
        logger.exception(
            "admin api resolve account failed provider=%s provider_user_id=%s",
            provider,
            provider_user_id,
        )
        return None


def _split_roles(
    roles: list[dict[str, str | None]],
) -> tuple[list[dict[str, str | None]], list[dict[str, str | None]]]:
    custom_roles = [role for role in roles if role.get("source") in {"custom", "system"}]
    external_roles = [role for role in roles if role.get("source") in {"discord", "telegram"}]
    return custom_roles, external_roles


def _build_user_payload(account_id: str) -> dict[str, Any]:
    access = RoleResolver.resolve_for_account(account_id)
    custom_roles, external_roles = _split_roles(access.roles)
    return {
        "account_id": account_id,
        "custom_roles": custom_roles,
        "external_roles": external_roles,
        "permissions": access.permissions,
    }


def _write_role_audit(
    actor_user_id: str,
    target_user_id: str,
    action: str,
    role_id: str,
    source: str,
    reason: str | None,
) -> None:
    RoleManagementService.record_role_change_audit(
        action=action,
        role_name=role_id,
        source=source,
        actor_user_id=str(actor_user_id),
        target_user_id=str(target_user_id),
        after={"reason": reason},
        error_message=reason,
    )


@admin_api_bp.get("/admin/api/users/<provider>/<provider_user_id>")
def admin_user_view(provider: str, provider_user_id: str):
    account_id = _resolve_account_id(provider, provider_user_id)
    if not account_id:
        logger.error(
            "admin api user view failed: user not found provider=%s provider_user_id=%s",
            provider,
            provider_user_id,
        )
        return jsonify({"ok": False, "error": "user_not_found"}), 404

    payload = _build_user_payload(account_id)
    payload.update({"ok": True, "provider": provider, "provider_user_id": str(provider_user_id)})
    return jsonify(payload)


@admin_api_bp.get("/admin/api/users/<provider>/<provider_user_id>/roles/external")
def admin_user_external_roles(provider: str, provider_user_id: str):
    account_id = _resolve_account_id(provider, provider_user_id)
    if not account_id:
        logger.error(
            "admin api external roles view failed: user not found provider=%s provider_user_id=%s",
            provider,
            provider_user_id,
        )
        return jsonify({"ok": False, "error": "user_not_found"}), 404

    payload = _build_user_payload(account_id)
    return jsonify({"ok": True, "account_id": account_id, "external_roles": payload["external_roles"]})


@admin_api_bp.post("/admin/api/users/<provider>/<provider_user_id>/roles/custom")
def admin_user_custom_roles(provider: str, provider_user_id: str):
    body = request.get_json(silent=True) or {}
    action = str(body.get("action") or "").strip().lower()
    role_name = str(body.get("role_id") or body.get("role_name") or "").strip().lower()
    actor_provider = str(body.get("actor_provider") or "").strip().lower()
    actor_user_id = str(body.get("actor_user_id") or "").strip()
    source = str(body.get("source") or "custom").strip().lower() or "custom"
    reason = str(body.get("reason") or "").strip() or None

    if action not in {"assign", "remove"} or not role_name or not actor_provider or not actor_user_id:
        logger.error(
            "admin api role change rejected: bad request provider=%s provider_user_id=%s action=%s role=%s actor=%s:%s",
            provider,
            provider_user_id,
            action,
            role_name,
            actor_provider,
            actor_user_id,
        )
        return jsonify({"ok": False, "error": "bad_request"}), 400

    if source not in {"custom", "system"}:
        logger.error(
            "admin api role change rejected: source not allowed provider=%s provider_user_id=%s source=%s",
            provider,
            provider_user_id,
            source,
        )
        return jsonify({"ok": False, "error": "source_not_allowed"}), 400

    if not AuthorityService.can_manage_role(actor_provider, actor_user_id, role_name):
        logger.error(
            "admin api role change forbidden actor=%s:%s target=%s:%s role=%s",
            actor_provider,
            actor_user_id,
            provider,
            provider_user_id,
            role_name,
        )
        return jsonify({"ok": False, "error": "forbidden_role_manage"}), 403

    account_id = _resolve_account_id(provider, provider_user_id)
    if not account_id:
        logger.error(
            "admin api role change failed: user not found provider=%s provider_user_id=%s",
            provider,
            provider_user_id,
        )
        return jsonify({"ok": False, "error": "user_not_found"}), 404

    if not db.supabase:
        logger.error("admin api roles change failed: supabase is not configured")
        return jsonify({"ok": False, "error": "db_not_configured"}), 500

    try:
        table = db.supabase.table("account_role_assignments")
        if action == "assign":
            table.upsert(
                {
                    "account_id": account_id,
                    "role_name": role_name,
                    "source": source,
                    "metadata": {"updated_by": actor_user_id},
                    "origin_label": "Назначено через admin API",
                    "synced_at": datetime.now(timezone.utc).isoformat(),
                },
                on_conflict="account_id,role_name,source",
            ).execute()
        else:
            table.delete().eq("account_id", account_id).eq("role_name", role_name).in_("source", ["custom", "system"]).execute()
    except Exception:
        logger.exception(
            "admin api role change failed action=%s provider=%s provider_user_id=%s role_name=%s actor=%s:%s",
            action,
            provider,
            provider_user_id,
            role_name,
            actor_provider,
            actor_user_id,
        )
        return jsonify({"ok": False, "error": "db_write_failed"}), 500

    _write_role_audit(
        actor_user_id=actor_user_id,
        target_user_id=str(provider_user_id),
        action=action,
        role_id=role_name,
        source=source,
        reason=reason,
    )

    payload = _build_user_payload(account_id)
    return jsonify({"ok": True, "account_id": account_id, "action": action, "custom_roles": payload["custom_roles"]})


@admin_api_bp.get("/admin/users/<provider>/<provider_user_id>/roles")
def admin_roles_view(provider: str, provider_user_id: str):
    account_id = _resolve_account_id(provider, provider_user_id)
    if not account_id:
        logger.error(
            "admin roles ui failed: user not found provider=%s provider_user_id=%s",
            provider,
            provider_user_id,
        )
        return "User not found", 404

    payload = _build_user_payload(account_id)
    return render_template_string(
        """
        <h1>Управление ролями пользователя {{ account_id }}</h1>
        <section>
          <h2>Кастомные роли (редактируемые)</h2>
          <ul>
          {% for role in custom_roles %}
            <li>{{ role['name'] }} <small>[{{ role['source'] }}]</small></li>
          {% else %}
            <li>Нет кастомных ролей</li>
          {% endfor %}
          </ul>
        </section>
        <section>
          <h2>Discord/Telegram роли (только просмотр, синк)</h2>
          <ul>
          {% for role in external_roles %}
            <li>{{ role['name'] }} <small>[{{ role['source'] }}]</small></li>
          {% else %}
            <li>Нет внешних ролей</li>
          {% endfor %}
          </ul>
        </section>
        """,
        account_id=account_id,
        custom_roles=payload["custom_roles"],
        external_roles=payload["external_roles"],
    )


def register_admin_routes(app: Flask) -> Flask:
    if "admin_api" not in app.blueprints:
        app.register_blueprint(admin_api_bp)
    return app


def create_admin_app() -> Flask:
    app = Flask(__name__)
    register_admin_routes(app)
    return app


def run_admin_api() -> None:
    host = os.getenv("ADMIN_API_HOST", "0.0.0.0")
    port = int(os.getenv("ADMIN_API_PORT", os.getenv("PORT", "8080")))
    debug = os.getenv("ADMIN_API_DEBUG", "").strip().lower() in {"1", "true", "yes", "on"}

    logging.basicConfig(level=logging.INFO)
    logger.info("starting admin api host=%s port=%s debug=%s", host, port, debug)

    try:
        create_admin_app().run(host=host, port=port, debug=debug)
    except Exception:
        logger.exception("admin api server crashed host=%s port=%s", host, port)
        raise


def main() -> None:
    run_admin_api()


admin_app = create_admin_app()


if __name__ == "__main__":
    main()
