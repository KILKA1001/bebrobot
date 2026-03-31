"""
Назначение: модуль "test discord roles admin" реализует продуктовый контур в зоне Discord/Telegram/общая логика (тесты).
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: Discord/Telegram/общая логика (тесты).
"""

import unittest
import importlib.util
import sys
import types
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from bot.services.role_management_service import DELETE_ROLE_REASON_DISCORD_MANAGED


class _FakeGroup:
    def __init__(self, func):
        self.func = func

    def command(self, *args, **kwargs):
        def decorator(func):
            return func

        return decorator


class _FakeBot:
    def hybrid_group(self, *args, **kwargs):
        def decorator(func):
            return _FakeGroup(func)

        return decorator


_base_module = types.ModuleType("bot.commands.base")
_base_module.bot = _FakeBot()
sys.modules.setdefault("bot.commands.base", _base_module)

_SPEC = importlib.util.spec_from_file_location(
    "test_bot_commands_roles_admin_module",
    Path(__file__).resolve().parents[1] / "bot" / "commands" / "roles_admin.py",
)
roles_admin = importlib.util.module_from_spec(_SPEC)
assert _SPEC and _SPEC.loader
_SPEC.loader.exec_module(roles_admin)


class DiscordRolesAdminTests(unittest.IsolatedAsyncioTestCase):
    def _build_ctx(self):
        guild = SimpleNamespace(id=222)
        author = SimpleNamespace(
            id=111,
            name="admin_user",
            display_name="Admin User",
            global_name="AdminGlobal",
            guild_permissions=SimpleNamespace(administrator=True),
        )
        return SimpleNamespace(guild=guild, author=author)

    def test_rolesadmin_help_embed_hides_categories_when_not_allowed(self):
        visibility = roles_admin.RolesAdminVisibilityContext(
            actor_level=80,
            actor_titles=("Вице города",),
            can_manage_categories=False,
            can_manage_roles=True,
            hidden_sections=("categories",),
        )

        embed = roles_admin._rolesadmin_help_embed(visibility=visibility)

        field_names = [field.name for field in embed.fields]
        self.assertIn("С чего начать", field_names)
        self.assertNotIn("Категории", field_names)
        self.assertIn("Роли", field_names)
        self.assertIn("Пользователи", field_names)
        self.assertIn("Некоторые кнопки скрыты", embed.description)
        self.assertIn("сначала настрой каталог", embed.description)

    def test_rolesadmin_help_embed_mentions_alias_and_platform_limits(self):
        visibility = roles_admin.RolesAdminVisibilityContext(
            actor_level=100,
            actor_titles=("Глава клуба",),
            can_manage_categories=True,
            can_manage_roles=True,
            hidden_sections=(),
        )

        embed = roles_admin._rolesadmin_help_embed(visibility=visibility)

        self.assertIn("Discord команда называется", embed.description)
        self.assertIn("/roles_admin", embed.description)
        self.assertIn("sync_discord_roles", embed.description)
        users_field = next(field for field in embed.fields if field.name == "Пользователи")
        self.assertIn("Пакетный режим доступен на обеих платформах", users_field.value)

    def test_rolesadmin_help_embed_describes_start_order(self):
        visibility = roles_admin.RolesAdminVisibilityContext(
            actor_level=100,
            actor_titles=("Глава клуба",),
            can_manage_categories=True,
            can_manage_roles=True,
            hidden_sections=(),
        )

        embed = roles_admin._rolesadmin_help_embed(visibility=visibility)

        self.assertEqual(embed.fields[0].name, "С чего начать")
        self.assertIn("Подход 1", embed.fields[0].value)
        self.assertIn("Подход 2", embed.fields[0].value)
        self.assertIn("сначала создай категорию", embed.fields[1].value)
        self.assertIn("создай роль", embed.fields[2].value)
        self.assertIn("выдаче или снятию роли", embed.fields[3].value)

    def test_rolesadmin_help_view_hides_category_button_without_permission(self):
        visibility = roles_admin.RolesAdminVisibilityContext(
            actor_level=80,
            actor_titles=("Вице города",),
            can_manage_categories=False,
            can_manage_roles=True,
            hidden_sections=("categories",),
        )

        view = roles_admin.RolesAdminHelpView(actor_id=111, visibility=visibility, guild_id=222)
        labels = [child.label for child in view.children]

        self.assertNotIn("Категории", labels)
        self.assertNotIn("⚙️ Настройка магазина", labels)
        self.assertIn("Роли", labels)
        self.assertIn("Пользователи", labels)

    def test_rolesadmin_help_view_hides_shop_settings_for_superadmins(self):
        visibility = roles_admin.RolesAdminVisibilityContext(
            actor_level=100,
            actor_titles=("Глава клуба",),
            can_manage_categories=True,
            can_manage_roles=True,
            hidden_sections=(),
            can_manage_shop_settings=True,
        )

        view = roles_admin.RolesAdminHelpView(actor_id=111, visibility=visibility, guild_id=222)
        labels = [child.label for child in view.children]

        self.assertNotIn("⚙️ Настройка магазина", labels)

    async def test_rolesadmin_list_runs_implicit_sync_before_listing(self):
        ctx = self._build_ctx()
        grouped = [{"category": "General", "roles": [{"name": "Role A", "discord_role_id": "1", "description": "Описание", "acquire_hint": "Через турнир"}]}]

        with (
            patch.object(roles_admin, "_ensure_roles_admin", AsyncMock(return_value=True)),
            patch.object(roles_admin, "_sync_ctx_discord_roles_catalog", AsyncMock(return_value=True)) as sync_mock,
            patch.object(roles_admin.RoleManagementService, "list_roles_grouped", return_value=grouped),
            patch.object(roles_admin, "send_temp", AsyncMock()) as send_mock,
        ):
            await roles_admin.rolesadmin_list(ctx)

        sync_mock.assert_awaited_once_with(ctx, operation="list")
        embed = send_mock.await_args.kwargs["embed"]
        self.assertEqual(embed.title, "🧩 Роли по категориям")
        self.assertIsNone(embed.description)
        self.assertIn("Описание", embed.fields[0].value)
        self.assertIn("Через турнир", embed.fields[0].value)

    async def test_rolesadmin_list_warns_and_logs_when_implicit_sync_fails(self):
        ctx = self._build_ctx()
        grouped = [{"category": "General", "roles": [{"name": "Role A", "discord_role_id": "1"}]}]

        with (
            patch.object(roles_admin, "_ensure_roles_admin", AsyncMock(return_value=True)),
            patch.object(roles_admin, "_sync_ctx_discord_roles_catalog", AsyncMock(return_value=False)),
            patch.object(roles_admin.RoleManagementService, "list_roles_grouped", return_value=grouped),
            patch.object(roles_admin, "send_temp", AsyncMock()) as send_mock,
        ):
            await roles_admin.rolesadmin_list(ctx)

        embed = send_mock.await_args.kwargs["embed"]
        self.assertIn("Автосинхронизация Discord-каталога", embed.description)
        self.assertIn("может быть неактуален", embed.description)

    async def test_resolve_discord_target_supports_mention_lookup(self):
        member = SimpleNamespace(
            id=555,
            mention="<@555>",
            name="target_user",
            display_name="Target User",
            global_name="TargetGlobal",
        )
        guild = SimpleNamespace(id=222, get_member=lambda user_id: member if user_id == 555 else None, members=[member])
        author = SimpleNamespace(
            id=111,
            name="admin_user",
            display_name="Admin User",
            global_name="AdminGlobal",
            guild_permissions=SimpleNamespace(administrator=True),
        )
        ctx = SimpleNamespace(guild=guild, author=author)

        with (
            patch.object(roles_admin.AccountsService, "persist_identity_lookup_fields"),
            patch.object(roles_admin.AccountsService, "resolve_account_id", return_value="acc-555"),
        ):
            result = await roles_admin._resolve_discord_target(ctx, "<@555>", operation="user_roles")

        self.assertEqual(result["provider"], "discord")
        self.assertEqual(result["provider_user_id"], "555")
        self.assertEqual(result["matched_by"], "guild_member")

    async def test_resolve_discord_target_returns_candidates_for_multiple_matches(self):
        first = SimpleNamespace(id=1, mention="<@1>", name="dup", display_name="Dup One", global_name=None)
        second = SimpleNamespace(id=2, mention="<@2>", name="dup", display_name="Dup Two", global_name="DupGlobal")
        guild = SimpleNamespace(id=222, get_member=lambda _user_id: None, members=[first, second])
        author = SimpleNamespace(
            id=111,
            name="admin_user",
            display_name="Admin User",
            global_name="AdminGlobal",
            guild_permissions=SimpleNamespace(administrator=True),
        )
        ctx = SimpleNamespace(guild=guild, author=author)

        with (
            patch.object(roles_admin.AccountsService, "persist_identity_lookup_fields"),
            patch.object(roles_admin, "send_temp", AsyncMock()) as send_mock,
        ):
            result = await roles_admin._resolve_discord_target(ctx, "dup", operation="user_grant")

        self.assertIsNone(result)
        message = send_mock.await_args.args[1]
        self.assertIn("Найдено несколько пользователей", message)
        self.assertIn("discord | username=@dup | display=Dup One | via=guild_member", message)

    async def test_rolesadmin_role_delete_denies_external_role(self):
        ctx = self._build_ctx()

        with (
            patch.object(roles_admin, "_ensure_roles_admin", AsyncMock(return_value=True)),
            patch.object(
                roles_admin.RoleManagementService,
                "delete_role",
                return_value={"ok": False, "reason": DELETE_ROLE_REASON_DISCORD_MANAGED},
            ),
            patch.object(roles_admin, "send_temp", AsyncMock()) as send_mock,
        ):
            await roles_admin.rolesadmin_role_delete(ctx, "External")

        self.assertIn("нельзя удалить", send_mock.await_args.args[1])

    async def test_rolesadmin_move_and_order_allow_external_catalog_role(self):
        ctx = self._build_ctx()

        with (
            patch.object(roles_admin, "_ensure_roles_admin", AsyncMock(return_value=True)),
            patch.object(roles_admin, "_sync_ctx_discord_roles_catalog", AsyncMock(return_value=True)),
            patch.object(roles_admin, "_catalog_role_exists", return_value=True),
            patch.object(
                roles_admin.RoleManagementService,
                "get_category_role_positioning",
                return_value={
                    "category": "Cat",
                    "current_roles": [{"name": "Existing"}],
                    "computed_last_position": 1,
                    "position_description": "будет добавлено в конец (#2)",
                },
            ),
            patch.object(roles_admin.RoleManagementService, "move_role", return_value=True) as move_mock,
            patch.object(roles_admin, "send_temp", AsyncMock()) as send_mock,
        ):
            await roles_admin.rolesadmin_role_move(ctx, "External", "Cat", 2)
            await roles_admin.rolesadmin_role_order(ctx, "External", "Cat", 3)

        self.assertEqual(move_mock.call_count, 2)
        embeds = [call.kwargs["embed"] for call in send_mock.await_args_list if "embed" in call.kwargs]
        self.assertEqual(embeds[0].title, "🧭 Предпросмотр перемещения роли")
        self.assertIn("будет добавлено в конец (#2)", embeds[0].description)
        self.assertEqual(embeds[1].title, "🧭 Предпросмотр порядка роли")
        messages = [call.args[1] for call in send_mock.await_args_list if len(call.args) > 1]
        self.assertTrue(any("перемещена" in message for message in messages))
        self.assertTrue(any("обновлён" in message for message in messages))

    async def test_rolesadmin_user_grant_shows_sync_only_discord_role_message(self):
        ctx = self._build_ctx()
        resolved = {"account_id": "acc-2", "label": "@target", "member": None, "provider_user_id": "222"}

        with (
            patch.object(roles_admin, "_ensure_roles_admin", AsyncMock(return_value=True)),
            patch.object(roles_admin, "_resolve_discord_target", AsyncMock(return_value=resolved)),
            patch.object(
                roles_admin.RoleManagementService,
                "apply_user_role_changes_by_account",
                return_value={
                    "grant_success": [],
                    "grant_denied": [
                        {
                            "reason": "sync_only_discord_role",
                            "message": "Эта скрытая Discord-роль управляется только через сам Discord и не меняется командами бота.",
                        }
                    ],
                },
            ),
            patch.object(roles_admin, "send_temp", AsyncMock()) as send_mock,
        ):
            await roles_admin.rolesadmin_user_grant(ctx, "@target", "Bot Hidden")

        self.assertIn("только через сам Discord", send_mock.await_args.args[1])

    async def test_rolesadmin_user_grant_shows_privileged_discord_role_message_for_vice(self):
        ctx = self._build_ctx()
        resolved = {"account_id": "acc-2", "label": "@target", "member": None, "provider_user_id": "222"}

        with (
            patch.object(roles_admin, "_ensure_roles_admin", AsyncMock(return_value=True)),
            patch.object(roles_admin, "_resolve_discord_target", AsyncMock(return_value=resolved)),
            patch.object(
                roles_admin.RoleManagementService,
                "apply_user_role_changes_by_account",
                return_value={
                    "grant_success": [],
                    "grant_denied": [
                        {
                            "reason": "privileged_discord_role",
                            "message": "Эту Discord-роль может выдавать только глава/главный вице.",
                        }
                    ],
                },
            ),
            patch.object(roles_admin, "send_temp", AsyncMock()) as send_mock,
        ):
            await roles_admin.rolesadmin_user_grant(ctx, "@target", "Discord Admin")

        self.assertIn("только глава/главный вице", send_mock.await_args.args[1])

    async def test_rolesadmin_access_denied_records_audit(self):
        ctx = self._build_ctx()
        ctx.author.guild_permissions = SimpleNamespace(administrator=False)

        with (
            patch.object(roles_admin.AuthorityService, "has_command_permission", return_value=False),
            patch.object(roles_admin.RoleManagementService, "record_role_change_audit") as audit_mock,
            patch.object(roles_admin, "send_temp", AsyncMock()) as send_mock,
        ):
            allowed = await roles_admin._ensure_roles_admin(ctx)

        self.assertFalse(allowed)
        audit_mock.assert_called_once()
        self.assertIn("Недостаточно полномочий", send_mock.await_args.args[1])

    async def test_rolesadmin_user_grant_audits_discord_sync_conflict(self):
        member = SimpleNamespace(id=222, add_roles=AsyncMock(side_effect=RuntimeError("boom")))
        guild_role = SimpleNamespace(id=999)
        ctx = self._build_ctx()
        ctx.guild = SimpleNamespace(id=222, get_role=lambda role_id: guild_role if role_id == 999 else None)
        resolved = {"account_id": "acc-2", "label": "@target", "member": member, "provider": "discord", "provider_user_id": "222"}

        with (
            patch.object(roles_admin, "_ensure_roles_admin", AsyncMock(return_value=True)),
            patch.object(roles_admin, "_resolve_discord_target", AsyncMock(return_value=resolved)),
            patch.object(roles_admin.RoleManagementService, "get_role", return_value={"discord_role_id": "999"}),
            patch.object(
                roles_admin.RoleManagementService,
                "apply_user_role_changes_by_account",
                return_value={"grant_success": ["Moderator"], "grant_denied": [], "grant_failed": []},
            ),
            patch.object(roles_admin.RoleManagementService, "record_role_change_audit") as audit_mock,
            patch.object(roles_admin, "send_temp", AsyncMock()) as send_mock,
        ):
            await roles_admin.rolesadmin_user_grant(ctx, "@target", "Moderator")

        self.assertTrue(audit_mock.called)
        self.assertIn("не удалось", send_mock.await_args.args[1])

    async def test_rolesadmin_role_create_sends_preview_embed_before_confirmation(self):
        ctx = self._build_ctx()

        with (
            patch.object(roles_admin, "_ensure_roles_admin", AsyncMock(return_value=True)),
            patch.object(
                roles_admin.RoleManagementService,
                "get_category_role_positioning",
                return_value={
                    "category": "General",
                    "current_roles": [{"name": "Alpha"}],
                    "computed_last_position": 1,
                    "position_description": "будет добавлено в конец (#2)",
                },
            ),
            patch.object(roles_admin.RoleManagementService, "create_role_result", return_value={"ok": True}),
            patch.object(roles_admin, "send_temp", AsyncMock()) as send_mock,
        ):
            await roles_admin.rolesadmin_role_create(ctx, "General", "New", "Описание", "Через турнир", None, None)

        first_embed = send_mock.await_args_list[0].kwargs["embed"]
        self.assertEqual(first_embed.title, "🧭 Предпросмотр создания роли")
        self.assertIn("Если позицию не указывать", first_embed.description)
        self.assertIn("Описание", first_embed.description)
        self.assertIn("Как получить", first_embed.description)

    async def test_role_category_autocomplete_returns_matching_categories(self):
        interaction = SimpleNamespace()

        with patch.object(
            roles_admin.RoleManagementService,
            "list_roles_grouped",
            return_value=[
                {"category": "General", "roles": []},
                {"category": "Events", "roles": []},
                {"category": "Gaming", "roles": []},
            ],
        ):
            result = await roles_admin._role_category_autocomplete(interaction, "ge")

        self.assertEqual([item.value for item in result], ["General"])

    async def test_rolesadmin_role_edit_description_calls_service(self):
        ctx = self._build_ctx()

        with (
            patch.object(roles_admin, "_ensure_roles_admin", AsyncMock(return_value=True)),
            patch.object(roles_admin.RoleManagementService, "update_role_description", return_value=True) as update_mock,
            patch.object(roles_admin, "send_temp", AsyncMock()) as send_mock,
        ):
            await roles_admin.rolesadmin_role_edit_description(ctx, "New", "Новое описание")

        update_mock.assert_called_once_with(
            "New",
            "Новое описание",
            actor_id=str(ctx.author.id),
            actor_provider="discord",
            actor_user_id=str(ctx.author.id),
            operation="role_edit_description",
            source="discord_command",
        )
        self.assertIn("Описание роли", send_mock.await_args.args[1])

    async def test_rolesadmin_role_edit_acquire_hint_calls_service(self):
        ctx = self._build_ctx()

        with (
            patch.object(roles_admin, "_ensure_roles_admin", AsyncMock(return_value=True)),
            patch.object(roles_admin.RoleManagementService, "update_role_acquire_hint", return_value=True) as update_mock,
            patch.object(roles_admin, "send_temp", AsyncMock()) as send_mock,
        ):
            await roles_admin.rolesadmin_role_edit_acquire_hint(ctx, "New", "Через турнир")

        update_mock.assert_called_once_with(
            "New",
            "Через турнир",
            actor_id=str(ctx.author.id),
            actor_provider="discord",
            actor_user_id=str(ctx.author.id),
            operation="role_edit_acquire_hint",
            source="discord_command",
        )
        self.assertIn("Способ получения роли", send_mock.await_args.args[1])

    def test_discord_user_role_flow_state_supports_multi_select_and_category_reentry(self):
        grouped = [
            {"category": "General", "roles": [{"name": "Alpha"}, {"name": "Beta"}]},
            {"category": "Events", "roles": [{"name": "Gamma"}]},
        ]
        state = roles_admin.DiscordUserRoleFlowState(
            actor_id=111,
            action="grant",
            target={"label": "@target", "account_id": "acc-7"},
            grouped=grouped,
            current_category="General",
        )

        state = state.with_category_selection("General", ["Alpha", "Beta"])
        state = state.with_category("Events")
        state = state.with_category_selection("Events", ["Gamma"])
        embed = roles_admin._build_user_role_flow_embed(state)

        self.assertEqual(list(state.selected_roles), ["Alpha", "Beta", "Gamma"])
        self.assertIn("Alpha", embed.fields[0].value)
        self.assertIn("Gamma", embed.fields[0].value)
        self.assertIn("можно продолжать по другим категориям", embed.description)

    async def test_rolesadmin_user_grant_without_role_name_opens_batch_flow(self):
        ctx = self._build_ctx()
        grouped = [{"category": "General", "roles": [{"name": "Alpha"}]}]
        resolved = {"label": "@target", "account_id": "acc-7", "provider": "discord", "provider_user_id": "555", "member": None}

        with (
            patch.object(roles_admin, "_ensure_roles_admin", AsyncMock(return_value=True)),
            patch.object(roles_admin, "_resolve_discord_target", AsyncMock(return_value=resolved)),
            patch.object(roles_admin.RoleManagementService, "list_roles_grouped", return_value=grouped),
            patch.object(roles_admin, "send_temp", AsyncMock()) as send_mock,
        ):
            await roles_admin.rolesadmin_user_grant(ctx, "target", None)

        self.assertIsInstance(send_mock.await_args.kwargs["view"], roles_admin.DiscordUserRoleFlowView)
        embed = send_mock.await_args.kwargs["embed"]
        self.assertIn("Уже выбрано ролей: **0**", embed.description)
        self.assertIn("Выбор можно продолжать по другим категориям", embed.description)
    async def test_rolesadmin_role_create_shows_profile_title_conflict_message(self):
        ctx = self._build_ctx()

        with (
            patch.object(roles_admin, "_ensure_roles_admin", AsyncMock(return_value=True)),
            patch.object(
                roles_admin.RoleManagementService,
                "get_category_role_positioning",
                return_value={
                    "category": "General",
                    "current_roles": [{"name": "Alpha"}],
                    "computed_last_position": 1,
                    "position_description": "будет добавлено в конец (#2)",
                },
            ),
            patch.object(
                roles_admin.RoleManagementService,
                "create_role_result",
                return_value={
                    "ok": False,
                    "reason": "profile_title_conflict",
                    "message": "Название совпадает с активным званием. Это уже звание, а не каталожная роль.",
                },
            ),
            patch.object(roles_admin, "send_temp", AsyncMock()) as send_mock,
        ):
            await roles_admin.rolesadmin_role_create(ctx, "General", "New", "Описание", "Через турнир", None, None)

        self.assertIn("Это уже звание, а не каталожная роль", send_mock.await_args_list[-1].args[1])
