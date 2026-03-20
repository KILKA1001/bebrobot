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
            patch.object(roles_admin.RoleManagementService, "create_role", return_value=True),
            patch.object(roles_admin, "send_temp", AsyncMock()) as send_mock,
        ):
            await roles_admin.rolesadmin_role_create(ctx, "New", "General", "Описание", "Через турнир", None, None)

        first_embed = send_mock.await_args_list[0].kwargs["embed"]
        self.assertEqual(first_embed.title, "🧭 Предпросмотр создания роли")
        self.assertIn("Если позицию не указывать", first_embed.description)
        self.assertIn("Описание", first_embed.description)
        self.assertIn("Как получить", first_embed.description)

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
            operation="role_edit_description",
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
            operation="role_edit_acquire_hint",
        )
        self.assertIn("Способ получения роли", send_mock.await_args.args[1])
