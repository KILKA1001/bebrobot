import importlib.util
from pathlib import Path
from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch


_BASE_PATH = Path(__file__).resolve().parents[1] / "bot" / "commands" / "base.py"
_SPEC = importlib.util.spec_from_file_location("test_discord_base_module", _BASE_PATH)
assert _SPEC and _SPEC.loader
base = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(base)


class DiscordRolesCatalogRenderTests(IsolatedAsyncioTestCase):
    async def test_roles_command_renders_grouped_catalog_with_acquire_method(self):
        ctx = SimpleNamespace(guild=SimpleNamespace(id=321), author=SimpleNamespace(id=111))
        send_temp = AsyncMock()

        with patch.object(
            base.RoleManagementService,
            "list_public_roles_catalog",
            return_value=[
                {
                    "category": "Турниры",
                    "roles": [
                        {
                            "name": "Чемпион",
                            "description": "Победитель сезона",
                            "acquire_method_label": "выдаёт администратор",
                            "acquire_hint": "Выиграть финал сезона",
                        }
                    ],
                }
            ],
        ), patch.object(base, "send_temp", send_temp):
            await base.roles_list.callback(ctx)

        embed = send_temp.await_args.kwargs["embed"]
        assert embed.title == "🏅 Каталог ролей"
        assert "Что это" in embed.description
        assert "Где смотреть способ получения" in embed.description
        assert "выдаются вручную" in embed.description
        assert embed.fields[0].name == "Турниры"
        assert "Победитель сезона" in embed.fields[0].value
        assert "выдаёт администратор" in embed.fields[0].value
        assert "Выиграть финал сезона" in embed.fields[0].value

    async def test_roles_command_renders_placeholders_for_empty_description_and_hint(self):
        ctx = SimpleNamespace(guild=SimpleNamespace(id=321), author=SimpleNamespace(id=111))
        send_temp = AsyncMock()

        with patch.object(
            base.RoleManagementService,
            "list_public_roles_catalog",
            return_value=[
                {
                    "category": "Роли за баллы",
                    "roles": [
                        {
                            "name": "Новичок",
                            "description": "",
                            "acquire_method_label": "за баллы",
                            "acquire_hint": "",
                        }
                    ],
                }
            ],
        ), patch.object(base, "send_temp", send_temp):
            await base.roles_list.callback(ctx)

        embed = send_temp.await_args.kwargs["embed"]
        assert "Описание пока не указано администратором" in embed.fields[0].value
        assert "за баллы" in embed.fields[0].value
        assert "Способ получения пока не указан администратором" in embed.fields[0].value


    async def test_build_embed_uses_shared_page_structure(self):
        page_data = {
            "page_index": 1,
            "total_pages": 3,
            "section_count": 1,
            "role_count": 1,
            "sections": [
                {
                    "category": "Турниры",
                    "section_index": 1,
                    "section_count": 2,
                    "is_category_continuation": True,
                    "continues_on_next_page": False,
                    "items": [
                        {
                            "name": "Чемпион",
                            "description": "Победитель сезона",
                            "acquire_method_label": "выдаёт администратор",
                            "acquire_hint": "Выиграть финал сезона",
                        }
                    ],
                }
            ],
        }

        embed = base._build_discord_roles_catalog_embed(page_data)

        assert "2/3" in embed.description
        assert embed.fields[0].name == "Турниры (продолжение 2/2)"
