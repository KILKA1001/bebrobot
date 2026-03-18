import asyncio
import importlib.util
import pathlib
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


_CORE_LOGIC_PATH = pathlib.Path(__file__).resolve().parents[1] / "bot" / "systems" / "core_logic.py"
_CORE_LOGIC_SPEC = importlib.util.spec_from_file_location("tests._core_logic_under_test", _CORE_LOGIC_PATH)
core_logic = importlib.util.module_from_spec(_CORE_LOGIC_SPEC)
sys.modules[_CORE_LOGIC_SPEC.name] = core_logic
assert _CORE_LOGIC_SPEC.loader is not None
_CORE_LOGIC_SPEC.loader.exec_module(core_logic)


class CoreLogicAccountFirstTests(unittest.TestCase):
    def test_build_balance_embed_prefers_account_first_scores(self):
        fake_db = SimpleNamespace(
            supabase=_FakeSupabase(
                scores_by_account={"acc-1": {"account_id": "acc-1", "points": 12, "tickets_normal": 2, "tickets_gold": 1}},
                leaderboard_rows=[{"account_id": "acc-1", "points": 12}],
            ),
            actions=[],
            history={111: [{"points": 999, "reason": "legacy"}]},
            scores={111: 999},
            ensure_core_data_loaded=lambda: None,
            _inc_metric=lambda *_args, **_kwargs: None,
        )
        member = _make_member(111, "Tester")

        with patch.object(core_logic, "db", fake_db):
            with patch.object(core_logic.AccountsService, "resolve_account_id", return_value="acc-1") as mock_resolve:
                with self.assertLogs(core_logic.logger.name, level="WARNING") as captured:
                    embed = core_logic.build_balance_embed(member)

        self.assertEqual(mock_resolve.call_args.args, ("discord", "111"))
        self.assertEqual(embed.fields[0].value, "12")
        self.assertEqual(embed.fields[1].value, "2")
        self.assertEqual(embed.fields[2].value, "1")
        combined = "\n".join(captured.output)
        self.assertIn("legacy identity path detected", combined)
        self.assertNotIn("legacy schema fallback", combined)

    def test_build_balance_embed_logs_legacy_scores_user_id_fallback(self):
        fake_db = SimpleNamespace(
            supabase=_FakeSupabase(
                scores_by_user={"111": {"user_id": "111", "points": 15, "tickets_normal": 0, "tickets_gold": 0}},
                leaderboard_rows=[{"user_id": "111", "points": 15}],
            ),
            actions=[],
            history={},
            scores={111: 15},
            ensure_core_data_loaded=lambda: None,
            _inc_metric=lambda *_args, **_kwargs: None,
        )
        member = _make_member(111, "Tester")

        with patch.object(core_logic, "db", fake_db):
            with patch.object(core_logic.AccountsService, "resolve_account_id", return_value="acc-1"):
                with self.assertLogs(core_logic.logger.name, level="WARNING") as captured:
                    embed = core_logic.build_balance_embed(member)

        self.assertEqual(embed.fields[0].value, "15")
        combined = "\n".join(captured.output)
        self.assertIn("legacy schema fallback", combined)
        self.assertIn("table=scores", combined)
        self.assertIn("field=user_id", combined)
        self.assertIn("developer_hint=temporary compatibility path; migrate scores rows to scores.account_id", combined)

    def test_render_history_logs_legacy_actions_user_id_fallback(self):
        fake_db = SimpleNamespace(
            supabase=None,
            actions=[
                {
                    "user_id": 111,
                    "points": 5,
                    "reason": "Бонус за активность",
                    "author_account_id": "acc-admin",
                    "timestamp": "2026-01-01T00:00:00+00:00",
                }
            ],
            history={},
            scores={111: 500},
            ensure_core_data_loaded=lambda: None,
            _inc_metric=lambda *_args, **_kwargs: None,
        )
        ctx = _FakeContext()
        member = _make_member(111, "Tester")

        with patch.object(core_logic, "db", fake_db):
            with patch.object(core_logic.AccountsService, "resolve_account_id", return_value="acc-1"):
                with self.assertLogs(core_logic.logger.name, level="WARNING") as captured:
                    asyncio.run(core_logic.render_history(ctx, member, 1))

        self.assertIsNotNone(ctx.embed)
        self.assertEqual(ctx.embed.fields[0].value, "```5 баллов```")
        combined = "\n".join(captured.output)
        self.assertIn("legacy schema fallback", combined)
        self.assertIn("table=actions", combined)
        self.assertIn("field=user_id", combined)

    def test_update_roles_uses_account_first_balance_snapshot(self):
        threshold_role = SimpleNamespace(id=777, name="Gold")
        base_role = SimpleNamespace(id=1, name="Base")
        fake_guild = SimpleNamespace(get_role=lambda role_id: threshold_role if role_id == 777 else None)
        fake_member = SimpleNamespace(
            id=111,
            roles=[base_role],
            guild=fake_guild,
            edit=AsyncMock(),
        )
        fake_db = SimpleNamespace(
            supabase=_FakeSupabase(scores_by_account={"acc-1": {"account_id": "acc-1", "points": 80}}),
            actions=[],
            history={},
            scores={111: 0},
            ensure_core_data_loaded=lambda: None,
            _inc_metric=lambda *_args, **_kwargs: None,
        )

        with patch.object(core_logic, "db", fake_db):
            with patch.object(core_logic, "ROLE_THRESHOLDS", {777: 50}):
                with patch.object(core_logic.AccountsService, "resolve_account_id", return_value="acc-1"):
                    asyncio.run(core_logic.update_roles(fake_member))

        fake_member.edit.assert_awaited_once()
        edited_roles = fake_member.edit.await_args.kwargs["roles"]
        self.assertEqual({role.id for role in edited_roles}, {1, 777})


class _FakeContext:
    def __init__(self):
        self.embed = None

    async def send(self, *, embed=None, **_kwargs):
        self.embed = embed


class _FakeResponse:
    def __init__(self, data):
        self.data = data


class _FakeTable:
    def __init__(self, name, supabase):
        self.name = name
        self.supabase = supabase
        self.filters = []
        self.limit_value = None
        self.order_by = None
        self.order_desc = False

    def select(self, _fields):
        return self

    def eq(self, key, value):
        self.filters.append((key, str(value)))
        return self

    def limit(self, value):
        self.limit_value = value
        return self

    def order(self, key, desc=False):
        self.order_by = key
        self.order_desc = desc
        return self

    def execute(self):
        rows = []
        filters = dict(self.filters)
        if self.name == "scores":
            if "account_id" in filters:
                row = self.supabase.scores_by_account.get(filters["account_id"])
                rows = [row] if row else []
            elif "user_id" in filters:
                row = self.supabase.scores_by_user.get(filters["user_id"])
                rows = [row] if row else []
            else:
                rows = list(self.supabase.leaderboard_rows)
        if self.order_by:
            rows = sorted(rows, key=lambda row: float(row.get(self.order_by) or 0), reverse=self.order_desc)
        if self.limit_value is not None:
            rows = rows[: self.limit_value]
        return _FakeResponse(rows)


class _FakeSupabase:
    def __init__(self, *, scores_by_account=None, scores_by_user=None, leaderboard_rows=None):
        self.scores_by_account = scores_by_account or {}
        self.scores_by_user = scores_by_user or {}
        self.leaderboard_rows = leaderboard_rows or []

    def table(self, name):
        return _FakeTable(name, self)


def _make_member(user_id: int, display_name: str):
    avatar = SimpleNamespace(url="https://example.com/avatar.png")
    default_avatar = SimpleNamespace(url="https://example.com/default.png")
    return SimpleNamespace(
        id=user_id,
        display_name=display_name,
        avatar=avatar,
        default_avatar=default_avatar,
        roles=[],
    )


if __name__ == "__main__":
    unittest.main()
