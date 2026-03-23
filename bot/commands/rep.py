from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import discord
from discord.ext import commands

from bot.commands.base import bot
from bot.services import AuthorityService, ModerationService
from bot.systems.moderation_rep_ui import render_rep_preview_text, render_rep_result_text, render_violator_notification_text
from bot.utils import safe_send, send_temp

logger = logging.getLogger(__name__)


@dataclass
class DiscordRepFlowState:
    actor_id: int
    guild_id: int | None
    chat_id: int | None
    target: dict[str, Any] | None = None
    violation_code: str | None = None
    preview: dict[str, Any] | None = None
    result: dict[str, Any] | None = None


class _RepTargetSelect(discord.ui.UserSelect):
    def __init__(self) -> None:
        super().__init__(placeholder="Шаг 1 — выберите нарушителя", min_values=1, max_values=1, row=0)

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if not isinstance(view, DiscordRepFlowView):
            await interaction.response.send_message("❌ Ошибка /rep. Проверь логи и попробуй ещё раз.", ephemeral=True)
            return
        member = interaction.guild.get_member(self.values[0].id) if interaction.guild else None
        selected_user = member or self.values[0]
        view.state.target = {
            "provider": "discord",
            "provider_user_id": str(selected_user.id),
            "label": getattr(selected_user, "mention", None) or getattr(selected_user, "display_name", None) or str(selected_user.id),
            "member": member,
        }
        view.state.violation_code = None
        view.state.preview = None
        logger.info(
            "rep target selected provider=%s chat_id=%s actor_account_id=%s target_account_id=%s violation_code=%s selected_actions=%s case_id=%s error_code=%s target_user_id=%s",
            "discord",
            view.state.chat_id,
            None,
            None,
            None,
            [],
            None,
            None,
            selected_user.id,
        )
        view.rebuild()
        await interaction.response.edit_message(embed=view.build_embed(), view=view)


class _RepViolationSelect(discord.ui.Select):
    def __init__(self, options_payload: list[dict[str, Any]], *, disabled: bool) -> None:
        options = [
            discord.SelectOption(label=str(item.get("title") or item.get("code") or "Нарушение")[:100], value=str(item.get("code") or ""))
            for item in options_payload[:25]
            if str(item.get("code") or "").strip()
        ]
        if not options:
            options = [discord.SelectOption(label="Нет активных нарушений", value="__empty__")]
            disabled = True
        super().__init__(
            placeholder="Шаг 2 — выберите нарушение",
            min_values=1,
            max_values=1,
            options=options,
            row=1,
            disabled=disabled,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if not isinstance(view, DiscordRepFlowView):
            await interaction.response.send_message("❌ Ошибка /rep. Проверь логи и попробуй ещё раз.", ephemeral=True)
            return
        code = str(self.values[0] or "").strip()
        if code == "__empty__":
            await interaction.response.send_message("Нарушения пока не настроены.", ephemeral=True)
            return
        if not view.state.target:
            await interaction.response.send_message("Сначала выберите нарушителя.", ephemeral=True)
            return
        view.state.violation_code = code
        try:
            preview = ModerationService.prepare_moderation_payload(
                "discord",
                {"provider": "discord", "provider_user_id": str(interaction.user.id), "label": interaction.user.mention},
                view.state.target,
                code,
                {"chat_id": view.state.chat_id, "source_platform": "discord", "reason_text": ""},
            )
            if not preview.get("ok"):
                error_code = str(preview.get("error_code") or "preview_failed")
                logger.warning(
                    "rep authority deny provider=%s chat_id=%s actor_account_id=%s target_account_id=%s violation_code=%s selected_actions=%s case_id=%s error_code=%s",
                    "discord",
                    view.state.chat_id,
                    ((preview.get("actor") or {}).get("account_id") if isinstance(preview.get("actor"), dict) else None),
                    ((preview.get("target") or {}).get("account_id") if isinstance(preview.get("target"), dict) else None),
                    code,
                    list(preview.get("selected_actions") or []),
                    None,
                    error_code,
                )
                await interaction.response.send_message(f"❌ {preview.get('message') or 'Не удалось построить предпросмотр.'}", ephemeral=True)
                return
            view.state.preview = preview
            actor = preview["actor"]
            target = preview["target"]
            ui_payload = preview["ui_payload"]
            logger.info(
                "rep preview built provider=%s chat_id=%s actor_account_id=%s target_account_id=%s violation_code=%s selected_actions=%s case_id=%s error_code=%s",
                "discord",
                view.state.chat_id,
                actor.get("account_id"),
                target.get("account_id"),
                code,
                list(ui_payload.get("selected_actions") or []),
                None,
                None,
            )
            view.rebuild()
            await interaction.response.edit_message(embed=view.build_embed(), view=view)
        except Exception:
            logger.exception(
                "rep preview failure provider=%s chat_id=%s actor_account_id=%s target_account_id=%s violation_code=%s selected_actions=%s case_id=%s error_code=%s",
                "discord",
                view.state.chat_id,
                None,
                None,
                code,
                [],
                None,
                "preview_exception",
            )
            await interaction.response.send_message("❌ Не удалось построить предпросмотр. Обнови экран и проверь логи.", ephemeral=True)


class _RepConfirmButton(discord.ui.Button):
    def __init__(self, *, disabled: bool) -> None:
        super().__init__(label="Подтвердить", style=discord.ButtonStyle.success, row=2, disabled=disabled)

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if not isinstance(view, DiscordRepFlowView):
            await interaction.response.send_message("❌ Ошибка /rep. Проверь логи и попробуй ещё раз.", ephemeral=True)
            return
        if not view.state.target or not view.state.violation_code:
            await interaction.response.send_message("Сначала выберите нарушителя и нарушение.", ephemeral=True)
            return
        try:
            result = ModerationService.moderate(
                "discord",
                {"provider": "discord", "provider_user_id": str(interaction.user.id), "label": interaction.user.mention},
                view.state.target,
                view.state.violation_code,
                {"chat_id": view.state.chat_id, "source_platform": "discord", "reason_text": ""},
            )
            if not result.get("ok"):
                logger.warning(
                    "rep apply failure provider=%s chat_id=%s actor_account_id=%s target_account_id=%s violation_code=%s selected_actions=%s case_id=%s error_code=%s",
                    "discord",
                    view.state.chat_id,
                    ((result.get("actor") or {}).get("account_id") if isinstance(result.get("actor"), dict) else None),
                    ((result.get("target") or {}).get("account_id") if isinstance(result.get("target"), dict) else None),
                    view.state.violation_code,
                    list(result.get("selected_actions") or []),
                    None,
                    result.get("error_code") or "apply_failed",
                )
                await interaction.response.send_message(f"❌ {result.get('message') or 'Не удалось применить модерацию.'}", ephemeral=True)
                return
            view.state.result = result
            ui_payload = result["ui_payload"]
            logger.info(
                "rep apply success provider=%s chat_id=%s actor_account_id=%s target_account_id=%s violation_code=%s selected_actions=%s case_id=%s error_code=%s",
                "discord",
                view.state.chat_id,
                result["actor"].get("account_id"),
                result["target"].get("account_id"),
                view.state.violation_code,
                list(ui_payload.get("selected_actions") or []),
                ui_payload.get("case_id"),
                None,
            )
            await view.notify_target(ui_payload)
            view.disable_all_items()
            await interaction.response.edit_message(embed=view.build_embed(), view=view)
        except Exception:
            preview = view.state.preview or {}
            ui_payload = preview.get("ui_payload") or {}
            logger.exception(
                "rep apply failure provider=%s chat_id=%s actor_account_id=%s target_account_id=%s violation_code=%s selected_actions=%s case_id=%s error_code=%s",
                "discord",
                view.state.chat_id,
                ((preview.get("actor") or {}).get("account_id") if isinstance(preview.get("actor"), dict) else None),
                ((preview.get("target") or {}).get("account_id") if isinstance(preview.get("target"), dict) else None),
                view.state.violation_code,
                list(ui_payload.get("selected_actions") or []),
                None,
                "apply_exception",
            )
            await interaction.response.send_message("❌ Не удалось применить модерацию. Обнови экран, проверь логи и попробуй ещё раз.", ephemeral=True)


class _RepCancelButton(discord.ui.Button):
    def __init__(self) -> None:
        super().__init__(label="Отмена", style=discord.ButtonStyle.secondary, row=2)

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if not isinstance(view, DiscordRepFlowView):
            await interaction.response.send_message("❌ Ошибка /rep. Проверь логи и попробуй ещё раз.", ephemeral=True)
            return
        view.disable_all_items()
        embed = discord.Embed(title="/rep отменён", description="Сценарий остановлен. Запусти /rep ещё раз, если нужно начать заново.", color=discord.Color.dark_grey())
        await interaction.response.edit_message(embed=embed, view=view)


class DiscordRepFlowView(discord.ui.View):
    def __init__(self, *, actor_id: int, guild_id: int | None, chat_id: int | None):
        super().__init__(timeout=300)
        self.state = DiscordRepFlowState(actor_id=actor_id, guild_id=guild_id, chat_id=chat_id)
        self._violations = ModerationService.list_active_violation_types()
        self.rebuild()

    def rebuild(self) -> None:
        self.clear_items()
        self.add_item(_RepTargetSelect())
        self.add_item(_RepViolationSelect(self._violations, disabled=self.state.target is None))
        self.add_item(_RepConfirmButton(disabled=self.state.preview is None))
        self.add_item(_RepCancelButton())

    def disable_all_items(self) -> None:
        for child in self.children:
            child.disabled = True

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.state.actor_id:
            logger.warning(
                "rep interaction denied provider=%s chat_id=%s actor_account_id=%s target_account_id=%s violation_code=%s selected_actions=%s case_id=%s error_code=%s foreign_actor_id=%s owner_id=%s",
                "discord",
                self.state.chat_id,
                None,
                None,
                self.state.violation_code,
                list((((self.state.preview or {}).get('ui_payload') or {}).get('selected_actions') or [])),
                (((self.state.result or {}).get('ui_payload') or {}).get('case_id')),
                "foreign_actor",
                interaction.user.id,
                self.state.actor_id,
            )
            await interaction.response.send_message("Эта панель /rep открыта другим модератором.", ephemeral=True)
            return False
        return True

    def build_embed(self) -> discord.Embed:
        if self.state.result:
            ui_payload = self.state.result["ui_payload"]
            embed = discord.Embed(title="✅ /rep завершён", description=render_rep_result_text(ui_payload), color=discord.Color.green())
            return embed
        if self.state.preview:
            ui_payload = self.state.preview["ui_payload"]
            embed = discord.Embed(title="Предпросмотр /rep", description=render_rep_preview_text(ui_payload), color=discord.Color.orange())
            return embed
        target_line = self.state.target.get("label") if self.state.target else "не выбран"
        embed = discord.Embed(title="🛡️ /rep", color=discord.Color.blurple())
        embed.description = (
            "Единая команда модерации с авторасчётом наказания.\n\n"
            f"Шаг 1: выберите нарушителя. Сейчас: **{target_line}**\n"
            "Шаг 2: выберите нарушение.\n"
            "Шаг 3: бот покажет авторасчёт наказания и следующий шаг эскалации.\n"
            "Шаг 4: подтвердите или отмените."
        )
        return embed

    async def notify_target(self, ui_payload: dict[str, Any]) -> None:
        member = (self.state.target or {}).get("member")
        if not member:
            return
        try:
            await safe_send(member, render_violator_notification_text(ui_payload))
        except Exception:
            logger.exception(
                "rep target notify failed provider=%s chat_id=%s actor_account_id=%s target_account_id=%s violation_code=%s selected_actions=%s case_id=%s error_code=%s",
                "discord",
                self.state.chat_id,
                ui_payload.get("actor_account_id"),
                ui_payload.get("target_account_id"),
                ui_payload.get("violation_code"),
                list(ui_payload.get("selected_actions") or []),
                ui_payload.get("case_id"),
                "target_notify_failed",
            )

    async def on_timeout(self) -> None:
        self.disable_all_items()


@bot.hybrid_command(name="rep", description="Интерактивная единая команда модерации")
async def rep(ctx: commands.Context):
    if not AuthorityService.has_command_permission("discord", str(ctx.author.id), "moderation_mute"):
        await send_temp(ctx, "❌ Команда /rep доступна только ролям модерации. Если доступ должен быть — проверь authority и попробуй ещё раз.")
        return
    logger.info(
        "rep start provider=%s chat_id=%s actor_account_id=%s target_account_id=%s violation_code=%s selected_actions=%s case_id=%s error_code=%s",
        "discord",
        ctx.channel.id if ctx.channel else (ctx.guild.id if ctx.guild else None),
        None,
        None,
        None,
        [],
        None,
        None,
    )
    view = DiscordRepFlowView(
        actor_id=ctx.author.id,
        guild_id=ctx.guild.id if ctx.guild else None,
        chat_id=ctx.channel.id if ctx.channel else (ctx.guild.id if ctx.guild else None),
    )
    await send_temp(ctx, embed=view.build_embed(), view=view, delete_after=None)
