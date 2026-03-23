from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import discord
from discord.ext import commands

from bot.commands.base import bot
from bot.commands.roles_admin import _resolve_discord_target
from bot.services import AuthorityService, ModerationService
from bot.systems.moderation_rep_ui import render_rep_preview_text, render_rep_result_text, render_violator_notification_text
from bot.utils import safe_send, send_temp

logger = logging.getLogger(__name__)


def _friendly_rep_error_text() -> str:
    return "Не удалось завершить /rep. Ничего не применено: обновите экран и попробуйте ещё раз."


async def _resolve_reply_message(ctx: commands.Context) -> discord.Message | None:
    reference = getattr(getattr(ctx, "message", None), "reference", None)
    if not reference or not reference.message_id or not getattr(ctx, "channel", None):
        return None
    resolved = getattr(reference, "resolved", None)
    if isinstance(resolved, discord.Message):
        return resolved
    try:
        return await ctx.channel.fetch_message(reference.message_id)
    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
        logger.exception(
            "rep reply lookup failed provider=%s chat_id=%s actor=%s target=%s violation_code=%s case_id=%s error_code=%s",
            "discord",
            getattr(ctx.channel, "id", None),
            getattr(ctx.author, "id", None),
            getattr(reference, "message_id", None),
            None,
            None,
            "reply_lookup_failed",
        )
        return None


def _actor_subject(user: discord.abc.User) -> dict[str, str]:
    return {
        "provider": "discord",
        "provider_user_id": str(user.id),
        "label": getattr(user, "mention", None) or getattr(user, "display_name", None) or str(user.id),
    }


def _target_label(target: dict[str, Any] | None) -> str:
    if not target:
        return "не выбран"
    return str(target.get("label") or target.get("provider_user_id") or "не выбран")


@dataclass
class DiscordRepFlowState:
    actor_id: int
    guild_id: int | None
    chat_id: int | None
    target: dict[str, Any] | None = None
    violation_code: str | None = None
    preview: dict[str, Any] | None = None
    result: dict[str, Any] | None = None
    status_text: str = ""
    target_hint: str = "Выберите пользователя через панель ниже. Для prefix-команды также можно указать reply, mention, username или display_name."
    is_applying: bool = False


class _RepTargetSelect(discord.ui.UserSelect):
    def __init__(self) -> None:
        super().__init__(placeholder="Шаг 1 — выберите нарушителя", min_values=1, max_values=1, row=0)

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if not isinstance(view, DiscordRepFlowView):
            await interaction.response.send_message("❌ Ошибка /rep. Откройте команду заново.", ephemeral=True)
            return
        member = interaction.guild.get_member(self.values[0].id) if interaction.guild else None
        selected_user = member or self.values[0]
        view.select_target(
            {
                "provider": "discord",
                "provider_user_id": str(selected_user.id),
                "label": getattr(selected_user, "mention", None) or getattr(selected_user, "display_name", None) or str(selected_user.id),
                "member": member,
                "matched_by": "interactive_user_select",
            },
            status_text="Шаг 1 завершён: нарушитель выбран. Теперь выберите вид нарушения кнопками ниже.",
        )
        view.log_event(
            "info",
            message="rep target selected",
            target_user_id=str(selected_user.id),
        )
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
            await interaction.response.send_message("❌ Ошибка /rep. Откройте команду заново.", ephemeral=True)
            return
        code = str(self.values[0] or "").strip()
        if code == "__empty__":
            await interaction.response.send_message("Нарушения пока не настроены.", ephemeral=True)
            return
        if not view.state.target:
            await interaction.response.send_message("Сначала выберите нарушителя.", ephemeral=True)
            return
        preview = view.build_preview(interaction.user, code)
        if not preview.get("ok"):
            await interaction.response.send_message(f"❌ {preview.get('message')}", ephemeral=True)
            return
        view.state.violation_code = code
        view.state.preview = preview
        view.state.status_text = "Шаг 3 готов: проверьте предпросмотр наказания, затем подтвердите или вернитесь назад."
        view.log_event("info", message="rep preview built")
        view.rebuild()
        await interaction.response.edit_message(embed=view.build_embed(), view=view)


class _RepBackButton(discord.ui.Button):
    def __init__(self, *, disabled: bool) -> None:
        super().__init__(label="Назад", style=discord.ButtonStyle.secondary, row=2, disabled=disabled)

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if not isinstance(view, DiscordRepFlowView):
            await interaction.response.send_message("❌ Ошибка /rep. Откройте команду заново.", ephemeral=True)
            return
        if view.state.result:
            await interaction.response.send_message("Сценарий уже завершён. Откройте /rep заново для нового кейса.", ephemeral=True)
            return
        if view.state.preview:
            view.state.preview = None
            view.state.violation_code = None
            view.state.status_text = "Возврат к шагу выбора нарушения. Пользователь сохранён, можно выбрать другой тип нарушения."
        elif view.state.target:
            view.state.target = None
            view.state.status_text = "Возврат к выбору пользователя."
            view.state.target_hint = "Выберите нарушителя через панель ниже или откройте /rep reply-сообщением для быстрого старта."
        view.rebuild()
        await interaction.response.edit_message(embed=view.build_embed(), view=view)


class _RepConfirmButton(discord.ui.Button):
    def __init__(self, *, disabled: bool) -> None:
        super().__init__(label="Подтвердить", style=discord.ButtonStyle.success, row=2, disabled=disabled)

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if not isinstance(view, DiscordRepFlowView):
            await interaction.response.send_message("❌ Ошибка /rep. Откройте команду заново.", ephemeral=True)
            return
        if view.state.is_applying or view.state.result:
            await interaction.response.send_message("Это действие уже обработано. Откройте /rep заново для нового кейса.", ephemeral=True)
            return
        if not view.state.target or not view.state.violation_code:
            await interaction.response.send_message("Сначала выберите нарушителя и нарушение.", ephemeral=True)
            return
        view.state.is_applying = True
        try:
            preview_payload = view.state.preview or {}
            preview_ui_payload = preview_payload.get("ui_payload") or {}
            result = ModerationService.commit_case(
                "discord",
                _actor_subject(interaction.user),
                view.state.target,
                view.state.violation_code,
                {
                    "chat_id": view.state.chat_id,
                    "source_platform": "discord",
                    "reason_text": "",
                    "moderation_op_key": preview_payload.get("moderation_op_key") or preview_ui_payload.get("moderation_op_key"),
                },
            )
            if not result.get("ok"):
                view.log_event(
                    "warning",
                    message="rep apply failure",
                    error_code=str(result.get("error_code") or "apply_failed"),
                    selected_actions=list(result.get("selected_actions") or []),
                )
                view.state.is_applying = False
                await interaction.response.send_message(f"❌ {result.get('user_message') or result.get('message') or _friendly_rep_error_text()}", ephemeral=True)
                return
            view.state.result = result
            view.state.status_text = "Шаг 5 завершён: кейс создан, итог показан модератору, уведомление нарушителю отправляется отдельно."
            view.log_event("info", message="rep apply success")
            await view.notify_target(result["ui_payload"])
            view.disable_all_items()
            await interaction.response.edit_message(embed=view.build_embed(), view=view)
        except Exception:
            view.state.is_applying = False
            view.log_event("exception", message="rep apply failure", error_code="apply_exception")
            await interaction.response.send_message(f"❌ {_friendly_rep_error_text()}", ephemeral=True)


class _RepCancelButton(discord.ui.Button):
    def __init__(self) -> None:
        super().__init__(label="Отмена", style=discord.ButtonStyle.secondary, row=2)

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if not isinstance(view, DiscordRepFlowView):
            await interaction.response.send_message("❌ Ошибка /rep. Откройте команду заново.", ephemeral=True)
            return
        view.disable_all_items()
        embed = discord.Embed(
            title="/rep отменён",
            description="Сценарий остановлен. Никаких действий не применено. Запустите /rep ещё раз, если нужно начать заново.",
            color=discord.Color.dark_grey(),
        )
        await interaction.response.edit_message(embed=embed, view=view)


class DiscordRepFlowView(discord.ui.View):
    def __init__(self, *, actor_id: int, guild_id: int | None, chat_id: int | None):
        super().__init__(timeout=300)
        self.state = DiscordRepFlowState(actor_id=actor_id, guild_id=guild_id, chat_id=chat_id)
        self._violations = ModerationService.list_active_violation_types()
        self.rebuild()

    def log_event(
        self,
        level: str,
        *,
        message: str,
        error_code: str | None = None,
        selected_actions: list[str] | None = None,
        target_user_id: str | None = None,
    ) -> None:
        payload = (self.state.result or self.state.preview or {})
        ui_payload = payload.get("ui_payload") or {}
        actor = payload.get("actor") or {}
        target = payload.get("target") or {}
        log_method = getattr(logger, level)
        log_method(
            "%s provider=%s chat_id=%s actor=%s actor_account_id=%s target=%s target_account_id=%s violation_code=%s selected_actions=%s case_id=%s error_code=%s",
            message,
            "discord",
            self.state.chat_id,
            self.state.actor_id,
            actor.get("account_id"),
            target_user_id or target.get("provider_user_id") or ((self.state.target or {}).get("provider_user_id")),
            target.get("account_id") or ((self.state.target or {}).get("account_id")),
            self.state.violation_code,
            list(selected_actions or ui_payload.get("selected_actions") or []),
            ui_payload.get("case_id"),
            error_code,
        )

    def select_target(self, target: dict[str, Any], *, status_text: str, target_hint: str | None = None) -> None:
        self.state.target = dict(target)
        self.state.violation_code = None
        self.state.preview = None
        self.state.status_text = status_text
        if target_hint:
            self.state.target_hint = target_hint
        self.rebuild()

    def build_preview(self, actor_user: discord.abc.User, code: str) -> dict[str, Any]:
        try:
            preview = ModerationService.prepare_moderation_payload(
                "discord",
                _actor_subject(actor_user),
                self.state.target,
                code,
                {"chat_id": self.state.chat_id, "source_platform": "discord", "reason_text": ""},
            )
        except Exception:
            self.log_event("exception", message="rep preview failure", error_code="preview_exception")
            return {"ok": False, "message": _friendly_rep_error_text()}
        if not preview.get("ok"):
            self.log_event(
                "warning",
                message="rep authority deny",
                error_code=str(preview.get("error_code") or "preview_failed"),
                selected_actions=list(preview.get("selected_actions") or []),
            )
            return {
                "ok": False,
                "message": str(preview.get("message") or "Действие сейчас недоступно. Проверьте выбранную цель и ваши полномочия."),
            }
        return preview

    def rebuild(self) -> None:
        self.clear_items()
        self.add_item(_RepTargetSelect())
        self.add_item(_RepViolationSelect(self._violations, disabled=self.state.target is None))
        self.add_item(_RepBackButton(disabled=not (self.state.target or self.state.preview)))
        self.add_item(_RepConfirmButton(disabled=self.state.preview is None))
        self.add_item(_RepCancelButton())

    def disable_all_items(self) -> None:
        for child in self.children:
            child.disabled = True

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.state.actor_id:
            self.log_event("warning", message="rep interaction denied", error_code="foreign_actor")
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
            embed = discord.Embed(title="🧾 Предпросмотр /rep", description=render_rep_preview_text(ui_payload), color=discord.Color.orange())
            embed.add_field(name="Статус", value=self.state.status_text or "Проверьте итог и подтвердите применение.", inline=False)
            return embed
        embed = discord.Embed(title="🛡️ /rep", color=discord.Color.blurple())
        embed.description = (
            "Единый интерактивный мастер модерации: бот сам подбирает наказание по доменной модели и authority-проверкам.\n\n"
            "**Как пользоваться**\n"
            "• Шаг 1: выберите нарушителя.\n"
            "• Шаг 2: выберите нарушение кнопками.\n"
            "• Шаг 3: проверьте предпросмотр наказания и следующий шаг эскалации.\n"
            "• Шаг 4: подтвердите или отмените действие.\n\n"
            f"**Текущая цель:** {_target_label(self.state.target)}\n"
            f"**Подсказка:** {self.state.target_hint}"
        )
        if self.state.status_text:
            embed.add_field(name="Короткий статус", value=self.state.status_text, inline=False)
        embed.add_field(
            name="Как это работает",
            value=(
                "• Наказание выбирается автоматически по типу нарушения и числу предупреждений.\n"
                "• Вручную менять исход в этом сценарии не нужно.\n"
                "• Если расчёт кажется неверным — нажмите «Отмена» и проверьте историю пользователя."
            ),
            inline=False,
        )
        return embed

    async def notify_target(self, ui_payload: dict[str, Any]) -> None:
        member = (self.state.target or {}).get("member")
        if not member:
            return
        try:
            await safe_send(member, render_violator_notification_text(ui_payload))
        except Exception:
            self.log_event("exception", message="rep target notify failed", error_code="target_notify_failed")

    async def on_timeout(self) -> None:
        self.disable_all_items()


async def _prefill_target_from_context(ctx: commands.Context, raw_target: str | None) -> dict[str, Any] | None:
    if raw_target:
        return await _resolve_discord_target(ctx, raw_target, operation="rep")
    reply_message = await _resolve_reply_message(ctx)
    if reply_message and getattr(reply_message, "author", None) and not reply_message.author.bot:
        reply_author = reply_message.author
        return {
            "account_id": None,
            "provider": "discord",
            "provider_user_id": str(reply_author.id),
            "member": ctx.guild.get_member(reply_author.id) if ctx.guild else None,
            "label": reply_author.mention,
            "matched_by": "reply",
        }
    return None


@bot.hybrid_command(name="rep", description="Интерактивная единая команда модерации")
async def rep(ctx: commands.Context, *, target: str | None = None):
    if not AuthorityService.has_command_permission("discord", str(ctx.author.id), "moderation_mute"):
        await send_temp(ctx, "❌ Команда /rep доступна только ролям модерации. Если доступ должен быть — проверьте authority и попробуйте ещё раз.")
        return
    view = DiscordRepFlowView(
        actor_id=ctx.author.id,
        guild_id=ctx.guild.id if ctx.guild else None,
        chat_id=ctx.channel.id if ctx.channel else (ctx.guild.id if ctx.guild else None),
    )
    prefilled_target = await _prefill_target_from_context(ctx, target)
    if target and not prefilled_target:
        return
    if prefilled_target:
        matched_by = str(prefilled_target.get("matched_by") or "lookup")
        target_hint = (
            "Нарушитель подставлен из reply-контекста. При необходимости можно выбрать другого пользователя в панели ниже."
            if matched_by == "reply"
            else "Нарушитель подставлен из lookup. При необходимости можно выбрать другого пользователя в панели ниже."
        )
        view.select_target(
            prefilled_target,
            status_text="Шаг 1 уже заполнен автоматически. Теперь выберите вид нарушения.",
            target_hint=target_hint,
        )
    logger.info(
        "rep start provider=%s chat_id=%s actor=%s actor_account_id=%s target=%s target_account_id=%s violation_code=%s selected_actions=%s case_id=%s error_code=%s",
        "discord",
        view.state.chat_id,
        ctx.author.id,
        None,
        (prefilled_target or {}).get("provider_user_id"),
        (prefilled_target or {}).get("account_id"),
        None,
        [],
        None,
        None,
    )
    await send_temp(ctx, embed=view.build_embed(), view=view, delete_after=None)
