from __future__ import annotations

import logging
from datetime import timedelta
from dataclasses import dataclass
from typing import Any

import discord
from discord.ext import commands

from bot.commands.base import bot
from bot.commands.roles_admin import _resolve_discord_target
from bot.services import AuthorityService, ModerationNotificationsService, ModerationService
from bot.systems.moderation_rep_ui import (
    render_rep_apply_error_text,
    render_rep_authority_deny_text,
    render_rep_cancelled_text,
    render_rep_duplicate_submit_text,
    render_rep_expired_text,
    render_rep_foreign_actor_text,
    render_rep_preview_text,
    render_rep_preview_failed_text,
    render_rep_result_text,
    render_rep_session_status_text,
    render_rep_start_text,
    render_rep_target_not_found_text,
    render_rep_target_prompt_text,
    render_violator_notification_text,
    render_rep_violation_prompt_text,
)
from bot.utils import safe_send, send_temp

logger = logging.getLogger(__name__)


def _friendly_rep_error_text() -> str:
    return render_rep_apply_error_text()


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


async def _apply_discord_sanctions(*, interaction: discord.Interaction, target: dict[str, Any], ui_payload: dict[str, Any]) -> dict[str, Any]:
    guild = interaction.guild
    if guild is None:
        return {"ok": False, "reason": "guild_missing"}
    target_user_id = int(str((target or {}).get("provider_user_id") or "0") or 0)
    if not target_user_id:
        return {"ok": False, "reason": "target_not_found"}
    member = guild.get_member(target_user_id)
    if member is None:
        return {"ok": False, "reason": "member_missing"}
    actions = set(ui_payload.get("selected_actions") or [])
    duration_minutes = int(ui_payload.get("mute_minutes") or ui_payload.get("ban_minutes") or ui_payload.get("action_duration_minutes") or 0)
    try:
        if "mute" in actions:
            until = discord.utils.utcnow() + timedelta(minutes=max(1, duration_minutes))
            await member.edit(timeout=until, reason=f"/rep mute by {interaction.user.id}")
        if "ban" in actions:
            await guild.ban(member, reason=f"/rep ban by {interaction.user.id}", delete_message_days=0)
        if "kick" in actions:
            await guild.kick(member, reason=f"/rep kick by {interaction.user.id}")
        return {"ok": True}
    except Exception:
        logger.exception(
            "discord rep sanction apply failed actor_id=%s target_id=%s guild_id=%s actions=%s duration_minutes=%s",
            interaction.user.id,
            target_user_id,
            guild.id,
            list(actions),
            duration_minutes,
        )
        return {"ok": False, "reason": "sanction_apply_failed"}


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
    target_hint: str = "Reply на сообщение — самый быстрый способ. Иначе используйте панель ниже; для prefix-команды подойдут reply, mention, username или display_name."
    is_applying: bool = False
    hidden_violations_count: int = 0
    manual_action: str | None = None
    manual_duration_minutes: int | None = None
    manual_reason_text: str = ""
    show_rules_menu: bool = False


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
            await interaction.response.send_message(f"❌ {preview.get('message') or render_rep_preview_failed_text()}", ephemeral=True)
            return
        view.state.violation_code = code
        view.state.preview = preview
        view.state.manual_action = None
        view.state.manual_duration_minutes = None
        view.state.manual_reason_text = ""
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
            view.state.status_text = "Возврат к шагу 2: пользователь сохранён, можно выбрать другой тип нарушения."
        elif view.state.manual_action:
            view.state.manual_action = None
            view.state.manual_duration_minutes = None
            view.state.manual_reason_text = ""
            view.state.status_text = "Ручное наказание сброшено. Выберите другой тип действия."
        elif view.state.show_rules_menu:
            view.state.show_rules_menu = False
            view.state.status_text = "Возврат к основным 5 кнопкам действий."
        elif view.state.target:
            view.state.target = None
            view.state.status_text = "Возврат к шагу 1: выберите другого пользователя."
            view.state.target_hint = "Reply на сообщение — самый быстрый способ. Также можно выбрать нарушителя через панель ниже."
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
            await interaction.response.send_message(render_rep_duplicate_submit_text(), ephemeral=True)
            return
        if not view.state.target or (not view.state.violation_code and not view.state.manual_action):
            await interaction.response.send_message("Сначала выберите нарушителя и тип наказания.", ephemeral=True)
            return
        view.state.is_applying = True
        try:
            if view.state.manual_action:
                result = ModerationService.commit_manual_action(
                    "discord",
                    _actor_subject(interaction.user),
                    view.state.target,
                    view.state.manual_action,
                    duration_minutes=int(view.state.manual_duration_minutes or 0),
                    reason_text=view.state.manual_reason_text,
                    context={"chat_id": view.state.chat_id, "source_platform": "discord"},
                )
            else:
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
            sanction_result = await _apply_discord_sanctions(interaction=interaction, target=view.state.target or {}, ui_payload=result.get("ui_payload") or {})
            if not sanction_result.get("ok"):
                view.state.status_text = (
                    "Кейс создан, но санкция не применилась на сервере "
                    "(права бота/роль цели). Проверьте /modstatus."
                )
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
            description=render_rep_cancelled_text(),
            color=discord.Color.dark_grey(),
        )
        await interaction.response.edit_message(embed=embed, view=view)


class _RepManualActionModal(discord.ui.Modal, title="Ручное наказание"):
    def __init__(self, action_key: str) -> None:
        super().__init__(timeout=180)
        self.action_key = action_key
        self.duration = discord.ui.TextInput(label="Срок (пример: 30m, 2h, 1d)", required=True, max_length=32)
        self.reason = discord.ui.TextInput(label="Причина", required=True, style=discord.TextStyle.paragraph, max_length=300)
        self.add_item(self.duration)
        self.add_item(self.reason)

    @staticmethod
    def _parse_duration(raw: str) -> int:
        text = str(raw or "").strip().lower().replace(" ", "")
        if text.endswith("m") and text[:-1].isdigit():
            return int(text[:-1])
        if text.endswith("h") and text[:-1].isdigit():
            return int(text[:-1]) * 60
        if text.endswith("d") and text[:-1].isdigit():
            return int(text[:-1]) * 24 * 60
        if text.isdigit():
            return int(text)
        return 0

    async def on_submit(self, interaction: discord.Interaction) -> None:
        view = self.parent
        if not isinstance(view, DiscordRepFlowView):
            await interaction.response.send_message("❌ Ошибка состояния. Откройте /rep заново.", ephemeral=True)
            return
        minutes = self._parse_duration(str(self.duration.value))
        reason_text = str(self.reason.value or "").strip()
        if minutes <= 0 or not reason_text:
            await interaction.response.send_message("❌ Укажите корректный срок и причину.", ephemeral=True)
            return
        view.state.manual_action = self.action_key
        view.state.manual_duration_minutes = minutes
        view.state.manual_reason_text = reason_text
        view.state.preview = None
        view.state.violation_code = None
        view.state.status_text = (
            f"Подготовлено ручное наказание: {self.action_key} на {minutes} мин. "
            "Проверьте данные и подтвердите."
        )
        view.rebuild()
        await interaction.response.edit_message(embed=view.build_embed(), view=view)


class _RepManualActionButton(discord.ui.Button):
    def __init__(self, *, action_key: str, title: str, disabled: bool) -> None:
        super().__init__(label=title, style=discord.ButtonStyle.secondary, row=3, disabled=disabled)
        self.action_key = action_key

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if not isinstance(view, DiscordRepFlowView):
            await interaction.response.send_message("❌ Ошибка /rep. Откройте команду заново.", ephemeral=True)
            return
        if not view.state.target:
            await interaction.response.send_message("Сначала выберите нарушителя.", ephemeral=True)
            return
        view.state.manual_action = self.action_key
        view.state.manual_duration_minutes = None
        view.state.manual_reason_text = ""
        view.state.preview = None
        view.state.violation_code = None
        view.state.show_rules_menu = False
        view.state.status_text = (
            f"Выбрано действие `{self.action_key}`. "
            "Выберите быстрый срок кнопками ниже или нажмите «Свой срок + причина»."
        )
        view.rebuild()
        await interaction.response.edit_message(embed=view.build_embed(), view=view)


class _RepManualPresetDurationButton(discord.ui.Button):
    def __init__(self, *, title: str, minutes: int, disabled: bool) -> None:
        super().__init__(label=title, style=discord.ButtonStyle.secondary, row=4, disabled=disabled)
        self.minutes = minutes

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if not isinstance(view, DiscordRepFlowView):
            await interaction.response.send_message("❌ Ошибка /rep. Откройте команду заново.", ephemeral=True)
            return
        if not view.state.manual_action:
            await interaction.response.send_message("Сначала выберите действие (Мут/Пред/Бан/Кик).", ephemeral=True)
            return
        view.state.manual_duration_minutes = self.minutes
        modal = _RepManualReasonModal()
        modal.parent = view
        await interaction.response.send_modal(modal)


class _RepManualCustomDurationButton(discord.ui.Button):
    def __init__(self, *, disabled: bool) -> None:
        super().__init__(label="Свой срок + причина", style=discord.ButtonStyle.primary, row=4, disabled=disabled)

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if not isinstance(view, DiscordRepFlowView):
            await interaction.response.send_message("❌ Ошибка /rep. Откройте команду заново.", ephemeral=True)
            return
        if not view.state.manual_action:
            await interaction.response.send_message("Сначала выберите действие (Мут/Пред/Бан/Кик).", ephemeral=True)
            return
        modal = _RepManualActionModal(view.state.manual_action)
        modal.parent = view
        await interaction.response.send_modal(modal)


class _RepManualReasonModal(discord.ui.Modal, title="Причина наказания"):
    def __init__(self) -> None:
        super().__init__(timeout=180)
        self.reason = discord.ui.TextInput(label="Причина", required=True, style=discord.TextStyle.paragraph, max_length=300)
        self.add_item(self.reason)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        view = self.parent
        if not isinstance(view, DiscordRepFlowView):
            await interaction.response.send_message("❌ Ошибка состояния. Откройте /rep заново.", ephemeral=True)
            return
        reason_text = str(self.reason.value or "").strip()
        if not reason_text:
            await interaction.response.send_message("❌ Причина обязательна.", ephemeral=True)
            return
        if not view.state.manual_duration_minutes or view.state.manual_duration_minutes <= 0:
            await interaction.response.send_message("❌ Сначала выберите срок наказания.", ephemeral=True)
            return
        view.state.manual_reason_text = reason_text
        view.state.status_text = (
            f"Подготовлено ручное наказание: {view.state.manual_action} "
            f"на {view.state.manual_duration_minutes} мин. Проверьте и подтвердите."
        )
        view.rebuild()
        await interaction.response.edit_message(embed=view.build_embed(), view=view)


class _RepEscalationRequestButton(discord.ui.Button):
    def __init__(self, *, disabled: bool) -> None:
        super().__init__(label="📨 Заявка старшему админу", style=discord.ButtonStyle.secondary, row=4, disabled=disabled)

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if not isinstance(view, DiscordRepFlowView):
            await interaction.response.send_message("❌ Ошибка /rep. Откройте команду заново.", ephemeral=True)
            return
        if not view.state.target:
            await interaction.response.send_message("Сначала выберите нарушителя.", ephemeral=True)
            return
        text = (
            f"📨 Заявка на недоступное наказание\n"
            f"Модератор: {interaction.user.mention}\n"
            f"Цель: {_target_label(view.state.target)}\n"
            f"Скрытых нарушений по полномочиям: {view.state.hidden_violations_count}\n"
            "Нужна проверка и подтверждение старшим администратором."
        )
        try:
            if interaction.channel:
                await interaction.channel.send(text)
            await interaction.response.send_message("✅ Заявка отправлена в чат для старших администраторов.", ephemeral=True)
        except Exception:
            logger.exception(
                "rep escalation request failed provider=%s chat_id=%s actor=%s target=%s",
                "discord",
                view.state.chat_id,
                interaction.user.id,
                (view.state.target or {}).get("provider_user_id"),
            )
            await interaction.response.send_message("❌ Не удалось отправить заявку. Подробности в консоли.", ephemeral=True)


class _RepRulesMenuButton(discord.ui.Button):
    def __init__(self, *, disabled: bool) -> None:
        super().__init__(label="📚 Нарушения из правил", style=discord.ButtonStyle.primary, row=3, disabled=disabled)

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if not isinstance(view, DiscordRepFlowView):
            await interaction.response.send_message("❌ Ошибка /rep. Откройте команду заново.", ephemeral=True)
            return
        if not view.state.target:
            await interaction.response.send_message("Сначала выберите нарушителя.", ephemeral=True)
            return
        view.state.show_rules_menu = True
        view.state.manual_action = None
        view.state.manual_duration_minutes = None
        view.state.manual_reason_text = ""
        view.state.status_text = "Открыт список нарушений из правил. Выберите нарушение из выпадающего списка."
        view.rebuild()
        await interaction.response.edit_message(embed=view.build_embed(), view=view)


class DiscordRepFlowView(discord.ui.View):
    def __init__(self, *, actor_id: int, guild_id: int | None, chat_id: int | None):
        super().__init__(timeout=300)
        self.state = DiscordRepFlowState(actor_id=actor_id, guild_id=guild_id, chat_id=chat_id)
        self._violations = ModerationService.list_active_violation_types()
        self._unavailable_violations: list[dict[str, Any]] = []
        self.message: discord.Message | None = None
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
        self.state.show_rules_menu = False
        self.state.status_text = status_text
        try:
            availability = ModerationService.list_available_violation_types(
                provider="discord",
                actor={"provider": "discord", "provider_user_id": str(self.state.actor_id), "label": str(self.state.actor_id)},
                target=self.state.target,
                chat_id=self.state.chat_id,
            )
            self._violations = list(availability.get("available") or [])
            self._unavailable_violations = list(availability.get("unavailable") or [])
            self.state.hidden_violations_count = len(self._unavailable_violations)
        except Exception:
            logger.exception("rep violation availability failed provider=%s actor=%s target=%s", "discord", self.state.actor_id, (self.state.target or {}).get("provider_user_id"))
            self._violations = ModerationService.list_active_violation_types()
            self._unavailable_violations = []
            self.state.hidden_violations_count = 0
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
            return {"ok": False, "message": render_rep_preview_failed_text()}
        if not preview.get("ok"):
            self.log_event(
                "warning",
                message="rep authority deny",
                error_code=str(preview.get("error_code") or "preview_failed"),
                selected_actions=list(preview.get("selected_actions") or []),
            )
            return {
                "ok": False,
                "message": render_rep_authority_deny_text(str(preview.get("message") or "")),
            }
        return preview

    def rebuild(self) -> None:
        self.clear_items()
        self.add_item(_RepTargetSelect())
        self.add_item(_RepViolationSelect(self._violations, disabled=(self.state.target is None or not self.state.show_rules_menu)))
        self.add_item(_RepBackButton(disabled=not (self.state.target or self.state.preview)))
        can_confirm_manual = bool(self.state.manual_action and self.state.manual_duration_minutes and self.state.manual_reason_text)
        self.add_item(_RepConfirmButton(disabled=not (self.state.preview is not None or can_confirm_manual)))
        self.add_item(_RepCancelButton())
        actor_id = str(self.state.actor_id)
        self.add_item(_RepManualActionButton(action_key="mute", title="Мут", disabled=not AuthorityService.has_command_permission("discord", actor_id, "moderation_mute")))
        self.add_item(_RepManualActionButton(action_key="warn", title="Пред", disabled=not AuthorityService.has_command_permission("discord", actor_id, "moderation_warn")))
        self.add_item(_RepManualActionButton(action_key="ban", title="Бан", disabled=not AuthorityService.has_command_permission("discord", actor_id, "moderation_ban")))
        self.add_item(_RepManualActionButton(action_key="kick", title="Кик", disabled=not AuthorityService.has_command_permission("discord", actor_id, "moderation_mute")))
        self.add_item(_RepRulesMenuButton(disabled=self.state.target is None))
        manual_selected = self.state.manual_action is not None
        self.add_item(_RepManualPresetDurationButton(title="15м", minutes=15, disabled=not manual_selected))
        self.add_item(_RepManualPresetDurationButton(title="1ч", minutes=60, disabled=not manual_selected))
        self.add_item(_RepManualPresetDurationButton(title="12ч", minutes=720, disabled=not manual_selected))
        self.add_item(_RepManualPresetDurationButton(title="1д", minutes=1440, disabled=not manual_selected))
        self.add_item(_RepManualCustomDurationButton(disabled=not manual_selected))
        self.add_item(_RepEscalationRequestButton(disabled=not (self.state.target and self.state.hidden_violations_count > 0)))

    def disable_all_items(self) -> None:
        for child in self.children:
            child.disabled = True

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.state.actor_id:
            self.log_event("warning", message="rep interaction denied", error_code="foreign_actor")
            await interaction.response.send_message(render_rep_foreign_actor_text(), ephemeral=True)
            return False
        return True

    def build_embed(self) -> discord.Embed:
        if self.state.result:
            ui_payload = self.state.result["ui_payload"]
            embed = discord.Embed(title="✅ /rep завершён", description=render_rep_result_text(ui_payload), color=discord.Color.green())
            embed.add_field(
                name="Состояние сессии",
                value=render_rep_session_status_text(
                    current_step=5,
                    status_text=self.state.status_text or "Кейс создан, summary показан, дополнительных действий в этой сессии больше не требуется.",
                ),
                inline=False,
            )
            return embed
        if self.state.preview:
            ui_payload = self.state.preview["ui_payload"]
            embed = discord.Embed(title="🧾 Предпросмотр /rep", description=render_rep_preview_text(ui_payload), color=discord.Color.orange())
            embed.add_field(
                name="Состояние сессии",
                value=render_rep_session_status_text(
                    current_step=3,
                    status_text=self.state.status_text or "Шаг 3/5: проверьте авторасчёт, затем подтвердите или вернитесь назад.",
                ),
                inline=False,
            )
            return embed
        if self.state.manual_action:
            embed = discord.Embed(title="🧾 Предпросмотр ручного наказания", color=discord.Color.orange())
            embed.description = (
                f"👤 Цель: {_target_label(self.state.target)}\n"
                f"⚙️ Действие: {self.state.manual_action}\n"
                f"⏱️ Срок: {self.state.manual_duration_minutes} мин\n"
                f"📝 Причина: {self.state.manual_reason_text}\n\n"
                "Проверьте данные и нажмите «Подтвердить»."
            )
            return embed
        embed = discord.Embed(title="🛡️ /rep", color=discord.Color.blurple())
        embed.description = render_rep_start_text(target_selection_hint=self.state.target_hint)
        embed.add_field(name="Текущая цель", value=_target_label(self.state.target), inline=False)
        current_step = 2 if self.state.target else 1
        embed.add_field(
            name="Состояние сессии",
            value=render_rep_session_status_text(
                current_step=current_step,
                status_text=self.state.status_text or ("Шаг 2/5: выберите нарушение." if self.state.target else "Шаг 1/5: выберите нарушителя."),
            ),
            inline=False,
        )
        embed.add_field(
            name="Шаг 1",
            value=render_rep_target_prompt_text(
                target_selection_hint=self.state.target_hint,
                target_label=_target_label(self.state.target) if self.state.target else None,
            ),
            inline=False,
        )
        if self.state.target:
            embed.add_field(name="Шаг 2", value=render_rep_violation_prompt_text(target_label=_target_label(self.state.target)), inline=False)
            embed.add_field(
                name="Порядок кнопок",
                value="Сначала выберите 1 из 4 ручных действий (Мут/Пред/Бан/Кик) или 5-ю кнопку «Нарушения из правил».",
                inline=False,
            )
            if self.state.hidden_violations_count > 0:
                embed.add_field(
                    name="Доступ по полномочиям",
                    value=f"Скрыто нарушений: {self.state.hidden_violations_count}. Если нужно эскалировать недоступное нарушение — обратитесь к старшему администратору.",
                    inline=False,
                )
        if self.state.status_text:
            embed.add_field(name="Короткий статус", value=self.state.status_text, inline=False)
        return embed

    async def notify_target(self, ui_payload: dict[str, Any]) -> None:
        target_account_id = str(ui_payload.get("target_account_id") or "") or None
        selected_actions = set(ui_payload.get("selected_actions") or [])
        text = render_violator_notification_text(ui_payload)
        try:
            if "mute" in selected_actions:
                await ModerationNotificationsService.dispatch_notification(
                    runtime_bot=getattr(self, "bot", None),
                    provider="discord",
                    target_account_id=target_account_id,
                    event_type=ModerationNotificationsService.EVENT_MUTE_STARTED,
                    message_text=text,
                    case_id=ui_payload.get("case_id"),
                    source_chat_id=self.state.chat_id,
                    requires_chat_delivery=True,
                    allow_dm_delivery=True,
                )
            if "fine_points" in selected_actions:
                await ModerationNotificationsService.dispatch_notification(
                    runtime_bot=getattr(self, "bot", None),
                    provider="discord",
                    target_account_id=target_account_id,
                    event_type=ModerationNotificationsService.EVENT_FINE_CREATED,
                    message_text=text,
                    case_id=ui_payload.get("case_id"),
                    source_chat_id=self.state.chat_id,
                    requires_chat_delivery=True,
                    allow_dm_delivery=True,
                )
            if not selected_actions.intersection({"mute", "fine_points"}):
                member = (self.state.target or {}).get("member")
                if member:
                    await safe_send(member, text)
        except Exception:
            self.log_event("exception", message="rep target notify failed", error_code="target_notify_failed")

    async def on_timeout(self) -> None:
        self.disable_all_items()
        self.state.status_text = render_rep_expired_text()
        self.log_event("warning", message="rep interaction expired", error_code="session_expired")
        message = getattr(self, "message", None)
        if not message:
            return
        try:
            embed = discord.Embed(title="⌛ /rep истёк", description=render_rep_expired_text(), color=discord.Color.dark_grey())
            await message.edit(embed=embed, view=self)
        except Exception:
            self.log_event("exception", message="rep timeout edit failed", error_code="timeout_edit_failed")


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
        logger.warning(
            "rep authority deny provider=%s chat_id=%s actor=%s actor_account_id=%s target=%s target_account_id=%s violation_code=%s selected_actions=%s case_id=%s error_code=%s",
            "discord",
            ctx.channel.id if ctx.channel else (ctx.guild.id if ctx.guild else None),
            ctx.author.id,
            None,
            None,
            None,
            None,
            [],
            None,
            "authority_denied",
        )
        await send_temp(ctx, f"❌ {render_rep_authority_deny_text('Команда /rep доступна только ролям модерации. Если доступ должен быть — проверьте authority и попробуйте ещё раз.')}")
        return
    view = DiscordRepFlowView(
        actor_id=ctx.author.id,
        guild_id=ctx.guild.id if ctx.guild else None,
        chat_id=ctx.channel.id if ctx.channel else (ctx.guild.id if ctx.guild else None),
    )
    view.bot = ctx.bot
    prefilled_target = await _prefill_target_from_context(ctx, target)
    if target and not prefilled_target:
        logger.warning(
            "rep target resolve failed provider=%s chat_id=%s actor=%s actor_account_id=%s target=%s target_account_id=%s violation_code=%s selected_actions=%s case_id=%s error_code=%s",
            "discord",
            view.state.chat_id,
            ctx.author.id,
            None,
            target,
            None,
            None,
            [],
            None,
            "target_not_found",
        )
        await send_temp(ctx, f"❌ {render_rep_target_not_found_text(target_selection_hint=view.state.target_hint)}")
        return
    if prefilled_target:
        matched_by = str(prefilled_target.get("matched_by") or "lookup")
        target_hint = (
            "Нарушитель подставлен из reply-контекста. Это самый быстрый способ; при необходимости можно выбрать другого пользователя в панели ниже."
            if matched_by == "reply"
            else "Нарушитель подставлен из lookup. Проверьте цель и при необходимости выберите другого пользователя в панели ниже."
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
    sent_message = await send_temp(ctx, embed=view.build_embed(), view=view, delete_after=None)
    if sent_message is not None:
        view.message = sent_message
