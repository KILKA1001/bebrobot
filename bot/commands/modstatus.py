"""
Назначение: модуль "modstatus" реализует продуктовый контур в зоне Discord.
Ответственность: единая точка для сценариев и правил модуля без дублирования логики между платформами.
Где используется: Discord.
Пользовательский вход: команда /modstatus и связанный пользовательский сценарий.
"""

from __future__ import annotations

import logging
from typing import Any

import discord
from discord.ext import commands

from bot.commands.base import bot
from bot.commands.fines import send_legacy_fines_for_discord_destination
from bot.commands.roles_admin import _resolve_discord_target
from bot.services import AccountsService, AuthorityService, ModerationNotificationsService, ModerationService
from bot.utils import send_temp

logger = logging.getLogger(__name__)
_PAYMENT_HINT = ModerationService.MODSTATUS_PAYMENT_HINT


def _snapshot_has_payable_manual_fines(snapshot: dict[str, Any]) -> bool:
    for fine in list(snapshot.get("active_fines") or []):
        kind = str(fine.get("kind") or "").strip().lower()
        if kind == "legacy_fine":
            return True
        if kind != "case_fine":
            continue
        payment_mode = str(
            fine.get("payment_mode") or ModerationService.FINE_PAYMENT_MODE_MANUAL
        ).strip().lower()
        if payment_mode != ModerationService.FINE_PAYMENT_MODE_INSTANT:
            return True
    return False


async def _rollback_discord_runtime_sanctions(
    *,
    interaction: discord.Interaction,
    target_subject: dict[str, Any],
    rollback_result: dict[str, Any],
) -> None:
    guild = interaction.guild
    if guild is None:
        logger.error(
            "modstatus rollback runtime skipped provider=%s reason=%s actor_id=%s target_id=%s case_id=%s",
            "discord",
            "guild_missing",
            interaction.user.id,
            target_subject.get("provider_user_id"),
            rollback_result.get("case_id"),
        )
        return

    target_user_id = int(str(target_subject.get("provider_user_id") or "0") or 0)
    if not target_user_id:
        logger.error(
            "modstatus rollback runtime skipped provider=%s reason=%s actor_id=%s target_id=%s case_id=%s",
            "discord",
            "target_not_found",
            interaction.user.id,
            target_subject.get("provider_user_id"),
            rollback_result.get("case_id"),
        )
        return

    if rollback_result.get("had_mute"):
        try:
            member = guild.get_member(target_user_id) or await guild.fetch_member(target_user_id)
        except Exception:
            logger.exception(
                "modstatus rollback runtime fetch_member failed provider=%s actor_id=%s target_id=%s guild_id=%s case_id=%s",
                "discord",
                interaction.user.id,
                target_user_id,
                guild.id,
                rollback_result.get("case_id"),
            )
            member = None
        if member is not None:
            try:
                await member.edit(timeout=None, reason=f"/modstatus rollback by {interaction.user.id}")
                logger.info(
                    "modstatus rollback runtime mute removed provider=%s actor_id=%s target_id=%s guild_id=%s case_id=%s",
                    "discord",
                    interaction.user.id,
                    target_user_id,
                    guild.id,
                    rollback_result.get("case_id"),
                )
            except Exception:
                logger.exception(
                    "modstatus rollback runtime unmute failed provider=%s actor_id=%s target_id=%s guild_id=%s case_id=%s",
                    "discord",
                    interaction.user.id,
                    target_user_id,
                    guild.id,
                    rollback_result.get("case_id"),
                )

    if rollback_result.get("had_ban_or_kick"):
        try:
            banned = await guild.fetch_ban(discord.Object(id=target_user_id))
            await guild.unban(banned.user, reason=f"/modstatus rollback by {interaction.user.id}")
            logger.info(
                "modstatus rollback runtime unban success provider=%s actor_id=%s target_id=%s guild_id=%s case_id=%s",
                "discord",
                interaction.user.id,
                target_user_id,
                guild.id,
                rollback_result.get("case_id"),
            )
        except discord.NotFound:
            logger.info(
                "modstatus rollback runtime unban skipped provider=%s actor_id=%s target_id=%s guild_id=%s case_id=%s reason=%s",
                "discord",
                interaction.user.id,
                target_user_id,
                guild.id,
                rollback_result.get("case_id"),
                "user_not_banned",
            )
        except Exception:
            logger.exception(
                "modstatus rollback runtime unban failed provider=%s actor_id=%s target_id=%s guild_id=%s case_id=%s",
                "discord",
                interaction.user.id,
                target_user_id,
                guild.id,
                rollback_result.get("case_id"),
            )


class _ModstatusActionView(discord.ui.View):
    def __init__(
        self,
        *,
        actor_id: int,
        actor_id_text: str,
        chat_id: int | None,
        can_open_payment: bool,
        can_rollback: bool,
        target_subject: dict[str, Any] | None = None,
        rollback_candidates: list[dict[str, Any]] | None = None,
    ):
        super().__init__(timeout=180)
        self.actor_id = actor_id
        self.actor_id_text = actor_id_text
        self.chat_id = chat_id
        self.can_open_payment = can_open_payment
        self.can_rollback = can_rollback
        self.target_subject = dict(target_subject or {})
        self.rollback_candidates = list(rollback_candidates or [])
        self.selected_case_id: str | None = None
        if self.can_open_payment:
            self.add_item(_OpenPaymentButton())
        if self.can_rollback:
            self.add_item(_RollbackPunishmentButton())
            self.add_item(_RollbackCaseSelect(candidates=self.rollback_candidates))


class _OpenPaymentButton(discord.ui.Button):
    def __init__(self) -> None:
        super().__init__(
            label="💳 Оплатить штраф",
            style=discord.ButtonStyle.green,
            row=0,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if not isinstance(view, _ModstatusActionView):
            await interaction.response.send_message("❌ Ошибка кнопки оплаты.", ephemeral=True)
            return
        if interaction.user.id != view.actor_id:
            await interaction.response.send_message("❌ Эта кнопка открыта для другого пользователя.", ephemeral=True)
            return
        if not view.can_open_payment:
            logger.warning("modstatus payment callback rejected provider=%s actor_id=%s reason=%s", "discord", interaction.user.id, "button_not_allowed")
            await interaction.response.send_message("❌ Оплата сейчас недоступна для этого статуса.", ephemeral=True)
            return
        snapshot = ModerationService.get_user_moderation_snapshot(
            view.actor_id_text,
            view.actor_id_text,
            "discord",
            view.chat_id,
            {
                "viewer_id": view.actor_id_text,
                "target_id": view.actor_id_text,
                "selected_via_reply": False,
                "explicit_target": False,
                "allow_lookup_others": False,
                "is_private": interaction.guild is None,
            },
        )
        if (not snapshot.get("ok")) or (not snapshot.get("target_is_self")) or (not _snapshot_has_payable_manual_fines(snapshot)):
            logger.warning(
                "modstatus payment callback denied provider=%s actor_id=%s reason=%s snapshot_ok=%s target_is_self=%s",
                "discord",
                interaction.user.id,
                "snapshot_not_payable",
                snapshot.get("ok"),
                snapshot.get("target_is_self"),
            )
            await interaction.response.send_message("❌ Нет доступных штрафов для ручной оплаты.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        try:
            sent = await send_legacy_fines_for_discord_destination(
                user_id=interaction.user.id,
                send_embed=lambda **kwargs: interaction.followup.send(ephemeral=True, **kwargs),
            )
        except Exception:
            logger.exception("modstatus legacy fines open failed actor_id=%s", interaction.user.id)
            await interaction.followup.send("❌ Не удалось открыть список штрафов. Подробности в консоли.", ephemeral=True)
            return
        if not sent:
            await interaction.followup.send("✅ У вас нет активных штрафов для оплаты.", ephemeral=True)

class _RollbackPunishmentButton(discord.ui.Button):
    def __init__(self) -> None:
        super().__init__(
            label="🧹 Убрать наказание",
            style=discord.ButtonStyle.danger,
            row=1,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if not isinstance(view, _ModstatusActionView):
            await interaction.response.send_message("❌ Ошибка кнопки отката.", ephemeral=True)
            return
        if interaction.user.id != view.actor_id:
            await interaction.response.send_message("❌ Эта кнопка открыта для другого пользователя.", ephemeral=True)
            return
        if not view.can_rollback:
            logger.warning("modstatus rollback callback rejected provider=%s actor_id=%s reason=%s", "discord", interaction.user.id, "button_not_allowed")
            await interaction.response.send_message("❌ Снятие наказания сейчас недоступно.", ephemeral=True)
            return
        if not AuthorityService.has_command_permission("discord", str(interaction.user.id), "moderation_mute"):
            logger.warning("modstatus rollback callback denied provider=%s actor_id=%s reason=%s", "discord", interaction.user.id, "no_permission")
            await interaction.response.send_message("❌ Недостаточно прав для снятия наказаний.", ephemeral=True)
            return
        target_provider_id = str((view.target_subject or {}).get("provider_user_id") or "").strip()
        if not target_provider_id or target_provider_id.lower() in {"none", "null"}:
            logger.warning("modstatus rollback callback denied provider=%s actor_id=%s reason=%s", "discord", interaction.user.id, "target_missing")
            await interaction.response.send_message("❌ Не удалось определить цель для отката.", ephemeral=True)
            return
        if view.selected_case_id and view.selected_case_id not in {str((item.get("case") or {}).get("id") or "").strip() for item in view.rollback_candidates}:
            logger.warning("modstatus rollback callback denied provider=%s actor_id=%s reason=%s case_id=%s", "discord", interaction.user.id, "invalid_case_selection", view.selected_case_id)
            await interaction.response.send_message("❌ Выбранный кейс недоступен для отката.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        try:
            result = ModerationService.rollback_latest_case(
                "discord",
                {"provider": "discord", "provider_user_id": str(interaction.user.id), "label": interaction.user.mention},
                view.target_subject,
                chat_id=view.chat_id,
                case_id=view.selected_case_id,
            )
        except Exception:
            logger.exception("modstatus rollback failed provider=%s actor_id=%s target=%s", "discord", interaction.user.id, view.target_subject.get("provider_user_id"))
            await interaction.followup.send("❌ Не удалось снять наказание. Подробности в консоли.", ephemeral=True)
            return
        if not result.get("ok"):
            await interaction.followup.send(f"❌ {result.get('message') or 'Не удалось снять наказание.'}", ephemeral=True)
            return
        await _rollback_discord_runtime_sanctions(
            interaction=interaction,
            target_subject=view.target_subject,
            rollback_result=result,
        )
        await interaction.followup.send(f"✅ {result.get('message') or 'Наказание снято.'}", ephemeral=True)
        if result.get("had_ban_or_kick"):
            link = (interaction.guild and interaction.guild.vanity_url) or ""
            text = (
                "ℹ️ Предыдущее наказание (бан/кик) было снято как ошибочное. "
                + (f"Можно снова зайти в чат: {link}" if link else "Можно снова зайти в чат, запросите ссылку у администрации.")
            )
            try:
                await ModerationNotificationsService.dispatch_notification(
                    runtime_bot=interaction.client,
                    provider="discord",
                    target_account_id=(result.get("target") or {}).get("account_id"),
                    event_type="punishment_revoked",
                    message_text=text,
                    case_id=result.get("case_id"),
                    source_chat_id=view.chat_id,
                    requires_chat_delivery=False,
                    allow_dm_delivery=True,
                )
            except Exception:
                logger.exception("modstatus rollback notify failed provider=%s case_id=%s", "discord", result.get("case_id"))


class _RollbackCaseSelect(discord.ui.Select):
    def __init__(self, *, candidates: list[dict[str, Any]]):
        options: list[discord.SelectOption] = []
        for item in candidates[:25]:
            case_row = dict(item.get("case") or {})
            case_id = str(case_row.get("id") or "").strip()
            if not case_id:
                continue
            actions = ", ".join(str(row.get("action_type") or "") for row in list(item.get("actions") or [])[:3] if str(row.get("action_type") or "").strip()) or "без действий"
            options.append(discord.SelectOption(label=f"Кейс #{case_id}", value=case_id, description=actions[:100]))
        if not options:
            options = [discord.SelectOption(label="Нет кейсов для отката", value="__none__")]
        super().__init__(
            placeholder="Выберите кейс для снятия наказания",
            min_values=1,
            max_values=1,
            options=options,
            row=2,
            disabled=options[0].value == "__none__",
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if not isinstance(view, _ModstatusActionView):
            await interaction.response.send_message("❌ Ошибка выбора кейса.", ephemeral=True)
            return
        selected = str(self.values[0] or "").strip()
        if selected == "__none__":
            await interaction.response.send_message("Нет кейсов для отката.", ephemeral=True)
            return
        view.selected_case_id = selected
        await interaction.response.send_message(f"✅ Выбран кейс #{selected}. Теперь нажмите «Убрать наказание».", ephemeral=True)


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
            "modstatus reply lookup failed provider=%s chat_id=%s viewer_id=%s target_id=%s account_id=%s",
            "discord",
            getattr(ctx.channel, "id", None),
            getattr(ctx.author, "id", None),
            getattr(reference, "message_id", None),
            None,
        )
        return None


@bot.hybrid_command(name="modstatus", description="Показать свои активные наказания, кейсы и штрафы")
async def modstatus(ctx: commands.Context, *, target: str | None = None) -> None:
    chat_id = ctx.channel.id if ctx.channel else (ctx.guild.id if ctx.guild else None)
    viewer_id = str(ctx.author.id)
    logger.info(
        "ux_screen_open event=ux_screen_open screen=modstatus provider=discord actor_user_id=%s chat_id=%s",
        viewer_id,
        chat_id,
    )
    viewer_account_id = AccountsService.resolve_account_id("discord", viewer_id)
    if not viewer_account_id:
        logger.warning(
            "modstatus viewer unresolved provider=%s chat_id=%s viewer_id=%s target_id=%s account_id=%s",
            "discord",
            chat_id,
            viewer_id,
            None,
            None,
        )
        await send_temp(
            ctx,
            "❌ Общий профиль пока не найден.\n"
            "Что делать сейчас: откройте /register_account или /link в личном чате с ботом.\n"
            "Что будет дальше: после привязки откроется экран /modstatus.",
        )
        return

    target_subject: dict[str, Any] | None = None
    selected_via_reply = False
    explicit_target = False
    try:
        reply_message = await _resolve_reply_message(ctx)
        if reply_message and getattr(reply_message, "author", None) and not getattr(reply_message.author, "bot", False):
            reply_author = reply_message.author
            target_subject = {
                "provider": "discord",
                "provider_user_id": str(reply_author.id),
                "account_id": AccountsService.resolve_account_id("discord", str(reply_author.id)),
                "label": getattr(reply_author, "mention", None) or getattr(reply_author, "display_name", None) or str(reply_author.id),
                "matched_by": "reply",
            }
            selected_via_reply = True
            explicit_target = True
        elif target:
            explicit_target = True
            target_subject = await _resolve_discord_target(ctx, target, operation="modstatus")
            if target_subject is None:
                logger.warning(
                    "modstatus target resolve failed provider=%s chat_id=%s viewer_id=%s target_id=%s account_id=%s",
                    "discord",
                    chat_id,
                    viewer_id,
                    target,
                    None,
                )
                return

        target_account_id = str((target_subject or {}).get("account_id") or "").strip() or str(viewer_account_id)
        snapshot = ModerationService.get_user_moderation_snapshot(
            target_account_id,
            str(viewer_account_id),
            "discord",
            chat_id,
            {
                "viewer_id": viewer_id,
                "target_id": (target_subject or {}).get("provider_user_id") or viewer_id,
                "selected_via_reply": selected_via_reply,
                "explicit_target": explicit_target,
                "allow_lookup_others": AuthorityService.has_command_permission("discord", viewer_id, "moderation_view_cases"),
                "is_private": ctx.guild is None,
            },
        )
        if not snapshot.get("ok"):
            logger.warning(
                "modstatus snapshot denied provider=%s chat_id=%s viewer_id=%s target_id=%s account_id=%s error_code=%s",
                "discord",
                chat_id,
                viewer_id,
                (target_subject or {}).get("provider_user_id") or viewer_id,
                target_account_id,
                snapshot.get("error_code"),
            )
            await send_temp(ctx, f"❌ {snapshot.get('message') or 'Не удалось загрузить модерационный статус.'}")
            return

        can_open_payment = bool(snapshot.get("target_is_self")) and _snapshot_has_payable_manual_fines(snapshot)
        can_rollback = False
        rollback_candidates: list[dict[str, Any]] = []
        if target_subject and AuthorityService.has_command_permission("discord", viewer_id, "moderation_mute"):
            candidates = [
                item
                for item in list(ModerationService.list_recent_cases(target_account_id, limit=10).get("items") or [])
                if str((item.get("case") or {}).get("status") or "").strip().lower() == ModerationService.STATUS_APPLIED
            ]
            if candidates:
                can_rollback = True
                rollback_candidates = candidates

        view = None
        if can_open_payment or can_rollback:
            view = _ModstatusActionView(
                actor_id=ctx.author.id,
                actor_id_text=viewer_id,
                chat_id=chat_id,
                can_open_payment=can_open_payment,
                can_rollback=can_rollback,
                target_subject=target_subject,
                rollback_candidates=rollback_candidates,
            )
        await send_temp(
            ctx,
            ModerationService.render_user_moderation_snapshot(snapshot, payment_hint=_PAYMENT_HINT),
            view=view,
            delete_after=None,
        )
    except Exception:
        logger.exception(
            "modstatus command failed provider=%s chat_id=%s viewer_id=%s target_id=%s account_id=%s",
            "discord",
            chat_id,
            viewer_id,
            (target_subject or {}).get("provider_user_id") if target_subject else target,
            (target_subject or {}).get("account_id") if target_subject else viewer_account_id,
        )
        await send_temp(ctx, "❌ Не удалось загрузить модерационный статус. Подробности записаны в консоль.")
