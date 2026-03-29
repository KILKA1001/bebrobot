from __future__ import annotations

import logging
from typing import Any

import discord
from discord.ext import commands

from bot.commands.base import bot
from bot.commands.fines import send_legacy_fines_for_discord_destination
from bot.commands.roles_admin import _resolve_discord_target
from bot.services import AccountsService, AuthorityService, ModerationService
from bot.utils import send_temp

logger = logging.getLogger(__name__)
_PAYMENT_HINT = ModerationService.MODSTATUS_PAYMENT_HINT


class _ModstatusFineView(discord.ui.View):
    def __init__(self, *, actor_id: int):
        super().__init__(timeout=180)
        self.actor_id = actor_id

    @discord.ui.button(label="💳 Оплатить legacy-штраф", style=discord.ButtonStyle.green)
    async def open_fines(self, interaction: discord.Interaction, _button: discord.ui.Button) -> None:
        if interaction.user.id != self.actor_id:
            await interaction.response.send_message("❌ Эта кнопка открыта для другого пользователя.", ephemeral=True)
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
            await interaction.followup.send("✅ У вас нет активных legacy-штрафов.", ephemeral=True)


class _ModstatusManagePunishmentView(discord.ui.View):
    def __init__(self, *, actor_id: int, target_subject: dict[str, Any], chat_id: int | None):
        super().__init__(timeout=180)
        self.actor_id = actor_id
        self.target_subject = dict(target_subject)
        self.chat_id = chat_id

    @discord.ui.button(label="🧹 Убрать наказание", style=discord.ButtonStyle.danger)
    async def rollback_case(self, interaction: discord.Interaction, _button: discord.ui.Button) -> None:
        if interaction.user.id != self.actor_id:
            await interaction.response.send_message("❌ Эта кнопка открыта для другого пользователя.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        try:
            result = ModerationService.rollback_latest_case(
                "discord",
                {"provider": "discord", "provider_user_id": str(interaction.user.id), "label": interaction.user.mention},
                self.target_subject,
                chat_id=self.chat_id,
            )
        except Exception:
            logger.exception("modstatus rollback failed provider=%s actor_id=%s target=%s", "discord", interaction.user.id, self.target_subject.get("provider_user_id"))
            await interaction.followup.send("❌ Не удалось снять наказание. Подробности в консоли.", ephemeral=True)
            return
        if not result.get("ok"):
            await interaction.followup.send(f"❌ {result.get('message') or 'Не удалось снять наказание.'}", ephemeral=True)
            return
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
                    source_chat_id=self.chat_id,
                    requires_chat_delivery=False,
                    allow_dm_delivery=True,
                )
            except Exception:
                logger.exception("modstatus rollback notify failed provider=%s case_id=%s", "discord", result.get("case_id"))


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
        await send_temp(ctx, "❌ Сначала привяжите общий аккаунт, затем повторите `/modstatus`.")
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

        view = None
        if snapshot.get("target_is_self") and list(snapshot.get("active_fines") or []):
            view = _ModstatusFineView(actor_id=ctx.author.id)
        elif target_subject and AuthorityService.has_command_permission("discord", viewer_id, "moderation_mute"):
            view = _ModstatusManagePunishmentView(actor_id=ctx.author.id, target_subject=target_subject, chat_id=chat_id)
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
