from __future__ import annotations

import logging
from dataclasses import dataclass

import discord
from discord.ext import commands

from bot.commands.base import bot
from bot.commands.roles_admin import _resolve_discord_target
from bot.services.title_management_service import TitleManagementService
from bot.utils import send_temp

logger = logging.getLogger(__name__)


@dataclass
class DiscordTitleFlowState:
    actor_id: int
    target: dict[str, str]
    mode: str = "promote"


class _TitleModeButtons(discord.ui.View):
    def __init__(self, *, state: DiscordTitleFlowState):
        super().__init__(timeout=180)
        self.state = state
        self.add_item(_TitleSelect(state=state))

    @discord.ui.button(label="⬆️ Повысить", style=discord.ButtonStyle.success)
    async def mode_promote(self, interaction: discord.Interaction, _button: discord.ui.Button) -> None:
        if interaction.user.id != self.state.actor_id:
            await interaction.response.send_message("❌ Эти кнопки открыты для другого администратора.", ephemeral=True)
            return
        self.state.mode = "promote"
        await interaction.response.send_message("✅ Режим: повышение. Теперь выбери звание из списка ниже.", ephemeral=True)

    @discord.ui.button(label="⬇️ Понизить", style=discord.ButtonStyle.danger)
    async def mode_demote(self, interaction: discord.Interaction, _button: discord.ui.Button) -> None:
        if interaction.user.id != self.state.actor_id:
            await interaction.response.send_message("❌ Эти кнопки открыты для другого администратора.", ephemeral=True)
            return
        self.state.mode = "demote"
        await interaction.response.send_message("✅ Режим: понижение. Теперь выбери звание из списка ниже.", ephemeral=True)


class _TitleSelect(discord.ui.Select):
    def __init__(self, *, state: DiscordTitleFlowState):
        self.state = state
        options = [
            discord.SelectOption(label=label[:100], value=key, description="Выбор звания для изменения")
            for key, label in TitleManagementService.managed_titles()[:25]
        ]
        super().__init__(
            placeholder="Выберите звание",
            min_values=1,
            max_values=1,
            options=options,
            row=1,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        if interaction.user.id != self.state.actor_id:
            await interaction.response.send_message("❌ Эти кнопки открыты для другого администратора.", ephemeral=True)
            return
        selected = str(self.values[0] or "").strip()
        try:
            result = TitleManagementService.apply_title_change(
                actor_provider="discord",
                actor_user_id=str(interaction.user.id),
                target_provider=str(self.state.target.get("provider") or "discord"),
                target_user_id=str(self.state.target.get("provider_user_id") or ""),
                title_key=selected,
                mode=self.state.mode,
                source="discord_title_command",
            )
        except Exception:
            logger.exception(
                "title command failed provider=%s actor_id=%s target_id=%s mode=%s title=%s",
                "discord",
                interaction.user.id,
                self.state.target.get("provider_user_id"),
                self.state.mode,
                selected,
            )
            await interaction.response.send_message("❌ Не удалось изменить звание. Подробности в консоли.", ephemeral=True)
            return

        target_label = str(self.state.target.get("label") or self.state.target.get("provider_user_id") or "пользователь")
        await interaction.response.send_message(
            f"{result.message}\nПользователь: {target_label}\nТекущие звания: {', '.join(result.titles) if result.titles else 'нет'}",
            ephemeral=True,
        )


@bot.hybrid_command(name="title", description="Повысить или понизить звание пользователя (только суперадмины)")
async def title(ctx: commands.Context, *, target: str | None = None) -> None:
    if not target:
        await send_temp(
            ctx,
            "❌ Укажи пользователя: /title <mention|username>.\n"
            "После запуска откроется кнопочная панель: сначала выбери режим (повысить/понизить), затем нужное звание из списка.",
        )
        return

    if not TitleManagementService.is_super_admin("discord", str(ctx.author.id)):
        await send_temp(ctx, "❌ Повышать или понижать звания могут только суперадмины.")
        return

    resolved = await _resolve_discord_target(ctx, target, operation="title")
    if resolved is None:
        return

    state = DiscordTitleFlowState(actor_id=ctx.author.id, target=resolved)
    view = _TitleModeButtons(state=state)
    await send_temp(
        ctx,
        "🛠️ Управление званием\n"
        "1) Выбери режим: повышение или понижение.\n"
        "2) Выбери звание из списка.\n"
        "Команда /title объединяет оба сценария в одном интерфейсе.",
        view=view,
    )
