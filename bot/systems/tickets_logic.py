import discord
from bot.data import db

TICKET_EMOJI = {
    "normal": "🎟 Обычный билет",
    "gold": "🪙 Золотой билет"
}

def is_valid_ticket_type(ticket_type: str) -> bool:
    return ticket_type in ("normal", "gold")

async def give_ticket_logic(user_id: int, ticket_type: str, amount: int, reason: str, author_id: int) -> discord.Embed:
    if not is_valid_ticket_type(ticket_type):
        return discord.Embed(title="❌ Неверный тип билета", description="Допустимые: `normal`, `gold`", color=discord.Color.red())

    success = db.give_ticket(user_id, ticket_type, amount, reason, author_id)
    if not success:
        return discord.Embed(title="❌ Ошибка при начислении", color=discord.Color.red())

    return build_ticket_embed(user_id, ticket_type, amount, reason, author_id, added=True)

async def remove_ticket_logic(user_id: int, ticket_type: str, amount: int, reason: str, author_id: int) -> discord.Embed:
    if not is_valid_ticket_type(ticket_type):
        return discord.Embed(title="❌ Неверный тип билета", description="Допустимые: `normal`, `gold`", color=discord.Color.red())

    success = db.remove_ticket(user_id, ticket_type, amount, reason, author_id)
    if not success:
        return discord.Embed(title="❌ Ошибка при списании", color=discord.Color.red())

    return build_ticket_embed(user_id, ticket_type, amount, reason, author_id, added=False)

def build_ticket_embed(user_id: int, ticket_type: str, amount: int, reason: str, author_id: int, added: bool) -> discord.Embed:
    action = "Начислено" if added else "Списано"
    emoji = TICKET_EMOJI.get(ticket_type, "🎫 Билет")
    embed = discord.Embed(
        title=f"{emoji} — {action}",
        color=discord.Color.green() if added else discord.Color.orange()
    )
    embed.add_field(name="👤 Пользователь", value=f"<@{user_id}>", inline=True)
    embed.add_field(name="📌 Количество", value=f"{amount}", inline=True)
    embed.add_field(name="📝 Причина", value=reason, inline=False)
    embed.set_footer(text=f"Автор действия: <@{author_id}>")
    return embed