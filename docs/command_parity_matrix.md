# Command parity matrix (Discord ↔ Telegram)

This matrix reflects the current command parity between Discord and Telegram implementations, based on:

- `bot/commands/base.py`
- `bot/commands/fines.py`
- `bot/commands/tournament.py`
- `bot/commands/linking.py`
- `bot/telegram_bot/main.py`
- `bot/telegram_bot/commands/linking.py`

> **Definition of done rule:** Any new user-facing command in Discord or Telegram is considered complete only if this matrix is updated in the same change. Help, onboarding, fallback тексты и сценарии первого запуска тоже входят в definition of done, если они меняют пользовательский путь команды.

| Domain | Discord command | Telegram command | Parity level | Notes |
|---|---|---|---|---|
| Account linking | `/register_account` | `/register` | full | Equivalent account registration command in both runtimes. |
| Account linking | `/profile` | `/profile` | full | Profile view exists on both platforms (UX details differ per platform). |
| Account linking | `/link` | `/link` | full | Code-based linking command is present in both runtimes. |
| Account linking | `/link_telegram` | `/link_discord` | partial | Same linking flow stage (generate link code in the opposite client), but command naming differs by platform perspective. |
| Utility | `/helpy` | `/helpy` | partial | Both expose help; onboarding order for roles now matches (`/roles` first, then acquisition method / next step), but Discord still uses interactive embeds while Telegram returns text help. |
| Roles admin | `/rolesadmin ...` | `/roles_admin ...` | partial | CRUD категорий/ролей и просмотр/выдача/снятие ролей доступны в обоих рантаймах; help/onboarding/fallback тексты теперь выровнены по одному двухшаговому сценарию: подход 1 — настрой каталог (категория → роль → описание/способ получения), подход 2 — выдай/сними роль пользователю. Telegram и Discord одинаково объясняют этот порядок, а в Discord дополнительно есть синхронизация с Discord role id при выдаче/снятии. |
| Points | `/addpoints` | — | missing | No Telegram counterpart in the current Telegram router/command registry. |
| Points | `/removepoints` | — | missing | No Telegram counterpart in the current Telegram router/command registry. |
| Points | `/leaderboard` | — | missing | No Telegram counterpart in the current Telegram router/command registry. |
| Points | `/history` | — | missing | No Telegram counterpart in the current Telegram router/command registry. |
| Points | `/roles` | `/roles` | full | Unified public roles catalog is available in both runtimes: roles are grouped by category and show description, acquisition method, and acquisition hint; both versions now start with the same onboarding block explaining what roles are, where to read acquisition info, and which roles are manual vs automatic. |
| Points | `/activities` | — | missing | No Telegram counterpart in the current Telegram router/command registry. |
| Points | `/undo` | — | missing | No Telegram counterpart in the current Telegram router/command registry. |
| Points | `/awardmonthtop` | — | missing | No Telegram counterpart in the current Telegram router/command registry. |
| Points | `/tophistory` | — | missing | No Telegram counterpart in the current Telegram router/command registry. |
| Points | `/ping` | — | missing | No Telegram counterpart in the current Telegram router/command registry. |
| Points | `/bank` | — | missing | No Telegram counterpart in the current Telegram router/command registry. |
| Points | `/bankadd` | — | missing | No Telegram counterpart in the current Telegram router/command registry. |
| Points | `/bankspend` | — | missing | No Telegram counterpart in the current Telegram router/command registry. |
| Points | `/bankhistory` | — | missing | No Telegram counterpart in the current Telegram router/command registry. |
| Points | `/balance` | `/balance [reply|id]` | partial | Read-only balance view is available in Telegram; Discord supports optional member argument via mention. |
| Fines | `/fine` | — | missing | No Telegram counterpart in the current Telegram router/command registry. |
| Fines | `/myfines` | — | missing | No Telegram counterpart in the current Telegram router/command registry. |
| Fines | `/allfines` | — | missing | No Telegram counterpart in the current Telegram router/command registry. |
| Fines | `/finedetails` | — | missing | No Telegram counterpart in the current Telegram router/command registry. |
| Fines | `/editfine` | — | missing | No Telegram counterpart in the current Telegram router/command registry. |
| Fines | `/cancel_fine` | — | missing | No Telegram counterpart in the current Telegram router/command registry. |
| Fines | `/finehistory` | — | missing | No Telegram counterpart in the current Telegram router/command registry. |
| Fines | `/topfines` | — | missing | No Telegram counterpart in the current Telegram router/command registry. |
| Tournament | `/createtournament` | — | missing | No Telegram counterpart in the current Telegram router/command registry. |
| Tournament | `/tournamentadmin` | — | missing | No Telegram counterpart in the current Telegram router/command registry. |
| Tournament | `/managetournament` | — | missing | No Telegram counterpart in the current Telegram router/command registry. |
| Tournament | `/jointournament` | — | missing | No Telegram counterpart in the current Telegram router/command registry. |
| Tournament | `/tournamenthistory` | — | missing | No Telegram counterpart in the current Telegram router/command registry. |
