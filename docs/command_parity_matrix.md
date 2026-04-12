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
| Roles admin | `/rolesadmin` (единая панель) | `/roles_admin` + текстовый alias `/rolesadmin` (единая панель) | full | Пользовательский вход унифицирован: на обеих платформах сначала открывается единая панель, а CRUD-сценарии выполняются кнопками внутри UI, без публичного slash-меню подкоманд. Разница в имени сохранена: Discord — `/rolesadmin`, Telegram — `/roles_admin` (+ alias `/rolesadmin`). |
| Roles admin detail | role descriptions | role descriptions | full | Описание роли обязательно отражается в обоих пользовательских путях: в публичном `/roles` каталоге и в admin-help/onboarding, чтобы админы сразу видели, что пользователю будет показано объяснение назначения роли. |
| Roles admin detail | acquire_hint | acquire_hint | full | Поле `acquire_hint` одинаково объясняется в Discord и Telegram: help подсказывает сначала заполнить описание и «как получить», а публичный `/roles` на обеих платформах выводит и `Способ получения`, и `Как получить`. |
| Roles admin detail | `user_grant` / `user_revoke` batch flow | кнопочный batch flow для `user_grant` / `user_revoke` | full | Массовая выдача/снятие ролей поддерживается на обеих платформах в интерактивной панели: можно проходить по категориям, выбирать несколько ролей и подтверждать пакет одной кнопкой. Ограничение явно зафиксировано в help: Telegram text fallback остаётся одно-ролевым, чтобы не обещать пакетный режим там, где его нет. |
| Moderation | `/rep` | `/rep` | partial | `/rep` — основная точка входа в модерацию на обеих платформах: выбрать нарушителя → выбрать нарушение → увидеть авторасчёт наказания → подтвердить → получить кейс, активный статус, предупреждения, историю нарушений и информацию о штрафе в банк, если он был частью кейса. Discord использует `discord.ui.View` + select, Telegram — pending-state + inline-кнопки. Временное ограничение Telegram зафиксировано явно: выбор нарушителя там начинается с reply/username/id ввода, потому что у платформы нет нативного user-select, но стартовая инструкция, preview/result тексты, отмена, expired-state, duplicate-submit и friendly error остаются общими по смыслу и объясняют, что наказание считается автоматически по типу нарушения и числу предупреждений. Legacy `/topfines` больше не считается продуктовой командой и не требует parity-поддержки. |
| Moderation | `/modstatus` | `/modstatus` | partial | Единая read-only команда пользовательского просмотра модерации на обеих платформах: без reply показывает только себя, через reply в группе/на сервере открывает чужой профиль, а в личке lookup чужого профиля доступен только модератору по явному правилу. Команда одинаково показывает активные предупреждения и муты, неоплаченные legacy-штрафы, последние кейсы и объясняет, как оплатить штраф и как смотреть другого пользователя через reply. Оплата legacy-штрафа теперь открывается кнопкой прямо из `/modstatus` на обеих платформах. |
| Guiy owner | hidden `guiy_owner` owner-only command | private `/guiy_owner` owner-only command | partial | Функциональность owner-only управления Гуем есть на обеих платформах, но интерфейс различается: в Discord это скрытая prefix-команда `guiy_owner`, а в Telegram — приватная slash-команда `/guiy_owner`, регистрируемая только в owner-scoped command menu. На обеих платформах вход в «Профиль Гуя» теперь автоматически делает bootstrap общего аккаунта бота при первом открытии и сразу показывает кнопки редактирования; отдельное действие `register_profile` оставлено как явный ручной шаг. Публичный help её не рекламирует, чтобы не создавать ложных ожиданий у обычных пользователей. |
| Points | `/addpoints` | — | missing | No Telegram counterpart in the current Telegram router/command registry. |
| Points | `/removepoints` | — | missing | No Telegram counterpart in the current Telegram router/command registry. |
| Points | `/top` | `/top` | full | Unified leaderboard with period switch (all time / month / week) is available in both runtimes. |
| Points | `/history` | — | missing | No Telegram counterpart in the current Telegram router/command registry. |
| Points | `/roles` | `/roles` | full | Unified public roles catalog is available in both runtimes: roles are grouped by category and show description, acquisition method, and acquisition hint; both versions now start with the same onboarding block explaining what roles are, where to read acquisition info, and which roles are manual vs automatic. |
| Points | `/activities` | — | missing | No Telegram counterpart in the current Telegram router/command registry. |
| Points | `/undo` | — | missing | No Telegram counterpart in the current Telegram router/command registry. |
| Points | `/ping` | — | missing | No Telegram counterpart in the current Telegram router/command registry. |
| Points | `/bank` + кнопка `⚙️ Настройка банка` (`add/spend/history`) | `/bank` + кнопка `⚙️ Настройка банка` (`add/spend/history`) | full | На обеих платформах `/bank` показывает баланс. Кнопочный сценарий настройки (пополнение/списание/история) доступен только суперадмину в ЛС, с обязательной причиной для изменений; отдельная команда `bankhistory` удалена как дублирующая кнопку истории. |
| Points | `/balance` | `/balance [reply|id]` | partial | Read-only balance view is available in Telegram; Discord supports optional member argument via mention. |
| Fines (legacy payment only) | `/modstatus` + кнопка оплаты | `/modstatus` + кнопка оплаты | full | Transitional payment UI for legacy fines открывается из `/modstatus` на обеих платформах. Внутри панели оплаты остаются кнопки долей (25/50/100%), а сам `/rep` остаётся точкой применения новых наказаний. |
| Tournament | `/createtournament` | — | missing | No Telegram counterpart in the current Telegram router/command registry. |
| Tournament | `/tournamentadmin` | — | missing | No Telegram counterpart in the current Telegram router/command registry. |
| Tournament | `/managetournament` | — | missing | No Telegram counterpart in the current Telegram router/command registry. |
| Tournament | `/jointournament` | — | missing | No Telegram counterpart in the current Telegram router/command registry. |
| Tournament | `/tournamenthistory` | — | missing | No Telegram counterpart in the current Telegram router/command registry. |
