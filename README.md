# Discord-бот для системы баллов и штрафов

> Начиная с версии 2.0 бот использует **slash‑команды**. Все функции доступны
> через меню Discord при вводе `/`.

## 🚀 Установка и запуск

1. **Установка зависимостей**:
```bash
pip install -r requirements.txt
```

2. **Настройка окружения**:
   - Скопируйте `.env.example` в `.env` в корне проекта
   - Заполните обязательные переменные:
     ```ini
    DISCORD_TOKEN=ваш_токен_бота
    SUPABASE_URL=ваш_url_supabase
    SUPABASE_KEY=ваш_ключ_supabase
    TOURNAMENT_ANNOUNCE_CHANNEL_ID=ID_канала_анонсов
     ```
   - При необходимости укажите дополнительные переменные для ролей:
     ```ini
     POINTS_ROLE_IDS=ID_роли1,ID_роли2
     FINE_ROLE_IDS=ID_роли1,ID_роли2
     TOURNAMENT_ROLE_IDS=ID_роли1,ID_роли2
     BOT_API_DELAY_SECONDS=3.0
     BOT_API_DELAY_JITTER=0.8
     PROFILE_DISCORD_TITLE_ROLE_IDS=ID_роли_главы,ID_роли_вице
     PROFILE_DISCORD_TITLE_ROLE_NAMES=Глава клуба,Главный вице
     PROFILE_TITLES_SYNC_INTERVAL_SEC=21600
     ```
     Здесь `ID_роли` — числовые идентификаторы ролей, которым разрешены
     соответствующие действия.

     `BOT_API_DELAY_SECONDS` и `BOT_API_DELAY_JITTER` используются для
     мягкого throttling bucket-ов `followup` и `channel_send`: followup/edit после
     ACK, обычных channel/user sends и фоновых уведомлений. Это помогает реже
     ловить 429, но не должно тормозить UX кнопок, модалок и slash-команд.

     Для первичного ACK interaction (`defer`, первый `send_message`) отдельные
     env-переменные не нужны: bucket `interaction_ack` в коде специально работает
     без pre-wait, чтобы подтверждение уходило мгновенно.

     `PROFILE_DISCORD_TITLE_ROLE_IDS` и/или `PROFILE_DISCORD_TITLE_ROLE_NAMES` задают роли Discord,
     которые бот переносит в поле званий профиля общего аккаунта (fallback, если таблица БД не заполнена).

     Приоритетный способ: таблица `profile_title_roles` (см. `sql/p5_profile_title_roles.sql`)
     с маппингом `discord_role_id -> title_name`. Это позволяет хранить звания централизованно в БД.

     Быстрое заполнение таблицы готовым шаблоном: `sql/p6_profile_title_roles_seed_template.sql`
     (замените NULL на свои ID ролей и выполните запрос).

     `PROFILE_TITLES_SYNC_INTERVAL_SEC` задаёт интервал (в секундах) для reconciliation-прохода.
     Основная синхронизация званий теперь идёт event-driven через Discord events изменения ролей и входа участника,
     поэтому безопасный дефолт поднят до `21600` секунд (6 часов), чтобы периодический job только перепроверял состояние.

3. **Запуск бота**:
```bash
python bot/main.py
```
После запуска список команд автоматически синхронизируется с Discord и будет
доступен во всплывающем меню при вводе `/`.

### Рекомендуемый запуск через systemd

Для VPS рекомендуется **не использовать внутренний бесконечный restart loop в приложении** и доверить перезапуск `systemd`. Telegram polling в проекте настроен на fail-fast: если обнаружен конфликт `getUpdates`/polling или после коротких retry не восстановилась сеть, процесс завершается с диагностикой в логах, а `systemd` уже решает, когда поднимать сервис заново.

Пример unit-файла для основного bot runtime:

```ini
[Unit]
Description=Bebrobot runtime
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/bebrobot
EnvironmentFile=/opt/bebrobot/.env
ExecStart=/opt/bebrobot/.venv/bin/python bot/main.py
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Если вы оставляете admin API, лучше держать его отдельным сервисом, чтобы HTTP-интерфейс не влиял на runtime бота:

```ini
[Unit]
Description=Bebrobot admin API
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/bebrobot
EnvironmentFile=/opt/bebrobot/.env
ExecStart=/opt/bebrobot/.venv/bin/python -m bot.admin_api.app
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Итоговая рекомендация для VPS: выбирайте **один** механизм перезапуска. В этом проекте предпочтителен `systemd`, а не комбинация `systemd` + внутренний бесконечный self-healing loop.

### Опциональный admin API

HTTP admin API не нужен для обычного запуска Discord/Telegram-бота и **не поднимается автоматически** из `bot/main.py`. Если вам действительно нужен административный HTTP-интерфейс на VPS, запускайте его отдельным процессом:

```bash
python -m bot.admin_api.app
```

Рекомендуется держать его в отдельном `systemd` unit или отдельной программе `supervisor`, чтобы падение HTTP-сервера не влияло на runtime бота и наоборот. Для настройки адреса используйте `ADMIN_API_HOST`, `ADMIN_API_PORT` и при необходимости `ADMIN_API_DEBUG`.

## 📋 Основные команды

### Система баллов
- `/addpoints @user количество [причина]` - Начислить баллы
- `/removepoints @user количество [причина]` - Снять баллы
- `/mypoints` - Проверить свой баланс

### Система модерации и legacy-штрафов
- `/rep` - Основная точка входа в модерацию: кейс, автонаказание, предупреждения, активный статус и штраф в банк при необходимости
- `/myfines` - Просмотреть свои активные legacy-штрафы, если они остались на переходный период
- `/payfine ID_штрафа` - Оплатить legacy-штраф

### Администрирование
- `/fine @user сумма тип причина` - Legacy-команда совместимости для денежного штрафа; новые сценарии модерации начинаются с `/rep`
- `/editfine ID параметры` - Изменить legacy-штраф
- `/cancel_fine ID` - Отменить legacy-штраф

### Турниры
- `/tournamentadmin` - открыть панель управления турнирами
- `/createtournament` - создать новый турнир
- `/managetournament ID` - панель конкретного турнира

## ⚙️ Технические требования
- Python 3.10+
- База данных Supabase
- Discord-сервер, на котором бот имеет права администратора

## 🛠 Поддержка
Для получения помощи обращайтесь к разработчику.

> **Примечание:** Перед первым запуском убедитесь, что все переменные окружения заполнены корректно!

## 📊 Мониторинг запросов

Бот отслеживает количество обращений к Discord API и фиксирует попадание под лимит 429.
Логи о превышении лимита выводятся в консоль. Дополнительно `safe_interaction` и
`safe_send` пишут диагностические записи о том, сколько interaction ждал до ACK,
какой bucket сработал (`interaction_ack`, `followup`, `channel_send`) и какой
получился effective delay — это помогает быстро увидеть реальные узкие места.

### Система ставок
- `/managetournament` позволяет открыть панель управления турниром, где доступна кнопка **Ставки**.
- Минимальная ставка зависит от стадии: 1 балл на ранних раундах, 2 в полуфинале и 3 в финале.
- Перед подтверждением бот показывает возможный выигрыш.
- Через меню можно просмотреть и изменить свои ставки до начала матча.
- Администратор может увидеть все текущие ставки, отсортированные по никнеймам пользователей,
  и общую сумму, поставленную в турнире.
- Информация о ставках хранится в таблице `tournament_bets`.
- При выборе пары бот отображает карты раунда.
- При создании турнира можно задать банк ставок (до 20 баллов из общего банка).
- Выплата по ставкам требует подтверждения после фиксации результата пары.
- При выборе режима **TEST** банк ставок и балансы пользователей не изменяются,
  что удобно для отладки.



## 🤖 Telegram (подготовка)
- Матрица паритета команд Discord/Telegram: [docs/command_parity_matrix.md](docs/command_parity_matrix.md) (обновляется вместе с новыми user-facing командами).
- Единая точка запуска: `python bot/main.py`.
- В проекте **нет** `keep_alive.py`, ping-to-self cron/workaround и auto-start HTTP web-server для legacy free-hosting: основной runtime поднимает только Discord и/или Telegram, а HTTP admin API запускается отдельно при явной необходимости.
- Для Discord укажите `DISCORD_TOKEN`.
- Для Telegram укажите `TELEGRAM_BOT_TOKEN`.
- Если задан только `DISCORD_TOKEN`, стартует только Discord runtime.
- Если задан только `TELEGRAM_BOT_TOKEN`, стартует только Telegram runtime.
- Если заданы `DISCORD_TOKEN` и `TELEGRAM_BOT_TOKEN`, оба бота стартуют автоматически в одном процессе-launcher без дополнительных флагов.
- Если не задан ни один токен, launcher завершает старт с ошибкой в логах.
- `bot/telegram_bot/main.py` — Telegram runtime-модуль, который `bot/main.py` запускает автоматически, когда найден `TELEGRAM_BOT_TOKEN`.
- Telegram polling-loop (aiogram) стартует автоматически и пишет в лог фактические диагностические сообщения по токену и состоянию блокировки.
- Конфликты polling (`TelegramPollingLockActiveError`, `TelegramPollingPreflightConflictError`, `TelegramPollingConflictDetectedError`) обрабатываются в fail-fast режиме: процесс завершается с понятной ошибкой в логах, чтобы внешний supervisor (`systemd`) мог корректно перезапустить сервис.
- Для действительно временных сетевых ошибок Telegram runtime делает только ограниченное число коротких retry внутри polling-цикла, а затем тоже завершает процесс, не создавая внутренний бесконечный restart loop. Лимит настраивается через `TELEGRAM_POLLING_MAX_TRANSIENT_FAILURES` (по умолчанию 3 подряд неудачных запроса = 2 коротких retry перед остановкой процесса).
- Discord background jobs и восстановление persistent views запускаются один раз на процесс, чтобы reconnect не создавал дубли фоновых задач и лишние повторные запросы к БД.
- В Telegram доступны команды `/start`, `/link`, `/helpy` (список команд обновляется через Telegram API при запуске).
- AI-ответы персонажа Гуй работают и в Discord, и в Telegram (паритет): бот отвечает только если его явно позвали словом `Гуй` или если сообщение является ответом на сообщение бота.
- AI не вмешивается в выполнение команд: в Discord при валидной команде сообщение обрабатывается только как команда, в Telegram AI-ответы пропускаются для командных сообщений и активных сценариев меню (`/points`, `/tickets`, `/profile_edit`).
- Добавлена усиленная защита роли: при попытке выхода модели из образа выполняется повторная генерация со строгим role-lock, а при повторном нарушении используется безопасный fallback-ответ Гуя.
- Если Groq временно недоступен/не настроен, Гуй отвечает диагностической fallback-репликой в чате (и пишет подробную ошибку в логи), чтобы было понятно, почему нет полноценного AI-ответа.
- При ошибках квоты/рейта (`429`) включается cooldown, чтобы не спамить API: в этот период Гуй сразу отвечает, сколько примерно ждать, а в логах фиксируется причина и длительность паузы. Для временного upstream rate-limit у конкретной модели cooldown не активируется глобально, чтобы сразу перейти к fallback-модели.
- Для Groq добавьте переменные окружения:
  - `GROQ_API_KEY` — API-ключ Groq (основной, обязателен для AI-ответов).
  - `GROQ_TEXT_MODEL` — рекомендованный явный pin для базовой text-модели.
  - `GROQ_TEXT_MODELS` — рекомендованная text fallback-цепочка через запятую; если задана, полностью переопределяет встроенный порядок text-моделей.
  - `GROQ_VISION_MODEL` — отдельная vision-модель, которая используется только для анализа медиа.
  - `GROQ_MODEL` — legacy-совместимость для text pin, если новые `GROQ_TEXT_*` env не заданы.
  - `GROQ_MODELS` — legacy-совместимость для text fallback-цепочки, если новые `GROQ_TEXT_*` env не заданы.
  - `GUIY_SYSTEM_PROMPT` — опциональный полный system prompt персонажа.
  - `GUIY_EXTRA_LORE` — опциональное доп.описание лора (добавляется к prompt).
- Базовая text-модель для обычного чата и для финального ответа после анализа медиа — `moonshotai/kimi-k2-instruct-0905`.
- Дефолтная text fallback-цепочка Groq: `moonshotai/kimi-k2-instruct-0905` → `qwen/qwen3-32b` → `llama-3.3-70b-versatile`.
- Media pipeline теперь жёстко разделён:
  - если медиа нет, vision-этап не вызывается вообще, а ответ строится только через text route;
  - если медиа есть, сначала отдельная vision-модель делает factual summary вложения, а затем text-модель Гуя формирует финальный ответ пользователю с учётом prompt, памяти и identity/public context.
- Vision-модель по умолчанию — `llama-3.3-70b-versatile`; она используется только для анализа медиа и не должна напрямую формировать финальный пользовательский ответ.
- Telegram-код изолирован в `bot/telegram_bot/`, чтобы не смешивать с Discord-рантаймом.

## 🔐 Account-first migration (P3)
- SQL hardening script: `sql/p3_account_hardening.sql`
- Audit script: `python scripts/check_account_migration.py`
- Ops runbook: `docs/account_migration_runbook.md`

## 📁 Структура Telegram-кода
- Весь код, связанный с Telegram, расположен в `bot/telegram_bot/`.
- Текущий обработчик привязки Telegram: `bot/telegram_bot/link_handler.py`.
