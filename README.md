# Discord-бот для системы баллов и штрафов

> Начиная с версии 2.0 бот использует **slash‑команды**. Все функции доступны
> через меню Discord при вводе `/`.

## 🚀 Установка и запуск

1. **Установка зависимостей**:
```bash
pip install -r requirements.txt
```

2. **Настройка окружения**:
   - Создайте файл `.env` в корне проекта
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
     PROFILE_TITLES_SYNC_INTERVAL_SEC=900
     ```
     Здесь `ID_роли` — числовые идентификаторы ролей, которым разрешены
     соответствующие действия.

     `BOT_API_DELAY_SECONDS` и `BOT_API_DELAY_JITTER` помогают распределять
     запросы к API во времени (чтобы реже ловить 429). Первый задаёт базовую
     задержку между запросами, второй добавляет случайный разброс сверху.

     `PROFILE_DISCORD_TITLE_ROLE_IDS` и/или `PROFILE_DISCORD_TITLE_ROLE_NAMES` задают роли Discord,
     которые бот переносит в поле званий профиля общего аккаунта (fallback, если таблица БД не заполнена).

     Приоритетный способ: таблица `profile_title_roles` (см. `sql/p5_profile_title_roles.sql`)
     с маппингом `discord_role_id -> title_name`. Это позволяет хранить звания централизованно в БД.

     `PROFILE_TITLES_SYNC_INTERVAL_SEC` задаёт интервал (в секундах) для фоновой синхронизации званий.

3. **Запуск бота**:
```bash
python bot/main.py
```
После запуска список команд автоматически синхронизируется с Discord и будет
доступен во всплывающем меню при вводе `/`.

## 📋 Основные команды

### Система баллов
- `/addpoints @user количество [причина]` - Начислить баллы
- `/removepoints @user количество [причина]` - Снять баллы
- `/mypoints` - Проверить свой баланс

### Система штрафов
- `/fine @user сумма тип причина` - Назначить штраф
- `/myfines` - Просмотреть свои штрафы
- `/payfine ID_штрафа` - Оплатить штраф

### Администрирование
- `/editfine ID параметры` - Изменить штраф
- `/cancel_fine ID` - Отменить штраф
- `/topfines` - Топ должников

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
Логи о превышении лимита выводятся в консоль.

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
- Переменная токена для Render: `TELEGRAM_BOT_TOKEN`.
- Единая точка запуска: `python bot/main.py`.
- Выбор рантайма через `BOT_RUNTIME`:
  - `BOT_RUNTIME=discord` (только Discord)
  - `BOT_RUNTIME=telegram` (только Telegram)
  - `BOT_RUNTIME=both` (запуск Telegram + Discord одновременно в одном asyncio event loop / основном потоке)
- Если `BOT_RUNTIME` не задан, лаунчер автоматически:
  - выберет `discord`, когда заданы и `DISCORD_TOKEN`, и Telegram-токен (`TELEGRAM_BOT_TOKEN`) (без параллельного старта Telegram);
  - выберет `telegram`, когда задан только Telegram-токен (`TELEGRAM_BOT_TOKEN`).
- `bot/telegram_bot/main.py` — это Telegram runtime-модуль, который вызывается из `bot/main.py` при `BOT_RUNTIME=telegram`.
- В Telegram-режиме поднимается polling-loop (aiogram) и в лог пишется `telegram bot started`.
- В Telegram-режиме доступны команды `/start`, `/link`, `/helpy` (список команд обновляется через Telegram API при запуске).
- Telegram-код изолирован в `bot/telegram_bot/`, чтобы не смешивать с Discord-рантаймом.

## 🔐 Account-first migration (P3)
- SQL hardening script: `sql/p3_account_hardening.sql`
- Audit script: `python scripts/check_account_migration.py`
- Ops runbook: `docs/account_migration_runbook.md`

## 📁 Структура Telegram-кода
- Весь код, связанный с Telegram, расположен в `bot/telegram_bot/`.
- Текущий обработчик привязки Telegram: `bot/telegram_bot/link_handler.py`.
