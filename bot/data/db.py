import os
import logging
from discord.ext import commands
from typing import Optional
from datetime import datetime, timezone, timedelta
from collections import UserDict, UserList
from supabase import create_client
from postgrest.exceptions import APIError
from dotenv import load_dotenv
import traceback
import asyncio

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LazyDict(UserDict):
    """Словарь с ленивой загрузкой данных при первом доступе."""

    def __init__(self, loader):
        super().__init__()
        self._loader = loader

    def _ensure(self):
        self._loader()

    def __getitem__(self, key):
        self._ensure()
        return super().__getitem__(key)

    def __setitem__(self, key, value):
        self._ensure()
        return super().__setitem__(key, value)

    def __contains__(self, item):
        self._ensure()
        return super().__contains__(item)

    def get(self, key, default=None):
        self._ensure()
        return super().get(key, default)

    def items(self):
        self._ensure()
        return super().items()

    def values(self):
        self._ensure()
        return super().values()

    def keys(self):
        self._ensure()
        return super().keys()

    def __iter__(self):
        self._ensure()
        return super().__iter__()

    def __len__(self):
        self._ensure()
        return super().__len__()

    def clear(self):
        self._ensure()
        return super().clear()

    def pop(self, key, default=None):
        self._ensure()
        return super().pop(key, default)

    def set_data(self, value):
        self.data = value


class LazyList(UserList):
    """Список с ленивой загрузкой данных при первом доступе."""

    def __init__(self, loader):
        super().__init__()
        self._loader = loader

    def _ensure(self):
        self._loader()

    def __iter__(self):
        self._ensure()
        return super().__iter__()

    def __len__(self):
        self._ensure()
        return super().__len__()

    def __getitem__(self, index):
        self._ensure()
        return super().__getitem__(index)

    def append(self, item):
        self._ensure()
        return super().append(item)

    def insert(self, i, item):
        self._ensure()
        return super().insert(i, item)

    def set_data(self, value):
        self.data = value

class Database:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.init_db()
        return cls._instance

    def init_db(self):
        load_dotenv()
        self.bot = None
        self.bot: Optional[commands.Bot] = None
        self.url = (os.getenv("SUPABASE_URL") or "").strip()
        self.key = (
            os.getenv("SUPABASE_KEY")
            or os.getenv("SUPABASE_SECRET_KEY")
            or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
            or ""
        ).strip()
        self.supabase = create_client(self.url, self.key) if self.url and self.key else None
        self.has_was_on_time = True
        self._core_data_loaded = False
        self._core_data_loading = False
        self._fines_data_loaded = False
        self._account_to_discord_cache = {}
        self._table_account_id_support = {}

        self.scores = LazyDict(self.ensure_core_data_loaded)
        self.actions = LazyList(self.ensure_core_data_loaded)
        self.history = LazyDict(self.ensure_core_data_loaded)
        self.fines = LazyList(self.ensure_fines_loaded)
        self.fine_payments = LazyList(self.ensure_fines_loaded)

        self._ensure_tables()
        self.quick_pay_streak = {}
        self.guild_id = int(os.getenv("GUILD_ID", 0))
        self.fast_payer_role_id = int(os.getenv("FAST_PAYER_ROLE_ID", 0))
        
    def _ensure_tables(self):
        """Проверяет существование обязательных таблиц"""
        if not self.supabase:
            return

        try:
            # Проверка существования таблицы scores
            self.supabase.table("scores").select("user_id").limit(1).execute()
        except Exception as e:
            raise RuntimeError(f"Таблица scores не существует или недоступна: {str(e)}")
        try:
            self.supabase.table("fines").select("id").limit(1).execute()
            try:
                self.supabase.table("fines").select("was_on_time").limit(1).execute()
                self.has_was_on_time = True
            except Exception:
                self.has_was_on_time = False
                logger.warning("Столбец 'was_on_time' отсутствует в таблице fines")
        except Exception as e:
            raise RuntimeError(f"Таблица fines не существует или недоступна: {str(e)}")

        try:
            self.supabase.table("fine_payments").select("id").limit(1).execute()
        except Exception as e:
            raise RuntimeError(f"Таблица fine_payments не существует или недоступна: {str(e)}")

    def ensure_core_data_loaded(self):
        if not self._core_data_loaded and not self._core_data_loading:
            self.load_data()

    def ensure_fines_loaded(self):
        if not self._fines_data_loaded:
            self.load_fines()

    def _get_account_id_for_discord_user(self, user_id: int) -> Optional[str]:
        """Возвращает account_id для Discord user_id (если есть связь)."""
        if not self.supabase:
            return None
        try:
            response = (
                self.supabase.table("account_identities")
                .select("account_id")
                .eq("provider", "discord")
                .eq("provider_user_id", str(user_id))
                .limit(1)
                .execute()
            )
            if response.data:
                return response.data[0].get("account_id")
        except Exception as e:
            logger.warning("Не удалось получить account_id для user_id=%s: %s", user_id, e)
        return None

    def _get_discord_user_for_account_id(self, account_id: str) -> Optional[int]:
        """Возвращает Discord user_id для account_id (если есть связь)."""
        if not account_id:
            return None
        if account_id in self._account_to_discord_cache:
            return self._account_to_discord_cache[account_id]
        if not self.supabase:
            return None
        try:
            response = (
                self.supabase.table("account_identities")
                .select("provider_user_id")
                .eq("provider", "discord")
                .eq("account_id", account_id)
                .limit(1)
                .execute()
            )
            if response.data:
                discord_user_id = int(response.data[0]["provider_user_id"])
                self._account_to_discord_cache[account_id] = discord_user_id
                return discord_user_id
        except Exception as e:
            logger.warning("Не удалось получить discord user_id для account_id=%s: %s", account_id, e)
        return None

    def _resolve_user_id_from_row(self, row: dict) -> Optional[int]:
        """Определяет user_id для локальных кешей, предпочитая account-based данные."""
        account_id = row.get("account_id")
        if account_id:
            mapped = self._get_discord_user_for_account_id(account_id)
            if mapped is not None:
                return mapped
        user_id = row.get("user_id")
        if user_id is None:
            return None
        try:
            return int(user_id)
        except (TypeError, ValueError):
            return None

    def _get_scores_row_for_user(self, user_id: int, fields: str) -> Optional[dict]:
        """Читает запись scores с приоритетом account_id, затем fallback на user_id."""
        if not self.supabase:
            return None

        account_id = self._get_account_id_for_discord_user(user_id)
        try:
            if account_id:
                by_account = (
                    self.supabase.table("scores")
                    .select(fields)
                    .eq("account_id", account_id)
                    .limit(1)
                    .execute()
                )
                if by_account.data:
                    return by_account.data[0]

            by_user = (
                self.supabase.table("scores")
                .select(fields)
                .eq("user_id", user_id)
                .limit(1)
                .execute()
            )
            if by_user.data:
                return by_user.data[0]
        except Exception as e:
            logger.warning("Не удалось прочитать scores для user_id=%s: %s", user_id, e)
        return None

    def _with_account_id(self, user_id: int, payload: dict) -> dict:
        """Возвращает payload, дополненный account_id (если он связан с user_id)."""
        account_id = self._get_account_id_for_discord_user(user_id)
        if account_id:
            payload["account_id"] = account_id
        return payload

    def _table_supports_account_id(self, table_name: str) -> bool:
        """Проверяет, есть ли в таблице столбец account_id (с кешированием)."""
        if table_name in self._table_account_id_support:
            return self._table_account_id_support[table_name]

        if not self.supabase:
            self._table_account_id_support[table_name] = False
            return False

        try:
            self.supabase.table(table_name).select("account_id").limit(1).execute()
            self._table_account_id_support[table_name] = True
        except Exception:
            self._table_account_id_support[table_name] = False

        return self._table_account_id_support[table_name]

    def _with_optional_account_id(self, table_name: str, user_id: int, payload: dict) -> dict:
        """Добавляет account_id только если таблица поддерживает столбец account_id."""
        if self._table_supports_account_id(table_name):
            return self._with_account_id(user_id, payload)
        return payload
      
    def load_data(self):
        """Загружает все данные с автоматическим восстановлением связей"""
        if self._core_data_loaded or self._core_data_loading:
            return

        self._core_data_loading = True
        logger.info("⚙️ Синхронизация с Supabase...")
        try:
            if not self.supabase:
                raise ConnectionError("Supabase: нет подключения")

            # 1. Загружаем баллы
            scores_response = self.supabase.from_('scores').select('*').execute()
            if hasattr(scores_response, 'data'):
                scores_data = {}
                for item in scores_response.data:
                    resolved_user_id = self._resolve_user_id_from_row(item)
                    if resolved_user_id is None:
                        continue
                    scores_data[resolved_user_id] = float(item['points'])
                self.scores.set_data(scores_data)
            else:
                raise ValueError("Некорректный ответ от Supabase при загрузке баллов")

            # 2. Загружаем действия
            actions_response = self.supabase.from_('actions')\
                .select('*')\
                .order('timestamp', desc=True)\
                .execute()

            if hasattr(actions_response, 'data'):
                self.actions.set_data(actions_response.data)
                self._build_history()
            else:
                raise ValueError("Некорректный ответ от Supabase при загрузке действий")

            self._core_data_loaded = True
            logger.info(f"✅ Данные синхронизированы | Пользователей: {len(self.scores.data)}")

        except Exception as e:
            logger.error(f"❌ Ошибка синхронизации: {str(e)}")
            traceback.print_exc()
            self.scores.set_data({})
            self.actions.set_data([])
            self.history.set_data({})
            self._core_data_loaded = True
        finally:
            self._core_data_loading = False

    def _build_history(self):
        """Строит историю действий"""
        history = {}
        for action in self.actions.data:
            user_id = self._resolve_user_id_from_row(action)
            if user_id is None:
                continue
            if user_id not in history:
                history[user_id] = []
            history[user_id].append({
                'points': float(action['points']),
                'reason': action['reason'],
                'author_id': int(action['author_id']),
                'timestamp': action['timestamp']
            })
        self.history.set_data(history)

    def update_scores(self, user_id: int, points_change: float):
        """Атомарное обновление баллов с проверкой"""
        if not self.supabase:
            return False

        self.ensure_core_data_loaded()

        try:
            # 1. Получаем текущие баллы из локального кеша
            # Это убирает лишний SELECT к Supabase на каждое изменение баллов.
            cached_points = self.scores.get(user_id)
            if cached_points is not None:
                current_points = float(cached_points)
            else:
                # Fallback для пользователей, которых ещё нет в кеше
                # (например, если запись была добавлена извне).
                try:
                    score_row = self._get_scores_row_for_user(user_id, "points")
                    current_points = float(score_row["points"]) if score_row else 0
                except Exception:
                    current_points = 0

            new_points = max(current_points + points_change, 0)  # Не уходим в минус

            # 2. Обновляем баллы через upsert
            account_id = self._get_account_id_for_discord_user(user_id)
            upsert_payload = {
                "user_id": user_id,
                "points": new_points,
            }
            if account_id:
                upsert_payload["account_id"] = account_id

            result = self.supabase.table("scores")\
                .upsert(upsert_payload)\
                .execute()

            if result:
                # Обновляем локальный кеш
                self.scores[user_id] = new_points
                return True
        except Exception as e:
            logger.error(f"🔥 Ошибка обновления баллов: {str(e)}")
            traceback.print_exc()
            return False

    def add_action(self, user_id: int, points: float, reason: str, author_id: int, is_undo: bool = False):
        """Добавляет действие с гарантированной синхронизацией"""
        self.ensure_core_data_loaded()

        try:
            # 1. Обновляем баллы
            if not self.update_scores(user_id, points):
                raise RuntimeError("Не удалось обновить баллы")

            # 2. Создаем запись действия
            action = {
                "user_id": user_id,
                "points": points,
                "reason": reason,
                "author_id": author_id,
                "action_type": "remove" if points < 0 else "add"
            }
            account_id = self._get_account_id_for_discord_user(user_id)
            if account_id:
                action["account_id"] = account_id
            # Добавляем is_undo только если True
            if is_undo:
                action["is_undo"] = True
                
            # 3. Сохраняем действие
            if not self.supabase:
                logger.warning("Supabase client is not initialized.")
                return False
            response = self.supabase.table("actions")\
                .insert(action)\
                .execute()

            if not response.data:
                raise ValueError("Пустой ответ от Supabase")

            # 4. Обновляем локальный кеш
            self.actions.insert(0, response.data[0])  # Добавляем в начало
            if user_id not in self.history:
                self.history[user_id] = []
            self.history[user_id].insert(0, {
                'points': points,
                'reason': reason,
                'author_id': author_id,
                'timestamp': response.data[0]['timestamp'],
                'is_undo': is_undo
            })

            logger.info(f"✅ Действие сохранено (ID: {response.data[0]['id']})")
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка добавления действия: {str(e)}")
            traceback.print_exc()
            return False

    def _handle_response(self, response):
        """Обработка ответа от Supabase"""
        if not response:
            return None
        if hasattr(response, 'error') and response.error:
            raise Exception(f"Supabase error: {response.error}")
        return response

    def save_all(self):
        try:
            if not self.supabase:
                logger.warning("⚠️ Supabase не инициализирован")
                return

            if not self._core_data_loaded:
                return
                
            if self.scores:
                scores_data = [{"user_id": k, "points": v} for k, v in self.scores.items()]
                response = self._handle_response(
                    self.supabase.table("scores").upsert(scores_data).execute()
                )
                if response:
                    logger.info(f"💾 Данные сохранены: {len(response.data if response.data else [])} записей")
        except Exception as e:
            logger.error(f"🔥 Ошибка сохранения: {str(e)}")
            traceback.print_exc()

    def log_monthly_top(self, entries: list, month: int, year: int):
        """Запись топа месяца в Supabase"""
        if not self.supabase:
            logger.warning("Supabase не инициализирован для логирования топа")
            return False

        log_entries = [
            {
                "user_id": uid,
                "month": month,
                "year": year,
                "place": i + 1,
                "bonus": round(points * percent, 2)
            }
            for i, (uid, points, percent) in enumerate(entries)
        ]

        try:
            self.supabase.table("monthly_top_log").insert(log_entries).execute()
            logger.info("✅ Лог топа месяца записан")
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка записи топа месяца: {e}")
            return False

#Штрафы
    def load_fines(self):
        """Загружает штрафы и оплаты из Supabase"""
        if self._fines_data_loaded:
            return

        if not self.supabase:
            self.fines.set_data([])
            self.fine_payments.set_data([])
            self._fines_data_loaded = True
            return

        try:
            fines_resp = self.supabase.table("fines").select("*").execute()
            payments_resp = self.supabase.table("fine_payments").select("*").execute()

            self.fines.set_data(fines_resp.data if hasattr(fines_resp, "data") else [])
            self.fine_payments.set_data(payments_resp.data if hasattr(payments_resp, "data") else [])

            self._fines_data_loaded = True
            logger.info(f"✅ Загружено штрафов: {len(self.fines.data)}, оплат: {len(self.fine_payments.data)}")

        except Exception as e:
            logger.error(f"❌ Ошибка при загрузке штрафов: {str(e)}")
            traceback.print_exc()
            self.fines.set_data([])
            self.fine_payments.set_data([])
            self._fines_data_loaded = True

    def add_fine(self, user_id: int, author_id: int, amount: float, fine_type: int, reason: str, due_date: datetime):
        """Создаёт штраф"""
        if not self.supabase:
            logger.warning("⚠️ Supabase не инициализирован")
            return None

        try:
            payload = {
                "user_id": user_id,
                "author_id": author_id,
                "amount": amount,
                "type": fine_type,
                "reason": reason,
                "due_date": due_date.isoformat()
            }
            account_id = self._get_account_id_for_discord_user(user_id)
            if account_id:
                payload["account_id"] = account_id

            result = self.supabase.table("fines").insert(payload).execute()

            if not result.data:
                raise ValueError("❌ Пустой ответ от Supabase при создании штрафа")

            fine = result.data[0]
            self.fines.insert(0, fine)
            return fine

        except Exception as e:
            logger.error(f"❌ Ошибка добавления штрафа: {str(e)}")
            traceback.print_exc()
            return None

    def get_user_fines(self, user_id: int, active_only: bool = True):
        """Возвращает список штрафов пользователя"""
        account_id = self._get_account_id_for_discord_user(user_id)
        return [
            fine for fine in self.fines
            if (
                (account_id and fine.get("account_id") == account_id)
                or (not account_id and fine.get("user_id") == user_id)
            )
            and (not active_only or (not fine["is_paid"] and not fine["is_canceled"]))
        ]

    def get_fine_by_id(self, fine_id: int):
        for fine in self.fines:
            if fine["id"] == fine_id:
                return fine
        return None

    def get_bank_balance(self):
        try:
            if not self.supabase:
                return 0.0
            response = self.supabase.table("bank").select("total").limit(1).execute()
            if response.data and len(response.data) > 0:
                return float(response.data[0]["total"])
        except Exception as e:
            logger.error(f"Ошибка чтения баланса банка: {str(e)}")
        return 0.0

    def add_to_bank(self, amount: float):
        try:
            if not self.supabase:
                logger.warning("❌ Supabase не инициализирован")
                return False
            current = self.get_bank_balance()
            new_total = current + amount
            self.supabase.table("bank").upsert({
                "id": 1,
                "total": new_total,
                "updated_at": datetime.now(timezone.utc).isoformat()
            }).execute()
            return True
        except Exception as e:
            logger.error(f"Ошибка обновления банка: {str(e)}")
            return False

    def record_payment(self, user_id: int, fine_id: int, amount: float, author_id: int) -> bool:
        """Записывает оплату штрафа, обновляет банк, баллы, штраф"""
        try:
            if not self.supabase:
                logger.warning("❌ Supabase не инициализирован")
                return False

            fine = self.get_fine_by_id(fine_id)
            is_test = False
            if fine:
                reason = str(fine.get("reason", ""))
                is_test = "test" in reason.lower()

            # 1. Обновляем баллы пользователя
            if not self.update_scores(user_id, -amount):
                return False

            # 2. Добавляем запись в fine_payments
            payment_payload = {
                "fine_id": fine_id,
                "user_id": user_id,
                "amount": amount,
                "author_id": author_id
            }
            account_id = self._get_account_id_for_discord_user(user_id)
            if account_id:
                payment_payload["account_id"] = account_id
            self.supabase.table("fine_payments").insert(payment_payload).execute()

            # 3. Лог действия
            self.add_action(user_id, -amount, f"Оплата штрафа ID #{fine_id}", author_id)

            # 4. Обновление баланса банка (если штраф не тестовый)
            if not is_test:
                self.add_to_bank(amount)
                self.log_bank_income(user_id, amount, f"Оплата штрафа ID #{fine_id}")

            # 5. Обновляем данные по штрафу
            if fine:
                fine['paid_amount'] = round(fine.get('paid_amount', 0) + amount, 2)
                fine['is_paid'] = fine['paid_amount'] >= fine['amount']

                update_data = {
                    "paid_amount": fine['paid_amount'],
                    "is_paid": fine['is_paid']
                }

                if self.has_was_on_time and fine['is_paid'] and 'was_on_time' not in fine:
                    paid_now = datetime.now(timezone.utc)

                    due_raw = fine.get("due_date")
                    post_raw = fine.get("postponed_until")

                    try:
                        if isinstance(due_raw, str):
                            due_dt = datetime.fromisoformat(due_raw)
                            if paid_now <= due_dt:
                                update_data["was_on_time"] = True
                            elif isinstance(post_raw, str):
                                post_dt = datetime.fromisoformat(post_raw)
                                update_data["was_on_time"] = paid_now <= post_dt
                            else:
                                update_data["was_on_time"] = False
                        else:
                            update_data["was_on_time"] = False
                    except Exception:
                        update_data["was_on_time"] = False

                try:
                    self.supabase.table("fines").update(update_data).eq("id", fine_id).execute()
                except APIError as e:
                    if "was_on_time" in str(e) and getattr(e, "code", "") == "PGRST204":
                        self.has_was_on_time = False
                        update_data.pop("was_on_time", None)
                        self.supabase.table("fines").update(update_data).eq("id", fine_id).execute()
                    else:
                        raise

                # 🎯 Быстрая и своевременная оплата (штраф ≥ 3)
                created_raw = fine.get("created_at")
                now = datetime.now(timezone.utc)

                try:
                    if (
                        fine['is_paid']
                        and fine.get("was_on_time")
                        and fine['amount'] >= 3
                        and isinstance(created_raw, str)
                    ):
                        created_dt = datetime.fromisoformat(created_raw)
                        if (now - created_dt).days <= 5:
                            self._track_quick_payment(user_id)
                        else:
                            self.quick_pay_streak[user_id] = 0
                    else:
                        self.quick_pay_streak[user_id] = 0
                except Exception:
                    self.quick_pay_streak[user_id] = 0


            return True

        except Exception as e:
            logger.error(f"❌ Ошибка при записи оплаты: {e}")
            traceback.print_exc()
            return False

    def can_postpone(self, user_id: int) -> bool:
        """Проверяет, был ли пользователь уже отсрочен за последние 60 дней"""
        if user_id not in self.history:
            return True
        now = datetime.now(timezone.utc)
        for entry in reversed(self.history[user_id]):
            if entry.get("reason", "").startswith("Отсрочка штрафа"):
                try:
                    ts = entry.get("timestamp")
                    if isinstance(ts, str):
                        ts = datetime.fromisoformat(ts)
                    if (now - ts).days < 60:
                        return False
                except Exception:
                    continue
        return True

    def apply_postponement(self, fine_id: int, days: int = 7) -> bool:
        """Добавляет дни к сроку штрафа и записывает в логи"""
        try:
            if not self.supabase:
                return False
            fine = self.get_fine_by_id(fine_id)
            if not fine:
                return False
            original_due = datetime.fromisoformat(fine["due_date"])
            new_due = original_due + timedelta(days=days)

            self.supabase.table("fines").update({
                "due_date": new_due.isoformat(),
                "postponed_until": datetime.now(timezone.utc).isoformat()
            }).eq("id", fine_id).execute()

            fine["due_date"] = new_due.isoformat()
            fine["postponed_until"] = datetime.now(timezone.utc).isoformat()

            self.add_action(
                user_id=fine["user_id"],
                points=0,
                reason=f"Отсрочка штрафа ID #{fine_id} на {days} дн.",
                author_id=fine["author_id"]
            )
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка при отсрочке штрафа: {e}")
            traceback.print_exc()
            return False

    def mark_overdue(self, fine: dict) -> bool:
        """Помечает штраф как просроченный и логирует"""
        try:
            if not self.supabase:
                return False
            self.supabase.table("fines").update({
                "is_overdue": True
            }).eq("id", fine["id"]).execute()

            fine["is_overdue"] = True

            self.add_action(
                user_id=fine["user_id"],
                points=0,
                reason=f"Просрочка штрафа ID #{fine['id']}",
                author_id=fine["author_id"]
            )
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка при отметке просрочки штрафа: {e}")
            traceback.print_exc()
            return False

    def log_monthly_fine_top(self, entries: list):
        if not self.supabase:
            logger.warning("Supabase не инициализирован для штрафного лога")
            return False

        now = datetime.now()
        month = now.month
        year = now.year

        logs = [
            {
                "user_id": uid,
                "month": month,
                "year": year,
                "place": i + 1,
                "penalty": round(total * percent, 2)
            }
            for i, ((uid, total), percent) in enumerate(entries)
        ]

        try:
            self.supabase.table("monthly_fine_hst").insert(logs).execute()
            logger.info("✅ История штрафного топа записана")
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка записи штрафного топа: {e}")
            return False

    def _track_quick_payment(self, user_id: int):
        self.quick_pay_streak[user_id] = self.quick_pay_streak.get(user_id, 0) + 1

        if self.quick_pay_streak[user_id] >= 10:
            logger.info(f"🏆 Пользователь {user_id} выполнил 10 быстрых оплат подряд")

            if self.bot:
                guild = self.bot.get_guild(self.guild_id)
                if guild:
                    member = guild.get_member(user_id)
                    role = guild.get_role(self.fast_payer_role_id)
                    if member and role and role not in member.roles:
                        asyncio.create_task(
                            member.add_roles(role, reason="Быстрая оплата 10 штрафов подряд")
                        )
                        logger.info(f"🎖 Роль выдана пользователю {user_id}")

            self.quick_pay_streak[user_id] = 0
        else:
            logger.info(f"⏱ Быстрая оплата: {self.quick_pay_streak[user_id]} подряд")

    def spend_from_bank(self, amount: float, user_id: int, reason: str) -> bool:
        try:
            if not self.supabase:
                return False
            current = self.get_bank_balance()
            if current < amount:
                return False
            new_total = current - amount
            self.supabase.table("bank").upsert({
                "id": 1,
                "total": new_total,
                "updated_at": datetime.now(timezone.utc).isoformat()
            }).execute()

            history_payload = {
                "user_id": user_id,
                "amount": -amount,
                "reason": reason,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            if self._table_supports_account_id("bank_history"):
                account_id = self._get_account_id_for_discord_user(user_id)
                if account_id:
                    history_payload["account_id"] = account_id

            self.supabase.table("bank_history").insert(history_payload).execute()

            return True
        except Exception as e:
            logger.error(f"Ошибка при трате из банка: {str(e)}")
            return False

    def log_bank_income(self, user_id: int, amount: float, reason: str) -> bool:
        try:
            if not self.supabase:
                return False
            history_payload = {
                "user_id": user_id,
                "amount": amount,
                "reason": reason,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            if self._table_supports_account_id("bank_history"):
                account_id = self._get_account_id_for_discord_user(user_id)
                if account_id:
                    history_payload["account_id"] = account_id

            self.supabase.table("bank_history").insert(history_payload).execute()
            return True
        except Exception as e:
            logger.error(f"Ошибка записи операции в банк: {e}")
            return False

    def update_tickets(self, user_id: int, ticket_type: str, amount: int) -> bool:
        """
    Обновляет количество билетов у пользователя.
    ticket_type: 'normal' | 'gold'
        """
        if ticket_type not in ("normal", "gold") or not self.supabase:
            return False

        field = f"tickets_{ticket_type}"
        try:
            # Получаем текущее значение
            score_row = self._get_scores_row_for_user(user_id, field)
            current = int(score_row[field]) if score_row and score_row.get(field) is not None else 0
            new_value = max(current + amount, 0)

            # Обновляем значение
            upsert_payload = {
                "user_id": user_id,
                field: new_value
            }
            if self._table_supports_account_id("scores"):
                account_id = self._get_account_id_for_discord_user(user_id)
                if account_id:
                    upsert_payload["account_id"] = account_id

            self.supabase.table("scores").upsert(upsert_payload).execute()

            return True
        except Exception as e:
            logger.error(f"Ошибка обновления билетов: {e}")
            return False

    def log_ticket_action(self, user_id: int, ticket_type: str, amount: int, reason: str, author_id: int):
        """Логирует изменение билетов в ticket_actions"""
        try:
            payload = {
                "user_id": user_id,
                "ticket_type": ticket_type,
                "amount": amount,
                "reason": reason,
                "author_id": author_id,
            }
            if self._table_supports_account_id("ticket_actions"):
                account_id = self._get_account_id_for_discord_user(user_id)
                if account_id:
                    payload["account_id"] = account_id

            self.supabase.table("ticket_actions").insert(payload).execute()
        except Exception as e:
            logger.error(f"Ошибка логирования тикета: {e}")

    def give_ticket(self, user_id: int, ticket_type: str, amount: int, reason: str, author_id: int) -> bool:
        """
        Начисляет билеты и логирует
        """
        if self.update_tickets(user_id, ticket_type, amount):
            self.log_ticket_action(user_id, ticket_type, amount, reason, author_id)
            return True
        return False

    def remove_ticket(self, user_id: int, ticket_type: str, amount: int, reason: str, author_id: int) -> bool:
        """
        Снимает билеты и логирует
        """
        if self.update_tickets(user_id, ticket_type, -amount):
            self.log_ticket_action(user_id, ticket_type, -amount, reason, author_id)
            return True
        return False

    def transfer_user_data(self, old_id: int, new_id: int) -> bool:
        """
        Переносит все данные (баллы, билеты, логи) от old_id → new_id.
        account-first: сначала account_id, затем fallback по user_id.
        """
        try:
            old_account_id = self._get_account_id_for_discord_user(old_id)
            new_account_id = self._get_account_id_for_discord_user(new_id)

            scores_has_account = self._table_supports_account_id("scores")
            actions_has_account = self._table_supports_account_id("actions")
            ticket_actions_has_account = self._table_supports_account_id("ticket_actions")

            def _fetch_scores_row_by_identity(account_id: Optional[str], user_id: int) -> Optional[dict]:
                row = None
                if account_id and scores_has_account:
                    response = (
                        self.supabase.table("scores")
                        .select("*")
                        .eq("account_id", account_id)
                        .limit(1)
                        .execute()
                    )
                    if response.data:
                        row = response.data[0]
                if row is None:
                    response = (
                        self.supabase.table("scores")
                        .select("*")
                        .eq("user_id", user_id)
                        .limit(1)
                        .execute()
                    )
                    if response.data:
                        row = response.data[0]
                return row

            old_score = _fetch_scores_row_by_identity(old_account_id, old_id)
            new_score = _fetch_scores_row_by_identity(new_account_id, new_id)

            if old_score:
                merged_points = float((new_score or {}).get("points", 0)) + float(old_score.get("points", 0))
                merged_tickets_normal = int((new_score or {}).get("tickets_normal", 0) or 0) + int(old_score.get("tickets_normal", 0) or 0)
                merged_tickets_gold = int((new_score or {}).get("tickets_gold", 0) or 0) + int(old_score.get("tickets_gold", 0) or 0)

                upsert_payload = {
                    "user_id": new_id,
                    "points": merged_points,
                    "tickets_normal": merged_tickets_normal,
                    "tickets_gold": merged_tickets_gold,
                }
                if new_account_id and scores_has_account:
                    upsert_payload["account_id"] = new_account_id

                self.supabase.table("scores").upsert(upsert_payload).execute()

                if old_account_id and scores_has_account:
                    self.supabase.table("scores").delete().eq("account_id", old_account_id).execute()
                self.supabase.table("scores").delete().eq("user_id", old_id).execute()

            action_update = {"user_id": new_id}
            ticket_update = {"user_id": new_id}

            if new_account_id and actions_has_account:
                action_update["account_id"] = new_account_id
            if new_account_id and ticket_actions_has_account:
                ticket_update["account_id"] = new_account_id

            if old_account_id and actions_has_account:
                self.supabase.table("actions").update(action_update).eq("account_id", old_account_id).execute()
                self.supabase.table("actions").update(action_update).eq("user_id", old_id).is_("account_id", "null").execute()
            else:
                self.supabase.table("actions").update(action_update).eq("user_id", old_id).execute()

            if old_account_id and ticket_actions_has_account:
                self.supabase.table("ticket_actions").update(ticket_update).eq("account_id", old_account_id).execute()
                self.supabase.table("ticket_actions").update(ticket_update).eq("user_id", old_id).is_("account_id", "null").execute()
            else:
                self.supabase.table("ticket_actions").update(ticket_update).eq("user_id", old_id).execute()

            self.load_data()
            return True
        except Exception as e:
            logger.error(f"Ошибка переноса пользователя: {e}")
            return False

# Глобальный экземпляр
db = Database()
