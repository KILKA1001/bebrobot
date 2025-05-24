import os
from datetime import datetime, timezone, timedelta
from supabase import create_client
from dotenv import load_dotenv
import traceback

class Database:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.init_db()
        return cls._instance

    def init_db(self):
        load_dotenv()
        self.url = os.getenv("SUPABASE_URL")
        self.key = os.getenv("SUPABASE_KEY")
        self.supabase = create_client(self.url, self.key) if self.url and self.key else None
        self._ensure_tables()
        self.load_data()
        self.load_fines()
        
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
        except Exception as e:
            raise RuntimeError(f"Таблица fines не существует или недоступна: {str(e)}")

        try:
            self.supabase.table("fine_payments").select("id").limit(1).execute()
        except Exception as e:
            raise RuntimeError(f"Таблица fine_payments не существует или недоступна: {str(e)}")

    def load_data(self):
        """Загружает все данные с автоматическим восстановлением связей"""
        print("⚙️ Синхронизация с Supabase...")
        try:
            if not self.supabase:
                raise ConnectionError("Supabase: нет подключения")

            # 1. Загружаем баллы
            scores_response = self.supabase.from_('scores').select('*').execute()
            if hasattr(scores_response, 'data'):
                self.scores = {int(item['user_id']): float(item['points']) for item in scores_response.data}
            else:
                raise ValueError("Некорректный ответ от Supabase при загрузке баллов")

            # 2. Загружаем действия
            actions_response = self.supabase.from_('actions')\
                .select('*')\
                .order('timestamp', desc=True)\
                .execute()

            if hasattr(actions_response, 'data'):
                self.actions = actions_response.data
                self._build_history()
            else:
                raise ValueError("Некорректный ответ от Supabase при загрузке действий")

            print(f"✅ Данные синхронизированы | Пользователей: {len(self.scores)}")

        except Exception as e:
            print(f"❌ Ошибка синхронизации: {str(e)}")
            traceback.print_exc()
            self.scores = {}
            self.actions = []
            self.history = {}

    def _build_history(self):
        """Строит историю действий"""
        self.history = {}
        for action in self.actions:
            user_id = int(action['user_id'])
            if user_id not in self.history:
                self.history[user_id] = []
            self.history[user_id].append({
                'points': float(action['points']),
                'reason': action['reason'],
                'author_id': int(action['author_id']),
                'timestamp': action['timestamp']
            })

    def update_scores(self, user_id: int, points_change: float):
        """Атомарное обновление баллов с проверкой"""
        if not self.supabase:
            return False

        try:
            # 1. Получаем текущие баллы
            try:
                current = self.supabase.table("scores")\
                    .select("points")\
                    .eq("user_id", user_id)\
                    .execute()
                current_points = float(current.data[0]['points']) if current.data else 0
            except Exception:
                current_points = 0

            new_points = max(current_points + points_change, 0)  # Не уходим в минус

            # 2. Обновляем баллы через upsert
            result = self.supabase.table("scores")\
                .upsert({
                    "user_id": user_id,
                    "points": new_points
                })\
                .execute()

            if result:
                # Обновляем локальный кеш
                self.scores[user_id] = new_points
                return True
        except Exception as e:
            print(f"🔥 Ошибка обновления баллов: {str(e)}")
            traceback.print_exc()
            return False

    def add_action(self, user_id: int, points: float, reason: str, author_id: int, is_undo: bool = False):
        """Добавляет действие с гарантированной синхронизацией"""
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
            # Добавляем is_undo только если True
            if is_undo:
                action["is_undo"] = True
                
            # 3. Сохраняем действие
            if not self.supabase:
                print("Supabase client is not initialized.")
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

            print(f"✅ Действие сохранено (ID: {response.data[0]['id']})")
            return True

        except Exception as e:
            print(f"❌ Ошибка добавления действия: {str(e)}")
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
                print("⚠️ Supabase не инициализирован")
                return
                
            if self.scores:
                scores_data = [{"user_id": k, "points": v} for k, v in self.scores.items()]
                response = self._handle_response(
                    self.supabase.table("scores").upsert(scores_data).execute()
                )
                if response:
                    print(f"💾 Данные сохранены: {len(response.data if response.data else [])} записей")
        except Exception as e:
            print(f"🔥 Ошибка сохранения: {str(e)}")
            traceback.print_exc()

    class Database:
        pass
    def log_monthly_top(self, entries: list):
        """Запись топа месяца в Supabase"""
        if not self.supabase:
            print("Supabase не инициализирован для логирования топа")
            return False

        now = datetime.now()
        month = now.month
        year = now.year

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
            print("✅ Лог топа месяца записан")
            return True
        except Exception as e:
            print(f"❌ Ошибка записи топа месяца: {e}")
            return False

#Штрафы
    def load_fines(self):
        """Загружает штрафы и оплаты из Supabase"""
        if not self.supabase:
            self.fines = []
            self.fine_payments = []
            return

        try:
            fines_resp = self.supabase.table("fines").select("*").execute()
            payments_resp = self.supabase.table("fine_payments").select("*").execute()

            self.fines = fines_resp.data if hasattr(fines_resp, "data") else []
            self.fine_payments = payments_resp.data if hasattr(payments_resp, "data") else []

            print(f"✅ Загружено штрафов: {len(self.fines)}, оплат: {len(self.fine_payments)}")

        except Exception as e:
            print(f"❌ Ошибка при загрузке штрафов: {str(e)}")
            traceback.print_exc()
            self.fines = []
            self.fine_payments = []

    def add_fine(self, user_id: int, author_id: int, amount: float, fine_type: int, reason: str, due_date: datetime):
        """Создаёт штраф"""
        if not self.supabase:
            print("⚠️ Supabase не инициализирован")
            return None

        try:
            result = self.supabase.table("fines").insert({
                "user_id": user_id,
                "author_id": author_id,
                "amount": amount,
                "type": fine_type,
                "reason": reason,
                "due_date": due_date.isoformat()
            }).execute()

            if not result.data:
                raise ValueError("❌ Пустой ответ от Supabase при создании штрафа")

            fine = result.data[0]
            self.fines.insert(0, fine)
            return fine

        except Exception as e:
            print(f"❌ Ошибка добавления штрафа: {str(e)}")
            traceback.print_exc()
            return None

    def get_user_fines(self, user_id: int, active_only: bool = True):
        """Возвращает список штрафов пользователя"""
        return [
            fine for fine in self.fines
            if fine["user_id"] == user_id
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
            print(f"Ошибка чтения баланса банка: {str(e)}")
        return 0.0

    def add_to_bank(self, amount: float):
        try:
            if not self.supabase:
                print("❌ Supabase не инициализирован")
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
            print(f"Ошибка обновления банка: {str(e)}")
            return False

    def record_payment(self, user_id: int, fine_id: int, amount: float, author_id: int) -> bool:
        """Записывает оплату штрафа, обновляет банк, баллы, штраф"""
        try:
            if not self.supabase:
                print("❌ Supabase не инициализирован")
                return False

            # 1. Обновляем баллы пользователя
            if not self.update_scores(user_id, -amount):
                return False

            # 2. Добавляем запись в fine_payments
            self.supabase.table("fine_payments").insert({
                "fine_id": fine_id,
                "user_id": user_id,
                "amount": amount,
                "author_id": author_id
            }).execute()

            # 3. Лог действия
            self.add_action(user_id, -amount, f"Оплата штрафа ID #{fine_id}", author_id)

            # 4. Обновление баланса банка
            self.add_to_bank(amount)

            # 5. Обновляем данные по штрафу
            fine = self.get_fine_by_id(fine_id)
            if fine:
                fine['paid_amount'] = round(fine.get('paid_amount', 0) + amount, 2)
                fine['is_paid'] = fine['paid_amount'] >= fine['amount']

                self.supabase.table("fines").update({
                    "paid_amount": fine['paid_amount'],
                    "is_paid": fine['is_paid']
                }).eq("id", fine_id).execute()

            return True

        except Exception as e:
            print(f"❌ Ошибка при записи оплаты: {e}")
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
            print(f"❌ Ошибка при отсрочке штрафа: {e}")
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
            print(f"❌ Ошибка при отметке просрочки штрафа: {e}")
            traceback.print_exc()
            return False

# Глобальный экземпляр
db = Database()
