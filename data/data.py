import os
from datetime import datetime, timezone
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

    def _ensure_tables(self):
        """Проверяет существование обязательных таблиц"""
        if not self.supabase:
            return

        try:
            # Проверка существования таблицы scores
            self.supabase.table("scores").select("user_id").limit(1).execute()
        except Exception as e:
            raise RuntimeError(f"Таблица scores не существует или недоступна: {str(e)}")

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


# Глобальный экземпляр
db = Database()
