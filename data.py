import os
from datetime import datetime, timezone
from supabase import create_client
from dotenv import load_dotenv
import traceback

class Database:
    _instance = None

    def __new__(cls):
        load_dotenv()
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.init_db()
        return cls._instance

    def init_db(self):
        self.url = os.getenv("SUPABASE_URL")
        self.key = os.getenv("SUPABASE_KEY")
        self.supabase = create_client(self.url, self.key) if self.url and self.key else None
        self.scores = {}
        self.history = {}
        self.actions = []
        self.load_data()

    def _handle_response(self, response):
        """Универсальная обработка ответа Supabase"""
        if isinstance(response, Exception):
            print(f"⚠️ Ошибка Supabase: {str(response)}")
            return None
        return response

    def load_data(self):
        print("⚙️ Загрузка данных из Supabase...")
        try:
            if not self.supabase:
                raise ConnectionError("Supabase не подключен")

            # Загрузка scores
            response = self._handle_response(
                self.supabase.table("scores").select("*").execute()
            )
            self.scores = {int(item['user_id']): float(item['points']) for item in response.data} if response else {}

            # Загрузка actions
            response = self._handle_response(
                self.supabase.table("actions").select("*").execute()
            )
            if response:
                self.actions = response.data
                self._build_history()
            else:
                self.actions = []
                self.history = {}

            print(f"✅ Данные загружены: {len(self.scores)} scores, {len(self.actions)} actions")

        except Exception as e:
            print(f"❌ Критическая ошибка загрузки: {str(e)}")
            traceback.print_exc()
            self.scores = {}
            self.actions = []
            self.history = {}

    def _build_history(self):
        """Построение истории из actions"""
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

    def add_action(self, user_id: int, points: float, reason: str, author_id: int):
        try:
            action = {
                "user_id": user_id,
                "points": points,
                "reason": reason,
                "author_id": author_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "action_type": "remove" if points < 0 else "add"
            }

            # Локальное кеширование
            self.actions.append(action)
            if user_id not in self.history:
                self.history[user_id] = []
            self.history[user_id].append(action)

            # Сохранение в БД
            if self.supabase:
                response = self._handle_response(
                    self.supabase.table("actions").insert(action).execute()
                )
                if response:
                    print(f"✅ Действие сохранено (ID: {response.data[0]['id'] if response.data else 'N/A'})")

        except Exception as e:
            print(f"🔥 Критическая ошибка в add_action(): {str(e)}")
            traceback.print_exc()

    def save_all(self):
        try:
            if self.scores and self.supabase:
                scores_data = [{"user_id": k, "points": v} for k, v in self.scores.items()]
                response = self._handle_response(
                    self.supabase.table("scores").upsert(scores_data).execute()
                )
                if response:
                    print(f"💾 Данные сохранены: {len(response.data if response.data else [])} записей")
        except Exception as e:
            print(f"🔥 Ошибка сохранения: {str(e)}")
            traceback.print_exc()

# Глобальный экземпляр
db = Database()
