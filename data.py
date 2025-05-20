import os
from datetime import datetime, timezone
from supabase import create_client, Client

class Database:
    _instance = None

    def __new__(cls):
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

    def load_data(self):
        print("⚙️ Загрузка данных из Supabase...")
        try:
            # Загрузка scores
            res = self.supabase.table("scores").select("*").execute()
            self.scores = {int(item['user_id']): float(item['points']) for item in res.data}

            # Загрузка actions и построение history
            res = self.supabase.table("actions").select("*").execute()
            self.actions = res.data
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

            print(f"✅ Данные загружены: {len(self.scores)} scores, {len(self.actions)} actions")

        except Exception as e:
            print(f"❌ Ошибка загрузки: {e}")
            self.scores = {}
            self.actions = []
            self.history = {}

    def add_action(self, user_id: int, points: float, reason: str, author_id: int):
        action = {
            "user_id": user_id,
            "points": points,
            "reason": reason,
            "author_id": author_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "action_type": "add"
        }

        # Локальное кеширование
        self.actions.append(action)
        if user_id not in self.history:
            self.history[user_id] = []
        self.history[user_id].append(action)

        # Немедленное сохранение в БД
        try:
            response = self.supabase.table("actions").insert(action).execute()
            print(f"💾 Дейтие сохранено: {action['points']} баллов для {user_id}")
        except Exception as e:
            print(f"🔥 Ошибка сохранения действия: {e}")

    def save_all(self):
        try:
            # Сохранение scores
            if self.scores:
                scores_data = [{"user_id": k, "points": v} for k, v in self.scores.items()]
                self.supabase.table("scores").upsert(scores_data).execute()

            print(f"💾 Все данные сохранены: {len(self.scores)} scores")
        except Exception as e:
            print(f"🔥 Ошибка сохранения: {e}")

# Глобальный экземпляр
db = Database()
