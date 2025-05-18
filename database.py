
from supabase import create_client
import os
from datetime import datetime
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv

class DatabaseHandler:
    def __init__(self):
        load_dotenv()
        if not os.getenv('SUPABASE_URL') or not os.getenv('SUPABASE_KEY'):
            raise ValueError("Missing Supabase credentials")
            
        self.supabase = create_client(
            os.getenv('SUPABASE_URL'),
            os.getenv('SUPABASE_KEY')
        )
        self.scores: Dict[int, float] = {}
        self.history: Dict[int, List[Dict[str, Any]]] = {}

    def save_points(self, user_id: int, score: float) -> Optional[Dict]:
        """Сохраняет баллы пользователя"""
        try:
            response = self.supabase.table('points').upsert({
                'user_id': str(user_id),
                'score': float(score)
            }).execute()
            print(f"Баллы сохранены для пользователя {user_id}")
            self.scores[user_id] = float(score)
            return response.data[0] if response and response.data else None
        except Exception as e:
            print(f"Ошибка при сохранении баллов: {e}")
            return None

    def save_history_entry(self, entry: Dict[str, Any]) -> Optional[Dict]:
        """Сохраняет запись в историю"""
        try:
            entry_copy = entry.copy()
            user_id = entry_copy.get('user_id')
            if user_id is None:
                print("Ошибка: отсутствует user_id в записи")
                return None

            entry_copy['user_id'] = str(user_id)
            response = self.supabase.table('history').insert(entry_copy).execute()
            user_id = int(entry_copy['user_id'])

            if user_id not in self.history:
                self.history[user_id] = []
            self.history[user_id].append(entry_copy)
            print(f"Запись в историю сохранена для пользователя {user_id}")
            return response.data[0] if response and response.data else None
        except Exception as e:
            print(f"Ошибка при сохранении истории: {e}")
            return None

    def load_data(self) -> None:
        """Загружает все данные из базы"""
        try:
            # Загружаем баллы
            points_response = self.supabase.table('points').select('*').execute()
            if not points_response or not points_response.data:
                print("Предупреждение: Данные о баллах не найдены")
                return
            self.scores = {
                int(record['user_id']): float(record['score'])
                for record in points_response.data
            }

            # Загружаем историю
            history_response = self.supabase.table('history').select('*').order('timestamp').execute()
            self.history.clear()
            
            for record in history_response.data:
                user_id = int(record['user_id'])
                if user_id not in self.history:
                    self.history[user_id] = []
                    
                self.history[user_id].append({
                    'points': float(record['points']),
                    'reason': str(record['reason']),
                    'author_id': int(record['author_id']) if record.get('author_id') else None,
                    'timestamp': str(record['timestamp'])
                })
                
        except Exception as e:
            print(f"Error loading data: {e}")
            raise

    def get_user_score(self, user_id: int) -> float:
        """Получает баллы пользователя"""
        return self.scores.get(user_id, 0)

    def get_user_history(self, user_id: int) -> List[Dict[str, Any]]:
        """Получает историю пользователя"""
        return self.history.get(user_id, [])

# Создаем глобальный экземпляр
db = DatabaseHandler()
