import json
import os
from datetime import datetime
from supabase import create_client, Client

# Учетные данные для Supabase
url: str = os.getenv("SUPABASE_URL")
key: str = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(url, key)

# Глобальные переменные для хранения баллов и действий
scores = {}
history = {}  # История действий по пользователям
actions = []  # Список всех действий


def load_data():
    global scores, actions, history
    print("Загрузка данных из Supabase...")
    
    try:
        if not url or not key:
            raise Exception("Отсутствуют учетные данные Supabase")
            
        # Загружаем баллы
        scores_response = supabase.table("scores").select("*").execute()
        print(f"Получен ответ от scores: {scores_response}")
        
        if hasattr(scores_response, 'data'):
            scores.clear()  # Очищаем текущие данные
            for item in scores_response.data:
                user_id = int(item['user_id'])
                points = float(item['points'])
                scores[user_id] = points
            print(f"Загружены баллы: {len(scores)} записей")
            
        # Загружаем действия
        actions_response = supabase.table("actions").select("*").execute()
        print(f"Получен ответ от actions: {actions_response}")
        
        if hasattr(actions_response, 'data'):
            actions.clear()  # Очищаем текущие данные
            actions.extend(actions_response.data)
            print(f"Загружены действия: {len(actions)} записей")
            
            # Формируем историю из действий
            history.clear()  # Очищаем текущие данные
            for action in actions:
                user_id = int(action['user_id'])
                if user_id not in history:
                    history[user_id] = []
                history[user_id].append({
                    'points': float(action['points']),
                    'reason': action['reason'],
                    'author_id': int(action['author_id']),
                    'timestamp': action['timestamp']
                })
            print(f"Сформирована история для {len(history)} пользователей")
                
    except Exception as e:
        print(f"Ошибка при загрузке из Supabase: {e}")
        # Инициализируем пустые структуры данных
        scores = {}
        actions = []
        history = {}


def add_action(user_id: int,
               points: float,
               reason: str,
               author_id: int,
               action_type: str = "add"):
    """Добавить новое действие в историю"""
    action = {
        "id": len(actions) + 1,
        "user_id": user_id,
        "points": points,
        "reason": reason,
        "author_id": author_id,
        "timestamp": datetime.now().strftime("%H:%M %d-%m-%Y"),
        "action_type": action_type
    }
    actions.append(action)
    save_data()


def get_user_actions(user_id: int, page: int = 1, per_page: int = 5):
    """Получить действия пользователя с пагинацией"""
    user_actions = [a for a in actions if a["user_id"] == user_id]
    start_idx = (page - 1) * per_page
    end_idx = start_idx + per_page
    return user_actions[start_idx:end_idx], len(user_actions)


def save_data():
    print("Сохранение данных в Supabase...")
    try:
        # Сохраняем баллы
        if scores:
            scores_data = [
                {
                    "user_id": int(user_id),
                    "points": float(points)
                }
                for user_id, points in scores.items()
            ]
            scores_response = supabase.table("scores").upsert(scores_data).execute()
            print(f"Баллы сохранены: {len(scores_data)} записей")
            print(f"Ответ scores: {scores_response}")

        # Сохраняем действия
        if actions:
            # Убедимся что все данные правильного типа
            for action in actions:
                action['user_id'] = int(action['user_id'])
                action['points'] = float(action['points'])
                action['author_id'] = int(action['author_id'])
            
            actions_response = supabase.table("actions").upsert(actions).execute()
            print(f"Действия сохранены: {len(actions)} записей")
            print(f"Ответ actions: {actions_response}")

    except Exception as e:
        print(f"❌ Ошибка при сохранении в Supabase: {e}")
        print("Проверьте подключение к базе данных и правильность учетных данных")
