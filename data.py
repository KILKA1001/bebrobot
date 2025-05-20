import json
import os
from datetime import datetime
from supabase import create_client, Client

# Учетные данные для Supabase
url: str = os.getenv("SUPABASE_URL")
key: str = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(url, key) if url and key else None

# Глобальные переменные для хранения баллов и действий
scores = {}
history = {}  # История действий по пользователям
actions = []  # Список всех действий

def load_data():
    global scores, actions, history
    print("Загрузка данных из Supabase...")
    
    try:
        if not supabase:
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
        scores.clear()
        actions.clear()
        history.clear()

def add_action(user_id: int, points: float, reason: str, author_id: int, action_type: str = "add"):
    """Добавить новое действие в историю"""
    timestamp = datetime.now().strftime("%H:%M %d-%m-%Y")
    action = {
        "user_id": user_id,
        "points": points,
        "reason": reason,
        "author_id": author_id,
        "timestamp": timestamp,
        "action_type": action_type
    }
    actions.append(action)
    
    # Обновляем историю
    if user_id not in history:
        history[user_id] = []
    history[user_id].append(action)
    
    print(f"Добавлено действие в кеш: {action}")
    save_data()

def get_user_actions(user_id: int, page: int = 1, per_page: int = 5):
    """Получить действия пользователя с пагинацией"""
    user_actions = history.get(user_id, [])
    total_actions = len(user_actions)
    start_idx = (page - 1) * per_page
    end_idx = start_idx + per_page
    return user_actions[start_idx:end_idx], total_actions

def save_data():
    if not supabase:
        print("Ошибка: отсутствует подключение к Supabase")
        return
        
    print("Сохранение данных в Supabase...")
    try:
        # Сохраняем баллы
        if scores:
            scores_data = [
                {"user_id": user_id, "points": points}
                for user_id, points in scores.items()
            ]
            supabase.table("scores").upsert(scores_data).execute()
            print(f"Баллы сохранены: {len(scores_data)} записей")

        # Сохраняем действия
        if actions:
            actions_data = []
            for action in actions:
                action_copy = action.copy()
                # Убираем id если он есть
                action_copy.pop('id', None)
                actions_data.append(action_copy)
                
            supabase.table("actions").upsert(actions_data).execute()
            print(f"Действия сохранены: {len(actions_data)} записей")

    except Exception as e:
        print(f"Ошибка при сохранении в Supabase: {e}")
