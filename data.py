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
        print("❌ Ошибка: отсутствует подключение к Supabase")
        return

    print("Сохранение данных в Supabase...")
    try:
        # Сохраняем баллы (оставляем без изменений)
        if scores:
            scores_data = [{"user_id": user_id, "points": points} for user_id, points in scores.items()]
            supabase.table("scores").upsert(scores_data).execute()

        # Сохраняем действия - ВАЖНОЕ ИЗМЕНЕНИЕ:
        if actions:
            for action in actions:
                # Для каждого действия делаем отдельный insert
                action_data = {
                    "user_id": int(action["user_id"]),
                    "points": float(action["points"]),
                    "reason": str(action["reason"]),
                    "author_id": int(action["author_id"]),
                    "action_type": str(action.get("action_type", "add"))
                }
                # Используем insert вместо upsert
                response = supabase.table("actions").insert(action_data).execute()
                print(f"Сохранено действие: {response}")

    except Exception as e:
        print(f"❌ Ошибка сохранения: {str(e)}")
        import traceback
        traceback.print_exc()

# Добавьте в конец data.py
if __name__ == "__main__":
    test_data = {
        "user_id": 999999999,
        "points": 99.9,
        "reason": "TEST DIRECT INSERT",
        "author_id": 888888888,
        "action_type": "test"
    }
    response = supabase.table("actions").insert(test_data).execute()
    print("Тестовый запрос:", response)
    print("Проверьте появилась ли запись в БД")
