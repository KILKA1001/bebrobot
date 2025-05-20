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
    global scores, actions
    current_dir = os.path.dirname(os.path.abspath(__file__))
    scores_path = os.path.join(current_dir, "scores.json")
    actions_path = os.path.join(current_dir, "actions.json")

    print("Загрузка данных...")
    print(f"Текущая директория: {current_dir}")

    try:
        if url and key:
            scores_data = supabase.table("scores").select("*").execute()
            actions_data = supabase.table("actions").select("*").execute()

            scores = {
                int(item['user_id']): float(item['points'])
                for item in scores_data.data
            }
            actions = actions_data.data
            print("Данные успешно загружены из Supabase")
        else:
            raise Exception("Нет учетных данных Supabase")

    except Exception as e:
        print(f"Загрузка из Supabase не удалась: {e}")
        print("Пробуем загрузить из локальных файлов...")

        try:
            if os.path.exists(scores_path):
                with open(scores_path, "r", encoding='utf-8') as f:
                    scores = {
                        int(k): float(v)
                        for k, v in json.load(f).items()
                    }
                print(f"Загружены баллы: {len(scores)} записей")

            if os.path.exists(actions_path):
                with open(actions_path, "r", encoding='utf-8') as f:
                    actions = json.load(f)
                print(f"Загружены действия: {len(actions)} записей")

        except Exception as load_error:
            print(f"Ошибка при загрузке локальных файлов: {load_error}")


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
    try:
        if actions:
            supabase.table("actions").upsert(actions).execute()

        scores_data = [{
            "user_id": user_id,
            "points": points
        } for user_id, points in scores.items()]
        if scores_data:
            supabase.table("scores").upsert(scores_data).execute()

    except Exception as e:
        print(f"Ошибка при сохранении в Supabase: {e}")
        try:
            with open("scores.json", "w", encoding='utf-8') as f:
                json.dump(scores, f, ensure_ascii=False, indent=2)

            with open("actions.json", "w", encoding='utf-8') as f:
                json.dump(actions, f, ensure_ascii=False, indent=2)
            print("Данные успешно сохранены локально")
        except Exception as e:
            print(f"Ошибка при локальном сохранении: {e}")
