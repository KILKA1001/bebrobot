import json
import os
from supabase import create_client, Client

# Учетные данные для Supabase
url: str = os.getenv("SUPABASE_URL")
key: str = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(url, key)

# Глобальные переменные для хранения баллов и истории
scores = {}
history = {}

def load_data():
    global scores, history
    try:
        # Загружаем данные из Supabase
        scores_data = supabase.table("scores").select("*").execute()
        history_data = supabase.table("history").select("*").execute()
        
        # Преобразуем данные о баллах
        scores = {int(item['user_id']): float(item['points']) for item in scores_data.data}
        
        # Преобразуем данные истории
        history = {int(item['user_id']): item['history_entries'] for item in history_data.data}
        
    except Exception as e:
        print(f"Ошибка при загрузке из Supabase: {e}")
        # Резервная загрузка из JSON файлов
        if os.path.exists("scores.json"):
            with open("scores.json", "r") as f:
                scores = {int(k): float(v) for k, v in json.load(f).items()}
        
        if os.path.exists("history.json"):
            with open("history.json", "r") as f:
                history = json.load(f)

def save_data():
    try:
        # Сохраняем баллы в Supabase
        for user_id, points in scores.items():
            supabase.table("scores").upsert({
                "user_id": user_id,
                "points": points
            }).execute()
        
        # Сохраняем историю в Supabase
        for user_id, entries in history.items():
            supabase.table("history").upsert({
                "user_id": user_id,
                "history_entries": entries
            }).execute()
            
    except Exception as e:
        print(f"Ошибка при сохранении в Supabase: {e}")
        # Резервное сохранение в JSON файлы
        with open("scores.json", "w") as f:
            json.dump(scores, f)
        
        with open("history.json", "w") as f:
            json.dump(history, f)
