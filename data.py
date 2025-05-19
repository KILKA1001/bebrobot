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
    current_dir = os.path.dirname(os.path.abspath(__file__))
    scores_path = os.path.join(current_dir, "scores.json")
    history_path = os.path.join(current_dir, "history.json")
    
    print("Загрузка данных...")
    print(f"Текущая директория: {current_dir}")
    
    try:
        # Пробуем загрузить из Supabase только если есть учетные данные
        if url and key:
            scores_data = supabase.table("scores").select("*").execute()
            history_data = supabase.table("history").select("*").execute()
            
            scores = {int(item['user_id']): float(item['points']) for item in scores_data.data}
            history = {int(item['user_id']): item['history_entries'] for item in history_data.data}
            print("Данные успешно загружены из Supabase")
        else:
            raise Exception("Нет учетных данных Supabase")
            
    except Exception as e:
        print(f"Загрузка из Supabase не удалась: {e}")
        print("Пробуем загрузить из локальных файлов...")
        
        try:
            if os.path.exists(scores_path):
                with open(scores_path, "r", encoding='utf-8') as f:
                    scores = {int(k): float(v) for k, v in json.load(f).items()}
                print(f"Загружены баллы: {len(scores)} записей")
            
            if os.path.exists(history_path):
                with open(history_path, "r", encoding='utf-8') as f:
                    history = json.load(f)
                print(f"Загружена история: {len(history)} пользователей")
            
        except Exception as load_error:
            print(f"Ошибка при загрузке локальных файлов: {load_error}")

def save_data():
    try:
        # Сохраняем баллы в Supabase
        scores_data = [{"user_id": user_id, "points": points} for user_id, points in scores.items()]
        if scores_data:
            supabase.table("scores").upsert(scores_data).execute()
        
        # Сохраняем историю в Supabase
        history_data = [{"user_id": user_id, "history_entries": entries} for user_id, entries in history.items()]
        if history_data:
            supabase.table("history").upsert(history_data).execute()
            
    except Exception as e:
        print(f"Ошибка при сохранении в Supabase: {e}")
        try:
            # Резервное сохранение в JSON файлы
            with open("scores.json", "w", encoding='utf-8') as f:
                json.dump(scores, f, ensure_ascii=False, indent=2)
            
            with open("history.json", "w", encoding='utf-8') as f:
                json.dump(history, f, ensure_ascii=False, indent=2)
            print("Данные успешно сохранены локально")
        except Exception as e:
            print(f"Ошибка при локальном сохранении: {e}")
