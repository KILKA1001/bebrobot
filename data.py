import json  # Импортируем модуль для работы с JSON
import os  # Импортируем модуль для работы с операционной системой
from supabase import create_client, Client  # Импортируем клиента Supabase

# Учетные данные для Supabase
url: str = os.getenv("SUPABASE_URL")  # Получаем URL Supabase из окружения
key: str = os.getenv("SUPABASE_KEY")  # Получаем ключ Supabase из окружения
supabase: Client = create_client(url, key)  # Создаем клиента Supabase

# Глобальные переменные для хранения баллов и истории
scores = {}
history = {}

# Функции для работы с данными о баллах и истории
def load_data():
    global scores, history  # Объявляем глобальные переменные
    # Загружаем данные о баллах
    scores_data = supabase.table("scores").select("*").execute()
    # Загружаем данные об истории
    history_data = supabase.table("history").select("*").execute()

    # Assuming data is stored in JSON files
    if os.path.exists("scores.json"):
        with open("scores.json", "r") as f:
            scores = json.load(f)

    if os.path.exists("history.json"):
        with open("history.json", "r") as f:
            history = json.load(f)
            print("Loaded history:", history)  # Debugging output
    
    # Преобразуем данные о баллах в словарь
    scores = {int(item['user_id']): float(item['points']) for item in scores_data.data}
    # Преобразуем данные об истории в словарь
    history = {
        int(item['user_id']): item['history_entries'] for item in history_data.data
    }

def save_data():
    # Сохраняем данные о баллах
    for user_id, points in scores.items():
        supabase.table("scores").upsert({"user_id": user_id, "points": points}).execute()

    # Сохраняем данные об истории
    for user_id, entries in history.items():
        for entry in entries:
            supabase.table("history").upsert({"user_id": user_id, "history_entries": entry}).execute()
