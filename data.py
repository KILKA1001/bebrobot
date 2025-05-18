from supabase import create_client
import os
from datetime import datetime
from dotenv import load_dotenv
from typing import Dict, List, Any, Optional

load_dotenv()

# Инициализация Supabase клиента
if not os.getenv('SUPABASE_URL') or not os.getenv('SUPABASE_KEY'):
    raise ValueError("Missing Supabase credentials in environment variables")

supabase = create_client(
    os.getenv('SUPABASE_URL'),
    os.getenv('SUPABASE_KEY')
)

scores: Dict[int, float] = {}
history: Dict[int, List[Dict[str, Any]]] = {}

async def save_data() -> None:
    """Сохраняет все изменения в базу данных"""
    try:
        # Обновляем или создаем записи баллов
        for user_id, score in scores.items():
            supabase.table('points').upsert({
                'user_id': int(user_id),
                'score': float(score)
            }).execute()

        # Сохраняем историю
        for user_id, user_history in history.items():
            for entry in user_history:
                if isinstance(entry, dict):  # Новый формат
                    insert_data = {
                        'user_id': int(user_id),
                        'points': float(entry['points']),
                        'reason': str(entry['reason']),
                        'timestamp': entry.get('timestamp', datetime.now().isoformat())
                    }
                    if entry.get('author_id') is not None:
                        insert_data['author_id'] = int(entry['author_id'])
                    else:
                        insert_data['author_id'] = 0
                else:  # Старый формат (tuple)
                    points, reason = entry
                    insert_data = {
                        'user_id': int(user_id),
                        'points': float(points),
                        'reason': str(reason),
                        'timestamp': datetime.now().isoformat(),
                        'author_id': 0
                    }

                supabase.table('history').insert(insert_data).execute()
    except Exception as e:
        print(f"Ошибка при сохранении данных: {e}")
        raise

async def load_data() -> None:
    """Загружает данные из базы данных"""
    global scores, history
    try:
        # Загружаем баллы
        points_response = supabase.table('points').select('*').execute()
        if points_response.data:
            for record in points_response.data:
                scores[int(record['user_id'])] = float(record['score'])

        # Загружаем историю
        history_response = supabase.table('history').select('*').execute()
        if history_response.data:
            for record in history_response.data:
                user_id = int(record['user_id'])
                if user_id not in history:
                    history[user_id] = []
                history[user_id].append({
                    'points': float(record['points']),
                    'reason': str(record['reason']),
                    'author_id': int(record['author_id']) if record.get('author_id') else None,
                    'timestamp': str(record['timestamp'])
                })
    except Exception as e:
        print(f"Ошибка при загрузке данных: {e}")
        scores = {}
        history = {}
        raise

async def add_points(user_id: int, points: float, reason: str, author_id: Optional[int] = None) -> bool:
    """Добавляет баллы пользователю"""
    try:
        # Обновляем баллы
        current_score = scores.get(user_id, 0)
        new_score = current_score + points
        scores[user_id] = new_score

        # Добавляем запись в историю
        timestamp = datetime.now().isoformat()
        history_entry = {
            'points': points,
            'reason': reason,
            'author_id': author_id,
            'timestamp': timestamp
        }

        if user_id not in history:
            history[user_id] = []
        history[user_id].append(history_entry)

        # Сохраняем изменения в базу
        await save_data()
        return True
    except Exception as e:
        print(f"Ошибка при добавлении баллов: {e}")
        return False

async def remove_points(user_id: int, points: float, reason: str, author_id: Optional[int] = None) -> bool:
    """Удаляет баллы у пользователя"""
    return await add_points(user_id, -points, reason, author_id)
