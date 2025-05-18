import json
import os

DATA_FILE = 'scores.json'
HISTORY_FILE = 'history.json'

# Начальные данные
scores = {}
history = {}

def load_data():
    global scores, history
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, 'r') as f:
            scores = {int(k): float(v) for k, v in json.load(f).items()}
    else:
        scores = {}

    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, 'r') as f:
            loaded = json.load(f)
            # Проверка на формат: кортеж или словарь
            def parse_entry(entry):
                if isinstance(entry, dict):
                    return entry
                elif isinstance(entry, list) and len(entry) == 2:
                    return {
                        'points': float(entry[0]),
                        'reason': str(entry[1]),
                        'author_id': None,
                        'timestamp': None
                    }
                return {}
            history = {int(k): [parse_entry(e) for e in v] for k, v in loaded.items()}
    else:
        history = {}

def save_data():
    with open(DATA_FILE, 'w') as f:
        json.dump({str(k): v for k, v in scores.items()}, f)
    with open(HISTORY_FILE, 'w') as f:
        json.dump({str(k): v for k, v in history.items()}, f)
