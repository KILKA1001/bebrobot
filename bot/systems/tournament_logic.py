import random
from typing import List, Dict, Optional


class Match:
    """
    Описывает поединок между двумя участниками.
    mode: игровой режим
    map_id: идентификатор карты (формат 'режим.порядковый_в_режиме общее_положение')
    result: 1 если выиграл player1, 2 если player2, None пока нет результата
    """
    def __init__(self, player1_id: int, player2_id: int, mode: str, map_id: str):
        self.player1_id = player1_id
        self.player2_id = player2_id
        self.mode = mode
        self.map_id = map_id
        self.result: Optional[int] = None


class Tournament:
    """
    Класс для управления турнирной сеткой.
    Каждый раунд генерируется автоматически случайным распределением участников по парам и картам.
    """
    def __init__(self, participants: List[int], modes: List[str], maps: Dict[str, List[str]]):
        self.participants = participants.copy()
        self.modes = modes
        self.maps = maps
        self.current_round = 1
        self.matches: Dict[int, List[Match]] = {}

    def generate_round(self) -> List[Match]:
        """
        Создает пары для текущего раунда, выбирает случайный режим и карту для каждой игры.
        Возвращает список объектов Match для раунда.
        """
        # Перемешиваем список участников
        random.shuffle(self.participants)
        round_matches: List[Match] = []
        for i in range(0, len(self.participants), 2):
            p1 = self.participants[i]
            p2 = self.participants[i+1]
            mode = random.choice(self.modes)
            map_id = random.choice(self.maps.get(mode, [])) if self.maps.get(mode) else ''
            round_matches.append(Match(p1, p2, mode, map_id))
        self.matches[self.current_round] = round_matches
        self.current_round += 1
        return round_matches

    def record_result(self, round_number: int, match_index: int, winner: int):
        """
        Записывает результат для конкретного поединка.
        winner: 1 или 2
        """
        match = self.matches.get(round_number, [])[match_index]
        if match:
            match.result = winner
        else:
            raise IndexError("Матч не найден в указанном раунде")

    def get_winners(self, round_number: int) -> List[int]:
        """
        Возвращает список победителей указанного раунда.
        """
        winners: List[int] = []
        for match in self.matches.get(round_number, []):
            if match.result == 1:
                winners.append(match.player1_id)
            elif match.result == 2:
                winners.append(match.player2_id)
        return winners


# Пример предопределенных игровых режимов и карт
MODES = ["режим1", "режим2", "режим3", "режим4"]
MAPS = {
    "режим1": ["1.1 1", "1.2 2", "1.3 3"],
    "режим2": ["2.1 4", "2.2 5", "2.3 6"],
    "режим3": ["3.1 7", "3.2 8", "3.3 9"],
    "режим4": ["4.1 10", "4.2 11", "4.3 12"],
}

# Утилиты

def create_tournament(participants: List[int]) -> Tournament:
    """Создает новый турнир с заданными участниками."""
    return Tournament(participants, MODES, MAPS)
