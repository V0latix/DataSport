from __future__ import annotations

from .balldontlie_nba_connector import BallDontLieNBAConnector
from .fiba_basketball_world_cup_history_connector import FibaBasketballWorldCupHistoryConnector
from .fiba_ranking_history_connector import FibaRankingHistoryConnector
from .fifa_ranking_history_connector import FifaRankingHistoryConnector
from .fifa_women_ranking_history_connector import FifaWomenRankingHistoryConnector
from .fifa_women_world_cup_history_connector import FifaWomenWorldCupHistoryConnector
from .football_data_connector import FootballDataConnector
from .olympics_keith_history_connector import OlympicsKeithHistoryConnector
from .paris_2024_summer_olympics_connector import Paris2024SummerOlympicsConnector
from .rugby_world_cup_history_connector import RugbyWorldCupHistoryConnector
from .wikidata_connector import WikidataConnector
from .world_cup_history_connector import WorldCupHistoryConnector
from .world_rugby_ranking_history_connector import WorldRugbyRankingHistoryConnector


CONNECTOR_REGISTRY = {
    "wikidata": WikidataConnector,
    "football_data": FootballDataConnector,
    "balldontlie_nba": BallDontLieNBAConnector,
    "fiba_ranking_history": FibaRankingHistoryConnector,
    "fiba_basketball_world_cup_history": FibaBasketballWorldCupHistoryConnector,
    "fifa_ranking_history": FifaRankingHistoryConnector,
    "fifa_women_ranking_history": FifaWomenRankingHistoryConnector,
    "fifa_women_world_cup_history": FifaWomenWorldCupHistoryConnector,
    "world_rugby_ranking_history": WorldRugbyRankingHistoryConnector,
    "rugby_world_cup_history": RugbyWorldCupHistoryConnector,
    "world_cup_history": WorldCupHistoryConnector,
    "paris_2024_summer_olympics": Paris2024SummerOlympicsConnector,
    "olympics_keith_history": OlympicsKeithHistoryConnector,
}


def build_connector(connector_name: str):
    key = connector_name.strip().lower()
    if key not in CONNECTOR_REGISTRY:
        available = ", ".join(sorted(CONNECTOR_REGISTRY))
        raise ValueError(f"Unknown connector '{connector_name}'. Available: {available}")
    return CONNECTOR_REGISTRY[key]()
