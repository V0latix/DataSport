from __future__ import annotations

from .balldontlie_nba_connector import BallDontLieNBAConnector
from .bwf_world_championships_history_connector import BwfWorldChampionshipsHistoryConnector
from .bwf_thomas_uber_cup_history_connector import BwfThomasUberCupHistoryConnector
from .fiba_basketball_world_cup_history_connector import FibaBasketballWorldCupHistoryConnector
from .fiba_ranking_history_connector import FibaRankingHistoryConnector
from .fih_hockey_world_cup_history_connector import FihHockeyWorldCupHistoryConnector
from .formulae_world_championship_history_connector import FormulaEWorldChampionshipHistoryConnector
from .formula1_world_championship_history_connector import Formula1WorldChampionshipHistoryConnector
from .fivb_volleyball_world_championship_history_connector import FivbVolleyballWorldChampionshipHistoryConnector
from .fifa_ranking_history_connector import FifaRankingHistoryConnector
from .fifa_women_ranking_history_connector import FifaWomenRankingHistoryConnector
from .fifa_women_world_cup_history_connector import FifaWomenWorldCupHistoryConnector
from .football_data_connector import FootballDataConnector
from .ihf_handball_world_championship_history_connector import IhfHandballWorldChampionshipHistoryConnector
from .ittf_world_table_tennis_championships_history_connector import IttfWorldTableTennisChampionshipsHistoryConnector
from .icc_team_ranking_history_connector import IccTeamRankingHistoryConnector
from .icc_cricket_world_cup_history_connector import IccCricketWorldCupHistoryConnector
from .olympics_keith_history_connector import OlympicsKeithHistoryConnector
from .paris_2024_summer_olympics_connector import Paris2024SummerOlympicsConnector
from .rugby_league_world_cup_history_connector import RugbyLeagueWorldCupHistoryConnector
from .rugby_world_cup_history_connector import RugbyWorldCupHistoryConnector
from .rugby_world_cup_sevens_history_connector import RugbyWorldCupSevensHistoryConnector
from .uci_road_cycling_major_competitions_history_connector import (
    UciRoadCyclingMajorCompetitionsHistoryConnector,
)
from .uci_road_nation_ranking_history_connector import UciRoadNationRankingHistoryConnector
from .uci_track_cycling_world_championships_history_connector import (
    UciTrackCyclingWorldChampionshipsHistoryConnector,
)
from .wbsc_baseball_softball_world_championship_history_connector import (
    WbscBaseballSoftballWorldChampionshipHistoryConnector,
)
from .wikidata_connector import WikidataConnector
from .world_cup_history_connector import WorldCupHistoryConnector
from .world_athletics_championships_history_connector import WorldAthleticsChampionshipsHistoryConnector
from .world_aquatics_championships_history_connector import WorldAquaticsChampionshipsHistoryConnector
from .world_judo_championships_history_connector import WorldJudoChampionshipsHistoryConnector
from .world_rowing_championships_history_connector import WorldRowingChampionshipsHistoryConnector
from .world_wrestling_championships_history_connector import WorldWrestlingChampionshipsHistoryConnector
from .world_rugby_ranking_history_connector import WorldRugbyRankingHistoryConnector


CONNECTOR_REGISTRY = {
    "wikidata": WikidataConnector,
    "football_data": FootballDataConnector,
    "balldontlie_nba": BallDontLieNBAConnector,
    "bwf_world_championships_history": BwfWorldChampionshipsHistoryConnector,
    "bwf_thomas_uber_cup_history": BwfThomasUberCupHistoryConnector,
    "ittf_world_table_tennis_championships_history": IttfWorldTableTennisChampionshipsHistoryConnector,
    "fiba_ranking_history": FibaRankingHistoryConnector,
    "fiba_basketball_world_cup_history": FibaBasketballWorldCupHistoryConnector,
    "fih_hockey_world_cup_history": FihHockeyWorldCupHistoryConnector,
    "formulae_world_championship_history": FormulaEWorldChampionshipHistoryConnector,
    "formula1_world_championship_history": Formula1WorldChampionshipHistoryConnector,
    "fivb_volleyball_world_championship_history": FivbVolleyballWorldChampionshipHistoryConnector,
    "fifa_ranking_history": FifaRankingHistoryConnector,
    "fifa_women_ranking_history": FifaWomenRankingHistoryConnector,
    "fifa_women_world_cup_history": FifaWomenWorldCupHistoryConnector,
    "world_rugby_ranking_history": WorldRugbyRankingHistoryConnector,
    "rugby_league_world_cup_history": RugbyLeagueWorldCupHistoryConnector,
    "rugby_world_cup_history": RugbyWorldCupHistoryConnector,
    "rugby_world_cup_sevens_history": RugbyWorldCupSevensHistoryConnector,
    "uci_road_cycling_major_competitions_history": UciRoadCyclingMajorCompetitionsHistoryConnector,
    "uci_road_nation_ranking_history": UciRoadNationRankingHistoryConnector,
    "uci_track_cycling_world_championships_history": UciTrackCyclingWorldChampionshipsHistoryConnector,
    "wbsc_baseball_softball_world_championship_history": WbscBaseballSoftballWorldChampionshipHistoryConnector,
    "ihf_handball_world_championship_history": IhfHandballWorldChampionshipHistoryConnector,
    "icc_team_ranking_history": IccTeamRankingHistoryConnector,
    "icc_cricket_world_cup_history": IccCricketWorldCupHistoryConnector,
    "world_cup_history": WorldCupHistoryConnector,
    "world_athletics_championships_history": WorldAthleticsChampionshipsHistoryConnector,
    "world_aquatics_championships_history": WorldAquaticsChampionshipsHistoryConnector,
    "world_judo_championships_history": WorldJudoChampionshipsHistoryConnector,
    "world_rowing_championships_history": WorldRowingChampionshipsHistoryConnector,
    "world_wrestling_championships_history": WorldWrestlingChampionshipsHistoryConnector,
    "paris_2024_summer_olympics": Paris2024SummerOlympicsConnector,
    "olympics_keith_history": OlympicsKeithHistoryConnector,
}


def build_connector(connector_name: str):
    key = connector_name.strip().lower()
    if key not in CONNECTOR_REGISTRY:
        available = ", ".join(sorted(CONNECTOR_REGISTRY))
        raise ValueError(f"Unknown connector '{connector_name}'. Available: {available}")
    return CONNECTOR_REGISTRY[key]()
