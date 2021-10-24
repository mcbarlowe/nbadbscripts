import requests
import time
import pandas as pd
from nba_api_models.base_api import BaseApi


class ShotDetails(BaseApi):
    """
    class to model the API endpoint of the NBA shot details
    """

    def __init__(self, game_id, player_id, **kwargs):
        self.user_agent = BaseApi.user_agent
        self.GameID = f"{kwargs.get('leagueid', '00')}{game_id}"
        self.PlayerID = player_id
        self.AheadBehind = kwargs.get("ahead_behind", "")
        self.ClutchTime = kwargs.get("clutch_time", "")
        self.ContextFilter = kwargs.get("context_filter", "")
        self.ContextMeasure = kwargs.get("context_measure", "FGM")
        self.DateFrom = kwargs.get("date_from", "")
        self.DateTo = kwargs.get("date_to", "")
        self.EndPeriod = kwargs.get("end_period", "")
        self.EndRange = kwargs.get("end_range", "")
        self.GameSegment = kwargs.get("game_segment", "")
        self.LastNGames = kwargs.get("last_n_games", 0)
        self.LeagueID = kwargs.get("leagueid", "00")
        self.Location = kwargs.get("location", "")
        self.Month = kwargs.get("month", 0)
        self.RookieYear = kwargs.get("rookie_year", "")
        self.OpponentTeamID = kwargs.get("opponent_team_id", 0)
        self.Outcome = kwargs.get("outcome", "")
        self.Period = kwargs.get("context_filter", 0)
        self.PlayerPosition = kwargs.get("player_position", "")
        self.PointDiff = kwargs.get("point_diff", "")
        self.Position = kwargs.get("position", "")
        self.RangeType = kwargs.get("range_type", "")
        self.Season = kwargs.get("season", "")
        self.SeasonSegment = kwargs.get("season_segment", "")
        self.SeasonType = kwargs.get("season_type", "Regular Season")
        self.StartPeriod = kwargs.get("start_period", "")
        self.StartRange = kwargs.get("start_range", "")
        self.TeamID = kwargs.get("team_id", 0)
        self.VsConference = kwargs.get("vs_conference", "")
        self.VsDivision = kwargs.get("context_filter", "")
        self.url = f"{BaseApi.base_url}stats/shotchartdetail"

    def response(self):
        """
        method to get the response of the API endpoint
        """
        url_parameters = self.build_parameters_dict()
        shots = requests.get(self.url, headers=self.user_agent, params=url_parameters)
        wait = 1
        while shots.status_code == 504:
            shots = requests.get(
                self.url, headers=self.user_agent, params=url_parameters
            )
            time.sleep(wait)
            wait += 1
        shots = shots.json()
        columns = shots["resultSets"][0]["headers"]
        rows = shots["resultSets"][0]["rowSet"]
        shots_df = pd.DataFrame(rows, columns=columns)
        shots_df.columns = list(map(str.lower, shots_df.columns))
        shots_df["game_id"] = shots_df["game_id"].str.slice(start=2).astype(int)

        return shots_df
