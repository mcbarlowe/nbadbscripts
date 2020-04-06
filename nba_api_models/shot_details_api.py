import requests
import pandas as pd
from nba_api_models.base_api import BaseApi


class ShotDetails(BaseApi):
    """
    class to model the API endpoint of the NBA shot details
    """

    def __init__(self, game_id, player_id, **kwargs):
        self.user_agent = BaseApi.user_agent
        self.game_id = game_id
        self.player_id = player_id
        self.ahead_behind = kwargs.get("ahead_behind", "")
        self.clutch_time = kwargs.get("clutch_time", "")
        self.context_filter = kwargs.get("context_filter", "")
        self.context_measure = kwargs.get("contest_measure", "FG")
        self.date_from = kwargs.get("date_from", "")
        self.date_to = kwargs.get("date_to", "")
        self.end_period = kwargs.get("end_period", "")
        self.end_range = kwargs.get("end_range", "")
        self.game_segment = kwargs.get("game_segment", "")
        self.last_n_games = kwargs.get("last_n_games", 0)
        self.leagueid = kwargs.get("leagueid", "00")
        self.location = kwargs.get("location", "")
        self.month = kwargs.get("month", 0)
        self.rookie_year = kwargs.get("rookie_year", "")
        self.opponent_team_id = kwargs.get("opponent_team_id", 0)
        self.outcome = kwargs.get("outcome", "")
        self.period = kwargs.get("context_filter", 0)
        self.player_position = kwargs.get("player_position", "")
        self.point_diff = kwargs.get("point_diff", "")
        self.position = kwargs.get("position", "")
        self.range_type = kwargs.get("range_type", "")
        self.season = kwargs.get("season", "")
        self.season_segment = kwargs.get("season_segment", "")
        self.season_type = kwargs.get("season_type", "Regular+Season")
        self.start_period = kwargs.get("start_period", "")
        self.start_range = kwargs.get("start_range", "")
        self.team_id = kwargs.get("team_id", 0)
        self.vs_conference = kwargs.get("vs_conference", "")
        self.vs_division = kwargs.get("context_filter", "")
        self.url = (
            f"https://stats.nba.com/stats/shotchartdetail?AheadBehind={self.ahead_behind}"
            f"&ClutchTime={self.clutch_time}&ContextFilter={self.context_filter}"
            f"&ContextMeasure={self.context_measure}A&DateFrom={self.date_from}"
            f"&DateTo={self.date_to}&EndPeriod={self.end_period}&EndRange={self.end_range}"
            f"&GameID={self.leagueid}{self.game_id}&GameSegment={self.game_segment}"
            f"&LastNGames={self.last_n_games}&LeagueID={self.leagueid}&Location{self.location}"
            f"=&Month={self.month}&OpponentTeamID={self.opponent_team_id}"
            f"&Outcome={self.outcome}&Period={self.period}&PlayerID={self.player_id}"
            f"&PlayerPosition={self.player_position}&PointDiff={self.point_diff}"
            f"&Position={self.position}&RangeType={self.range_type}&RookieYear={self.rookie_year}"
            f"&Season={self.season}&SeasonSegment={self.season_segment}&SeasonType={self.season_type}"
            f"&StartPeriod={self.start_period}&StartRange={self.start_range}"
            f"&TeamID={self.team_id}&VsConference={self.vs_conference}&VsDivision={self.vs_division}"
        )

    def response(self):
        '''
        method to get the response of the API endpoint
        '''
        shots = requests.get(self.url, headers=self.user_agent).json()
        columns = shots["resultSets"][0]["headers"]
        rows = shots["resultSets"][0]["rowSet"]
        shots_df = pd.DataFrame(rows, columns=columns)
        shots_df.columns = list(map(str.lower, shots_df.columns))
        shots_df["game_id"] = shots_df["game_id"].str.slice(start=2).astype(int)

        print(shots_df.head())
        return shots_df
