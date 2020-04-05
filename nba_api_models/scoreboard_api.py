import requests
from nba_api_models.base_api import BaseApi


class ScoreBoard(BaseApi):
    """
    Class to model the NBA scoreboard api endpoint
    """

    def __init__(self, game_date, **kwargs):
        self.user_agent = BaseApi.user_agent
        self.leagueid = kwargs.get("leagueid", "00")
        self.day_offset = kwargs.get("dayoffset", "0")
        self.game_date = game_date
        self.game_date_str = self.game_date.strftime("%Y-%m-%d")
        self.url = f"{BaseApi.base_url}stats/scoreboard?DayOffset={self.day_offset}&GameDate={self.game_date_str}&LeagueID={self.leagueid}"

    def response(self):
        res = requests.get(self.url, headers=self.user_agent).json()
        games = res["resultSets"][0]["rowSet"]
        game_ids = [int(game[2][2:]) for game in games]
        return game_ids
