import requests
from nba_api_models.base_api import BaseApi


class ScoreBoard(BaseApi):
    """
    Class to model the NBA scoreboard api endpoint
    """

    def __init__(self, game_date, **kwargs):
        self.user_agent = BaseApi.user_agent
        self.LeagueID = kwargs.get("leagueid", "00")
        self.DayOffset = kwargs.get("dayoffset", "0")
        self.GameDate = game_date.strftime("%Y-%m-%d")
        self.url = f"{BaseApi.base_url}stats/scoreboard"

    def response(self):
        url_parameters = self.build_parameters_dict()
        res = requests.get(
            self.url, headers=self.user_agent, params=url_parameters
        ).json()
        # TODO the scoreboard now has entry for each team/game combo rewrite
        # TODO to only pull unique game ids
        games = res["resultSets"][0]["rowSet"]
        game_ids = [int(game[2][2:]) for game in games]
        return game_ids
