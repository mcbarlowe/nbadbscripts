import requests
import pandas as pd
from nba_api_models.base_api import BaseApi


class PlayerDetails(BaseApi):
    """
    This class models the nba player detail api endpoint
    """

    def __init__(self, **kwargs):
        self.user_agent = BaseApi.user_agent
        self.playerid = kwargs["playerid"]
        self.leagueid = kwargs.get("leagueid", "")
        self.url = f"{BaseApi.base_url}stats/commonplayerinfo?LeagueID={self.leagueid}&PlayerID={self.playerid}"

    def get_player_data(self):
        """
        function to return the relevant player data from the nba player
        detail API
        """
        info = requests.get(self.url, headers=self.user_agent).json()
        headers = list(map(str.lower, info["resultSets"][0]["headers"]))
        player = info["resultSets"][0]["rowSet"][0]
        player_dict = {}
        for play, head in zip(player, headers):
            player_dict[head] = [play]
        player_df = pd.DataFrame.from_dict(player_dict)
        player_df = player_df.rename(
            columns={
                "person_id": "player_id",
                "season_exp": "season_experience",
                "jersey": "jersey_number",
            }
        )

        return player_df
