"""
This script is used to build the initial database from scraped csv files and the
pbp, playerbygamestats, teambygamestats, and player_rapm_shifts tables. It could
be reworked to use the outputs from scraping just as easily. I normally only do
a season at a time to prevent any major loss if something bad happens
"""
import pandas as pd
import requests
import os

# import numpy as np
import nba_parser as npar
from sqlalchemy import create_engine

engine = create_engine(os.environ["NBA_CONNECT"])
historical_scrapes = (
    "/Users/MattBarlowe/code/python/historical_scraping/seasons/19992000/"
)


sql = """select player_id from nba.player_details"""
data = engine.connect().execute(sql)
player_ids = [row.player_id for row in data]
player_ids = set(player_ids)


# TODO rewrite this to use PlayerDetails api model 2020-04-05
def parse_player_details(game_df, engine, player_ids):
    """
    function to deterimine if the players details need to be added to the
    database
    Inputs:
    game_df  - pandas dataframe of play by play
    engine  - SQL ALchemy Engine

    Outputs:
    None
    """
    user_agent = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:72.0) Gecko/20100101 Firefox/72.0",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.5",
        "X-NewRelic-ID": "VQECWF5UChAHUlNTBwgBVw==",
        "x-nba-stats-origin": "stats",
        "x-nba-stats-token": "true",
        "Connection": "keep-alive",
        "Referer": "https://stats.nba.com/",
    }
    players = list(
        set(
            list(game_df["home_player_1_id"].unique())
            + list(game_df["home_player_2_id"].unique())
            + list(game_df["home_player_3_id"].unique())
            + list(game_df["home_player_4_id"].unique())
            + list(game_df["home_player_5_id"].unique())
            + list(game_df["away_player_1_id"].unique())
            + list(game_df["away_player_2_id"].unique())
            + list(game_df["away_player_3_id"].unique())
            + list(game_df["away_player_4_id"].unique())
            + list(game_df["away_player_5_id"].unique())
        )
    )
    for p in players:
        if p not in player_ids:
            url = f"https://stats.nba.com/stats/commonplayerinfo?LeagueID=&PlayerID={p}"
            info = requests.get(url, headers=user_agent).json()
            headers = list(map(str.lower, info["resultSets"][0]["headers"]))
            player = info["resultSets"][0]["rowSet"][0]
            player_dict = {}
            for play, head in zip(player, headers):
                player_dict[head] = [play]
            print(f"Inserting {player_dict['display_first_last']} into player database")
            player_df = pd.DataFrame.from_dict(player_dict)
            player_df = player_df.rename(
                columns={
                    "person_id": "player_id",
                    "season_exp": "season_experience",
                    "jersey": "jersey_number",
                }
            )
            player_df.to_sql(
                "player_details",
                engine,
                schema="nba",
                method="multi",
                if_exists="append",
                index=False,
            )


for root, dirs, files in os.walk(historical_scrapes):
    dirs.sort()
    for f in sorted(files):
        print(f)
        pbp_df = pd.read_csv(os.path.join(root, f))
        if str(pbp_df["game_id"].unique()[0])[1:3] == "99":
            pbp_df["season"] = 2000
        elif str(pbp_df["game_id"].unique()[0])[1:3] == "00":
            pbp_df["season"] = 2001
        else:
            pbp_df.loc[
                :, ("season")
            ] = f"20{int(str(pbp_df['game_id'].unique()[0])[1:3])+1:02}"
        pbp = npar.PbP(pbp_df)
        pbg = pbp.playerbygamestats()
        tbg = pbp.teambygamestats()
        rapm_shifts = pbp.rapm_possessions()
        # parse_player_details(pbp.df, engine, player_ids)
        pbp.df.to_sql(
            "pbp",
            con=engine,
            schema="nba",
            if_exists="append",
            index=False,
            method="multi",
        )
        pbg.to_sql(
            "playerbygamestats",
            con=engine,
            schema="nba",
            if_exists="append",
            index=False,
            method="multi",
        )
        tbg.to_sql(
            "teambygamestats",
            con=engine,
            schema="nba",
            if_exists="append",
            index=False,
            method="multi",
        )
        rapm_shifts.to_sql(
            "player_rapm_shifts",
            con=engine,
            schema="nba",
            if_exists="append",
            index=False,
            method="multi",
        )
