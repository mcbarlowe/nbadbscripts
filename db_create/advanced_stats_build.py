"""
This script computes all the season advanced stats for both players and teams
"""
import pandas as pd
import os

# import numpy as np
import nba_parser as npar
from sqlalchemy import create_engine

engine = create_engine(os.environ["NBA_CONNECT"])


seasons = engine.execute("select distinct season from nba.teambygamestats;")


for s in seasons:
    if s.season == 2020:
        continue
    pbg_df = pd.read_sql(
        f"select * from nba.playerbygamestats where season = {s.season}", engine
    )
    player_totals = npar.PlayerTotals([pbg_df])
    player_adv_stats = player_totals.player_advanced_stats()

    player_adv_stats = player_adv_stats[
        [
            "player_id",
            "player_name",
            "team_abbrev",
            "gp",
            "off_rating",
            "def_rating",
            "efg_percent",
            "ts_percent",
            "oreb_percent",
            "dreb_percent",
            "ast_percent",
            "blk_percent",
            "stl_percent",
            "tov_percent",
            "usg_percent",
            "min_season",
            "max_season",
        ]
    ]
    player_adv_stats.to_sql(
        "player_advanced_stats",
        con=engine,
        schema="nba",
        if_exists="append",
        index=False,
        method="multi",
    )
    tbg_sql = f"select * from nba.teambygamestats where season in ({s.season})"
    tbg_df = pd.read_sql(tbg_sql, engine)

    team_totals = npar.TeamTotals([tbg_df])
    team_adv_stats = team_totals.team_advanced_stats()
    team_adv_stats = team_adv_stats[
        [
            "team_id",
            "team_abbrev",
            "gp",
            "efg_percentage",
            "true_shooting_percentage",
            "oreb_percentage",
            "ft_per_fga",
            "tov_percentage",
            "opp_efg_percentage",
            "opp_tov_percentage",
            "dreb_percentage",
            "opp_ft_per_fga",
            "off_rating",
            "def_rating",
            "min_season",
            "max_season",
        ]
    ]

    team_adv_stats.to_sql(
        "team_advanced_stats",
        con=engine,
        schema="nba",
        if_exists="append",
        index=False,
        method="multi",
    )
