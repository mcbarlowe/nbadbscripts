import pandas as pd
import os

# import numpy as np
import nba_parser as npar
from sqlalchemy import create_engine

engine = create_engine(os.environ["NBA_CONNECT"])


pbg_sql = f"select * from nba.player_rapm_shifts where season = 2005"
pbg_df = pd.read_sql(pbg_sql, engine)

pbg_rapm_results = npar.PlayerTotals.player_rapm_results(pbg_df)

pbg_rapm_results.to_sql(
    "player_single_year_rapm",
    con=engine,
    schema="nba",
    if_exists="append",
    index=False,
    method="multi",
)

print(f"calculating 2005 single season team rapm")
tbg_sql = f"select * from nba.teambygamestats where season = 2005"
tbg_df = pd.read_sql(tbg_sql, engine)

tbg_rapm = npar.TeamTotals([tbg_df])

tbg_rapm_results = tbg_rapm.team_rapm_results()
tbg_rapm_results.to_sql(
    "team_single_year_rapm",
    con=engine,
    schema="nba",
    if_exists="append",
    index=False,
    method="multi",
)
pbg_df = pd.read_sql(f"select * from nba.playerbygamestats where season = 2005", engine)
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
tbg_sql = f"select * from nba.teambygamestats where season in (2005)"
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
seasons = [2005, 2006, 2007]

for s in seasons:
    print(f"calculating {s} multi season player rapm")
    multi_pbg_sql = (
        f"select * from nba.player_rapm_shifts where season in ({s}, {s - 1}, {s-2})"
    )
    multi_pbg_df = pd.read_sql(multi_pbg_sql, engine)

    pbg_rapm_results = npar.PlayerTotals.player_rapm_results(multi_pbg_df)

    pbg_rapm_results.to_sql(
        "player_three_year_rapm",
        con=engine,
        schema="nba",
        if_exists="append",
        index=False,
        method="multi",
    )
