"""
This will be the weekly script I run to recalculate RAPMs and advanced stats for
both players and teams.
"""

import os
import logging
import argparse
import nba_parser as npar
import pandas as pd

from sqlalchemy import create_engine


def player_single_year_rapm_calc(season, engine):
    """
    calculating player rapms based on seasons of shifts passed to it

    Inputs:
    season    - list of seasons
    engine    - sql alchemy engine

    Outputs:
    None
    """

    print(f"calculating {season} season player rapm")
    pbg_sql = f"select * from nba.player_rapm_shifts where season = {season} and (game_id >= 20000000 and game_id < 30000000)"
    rapm_df = pd.read_sql(pbg_sql, engine)

    pbg_rapm_results = npar.PlayerTotals.player_rapm_results(rapm_df)

    engine.execute(
        f"delete from nba.player_single_year_rapm where min_season = {season}"
    )

    pbg_rapm_results.to_sql(
        "player_single_year_rapm",
        con=engine,
        schema="nba",
        if_exists="append",
        index=False,
        method="multi",
    )


def player_three_year_rapm_calc(min_season, max_season, engine):
    """
    calculating player rapms based on seasons of shifts passed to it

    Inputs:
    season    - list of seasons
    engine    - sql alchemy engine

    Outputs:
    None
    """

    print(f"calculating {min_season}-{max_season} player rapm")
    pbg_sql = f"select * from nba.player_rapm_shifts where season between {min_season} and {max_season} and (game_id >= 20000000 and game_id < 30000000)"
    rapm_df = pd.read_sql(pbg_sql, engine)

    pbg_rapm_results = npar.PlayerTotals.player_rapm_results(rapm_df)

    engine.execute(
        f"delete from nba.player_three_year_rapm where min_season = {min_season}"
    )

    pbg_rapm_results.to_sql(
        "player_three_year_rapm",
        con=engine,
        schema="nba",
        if_exists="append",
        index=False,
        method="multi",
    )


def team_rapm_calc(season, engine):
    """
    calculating team rapm

    Inputs:
    season    - number representing season
    engine    - sql alchemy engine

    Outputs:
    None
    """

    print(f"calculating {season} season team rapm")
    tbg_sql = f"select * from nba.teambygamestats where season = {season }and (game_id >= 20000000 and game_id < 30000000)"
    tbg_df = pd.read_sql(tbg_sql, engine)

    tbg_rapm = npar.TeamTotals([tbg_df])

    engine.execute(f"delete from nba.team_single_year_rapm where min_season = {season}")
    tbg_rapm_results = tbg_rapm.team_rapm_results()
    tbg_rapm_results.to_sql(
        "team_single_year_rapm",
        con=engine,
        schema="nba",
        if_exists="append",
        index=False,
        method="multi",
    )


def player_advanced_stats_calc(season, engine):
    print(f"calculating {season} season player advanced stats")
    pbg_sql = f"select * from nba.playerbygamestats where season in ({season}) and (game_id >= 20000000 and game_id < 30000000)"
    pbg_df = pd.read_sql(pbg_sql, engine)

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
    engine.execute(
        f"delete from nba.player_advanced_stats where min_season in ({season})"
    )
    player_adv_stats.to_sql(
        "player_advanced_stats",
        con=engine,
        schema="nba",
        if_exists="append",
        index=False,
        method="multi",
    )


def team_advanced_stats_calc(season, engine):
    tbg_sql = f"select * from nba.teambygamestats where season in ({season}) and (game_id >= 20000000 and game_id < 30000000)"
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

    engine.execute(
        f"delete from nba.team_advanced_stats where min_season in ({season})"
    )
    team_adv_stats.to_sql(
        "team_advanced_stats",
        con=engine,
        schema="nba",
        if_exists="append",
        index=False,
        method="multi",
    )


def main():

    logging.basicConfig(
        level=logging.INFO,
        filename="weeklyprocess.logs",
        format="%(asctime)s - %(levelname)s: %(message)s",
    )
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int)

    args = parser.parse_args()
    season = int(args.season)

    engine = create_engine(os.environ["NBA_CONNECT"])

    logging.info("Calculating Team Advanced Stats")
    team_advanced_stats_calc(season, engine)
    logging.info("Calculating Three Year Player Rapm")
    player_three_year_rapm_calc(season - 2, season, engine)
    logging.info("Calculating Player Advanced Stats")
    player_advanced_stats_calc(season, engine)
    logging.info("Calculating Single Year Player Rapm")
    player_single_year_rapm_calc(season, engine)
    logging.info("Calculating Single Year Team Rapm")
    team_rapm_calc(season, engine)


if __name__ == "__main__":
    main()
