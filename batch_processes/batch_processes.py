"""
This script runs the batch processes for the NBA database
"""
# import datetime
import math
import os
import logging
import nba_scraper.nba_scraper as ns
import requests
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import sqlqueries
import datetime
import time
import player_advanced_stats as pas
import calc_game_shifts as cgs
from rapm_calculation import (
    one_year_team_rapm_calc,
    multi_year_rapm_calc,
    one_year_rapm_calc,
)
import random


def parse_player_details(game_df, engine):
    """
    function to deterimine if the players details need to be added to the
    database
    Inputs:
    game_df  - pandas dataframe of play by play
    engine  - SQL ALchemy Engine

    Outputs:
    None
    """
    sql = """select player_id from nba.player_details"""
    data = engine.connect().execute(sql)
    player_ids = [row[0] for row in data]
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


def parse_player_shots(game_df, engine):
    """
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
    game_id = "00" + game_df["game_id"].astype(str).unique()[0]
    shots_df_list = []
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
    for player in players:
        url = (
            "https://stats.nba.com/stats/shotchartdetail?AheadBehind="
            "&ClutchTime=&ContextFilter=&ContextMeasure=FGA&DateFrom="
            f"&DateTo=&EndPeriod=&EndRange=&GameID={game_id}&GameSegment=&LastNGames=0"
            "&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&Period=0"
            f"&PlayerID={player}&PlayerPosition=&PointDiff=&Position=&RangeType="
            f"&RookieYear=&Season=&SeasonSegment=&SeasonType=Regular+Season"
            "&StartPeriod=&StartRange=&TeamID=0&VsConference=&VsDivision="
        )
        shots = requests.get(url, headers=user_agent).json()
        columns = shots["resultSets"][0]["headers"]
        rows = shots["resultSets"][0]["rowSet"]
        shots_df = pd.DataFrame(rows, columns=columns)
        shots_df.columns = list(map(str.lower, shots_df.columns))
        shots_df["game_id"] = shots_df["game_id"].str.slice(start=2).astype(int)
        shots_df_list.append(shots_df)
        time.sleep(5)

    total_shots = pd.concat(shots_df_list)
    total_shots["key_col"] = (
        total_shots["player_id"].astype(str)
        + total_shots["game_id"].astype(str)
        + total_shots["game_event_id"].astype(str)
    )
    total_shots.to_sql(
        "shot_locations",
        engine,
        schema="nba",
        if_exists="append",
        index=False,
        method="multi",
    )


def calc_team_advanced_stats(season, engine):

    engine.connect().execute(
        f"DELETE from nba.team_advanced_stats where season = {season};"
    )
    teams_df = pd.read_sql_query(
        "select tbg.*, tp.possessions from nba.teambygamestats tbg join nba.team_possessions tp "
        f"on tp.game_id = tbg.game_id and tp.team_id = tbg.team_id where season={season};",
        engine,
    )
    teams_df = teams_df.merge(teams_df, on="game_id", suffixes=["", "_opponent"])
    teams_df = teams_df[teams_df.team_id != teams_df.team_id_opponent]
    team_advanced_stats = (
        teams_df.groupby(["team_id", "team_abbrev", "season"])[
            "fgm",
            "tpm",
            "fga",
            "points_for",
            "points_against",
            "fta",
            "tov",
            "dreb",
            "oreb",
            "ftm",
            "dreb_opponent",
            "oreb_opponent",
            "fgm_opponent",
            "fga_opponent",
            "tpm_opponent",
            "tpa_opponent",
            "fta_opponent",
            "ftm_opponent",
            "tov_opponent",
            "possessions",
            "possessions_opponent",
        ]
        .sum()
        .reset_index()
    )
    team_advanced_stats["efg_percentage"] = (
        team_advanced_stats["fgm"] + (0.5 * team_advanced_stats["tpm"])
    ) / team_advanced_stats["fga"]
    team_advanced_stats["true_shooting_percentage"] = team_advanced_stats[
        "points_for"
    ] / (2 * (team_advanced_stats["fga"] + (team_advanced_stats["fta"] * 0.44)))
    team_advanced_stats["tov_percentage"] = 100 * (
        team_advanced_stats["tov"] / team_advanced_stats["possessions"]
    )
    team_advanced_stats["oreb_percentage"] = 100 * (
        team_advanced_stats["oreb"]
        / (team_advanced_stats["oreb"] + team_advanced_stats["dreb_opponent"])
    )
    team_advanced_stats["ft_per_fga"] = (
        team_advanced_stats["ftm"] / team_advanced_stats["fta"]
    )
    team_advanced_stats["opp_efg_percentage"] = (
        team_advanced_stats["fgm_opponent"]
        + (0.5 * team_advanced_stats["tpm_opponent"])
    ) / team_advanced_stats["fga_opponent"]
    team_advanced_stats["opp_tov_percentage"] = 100 * (
        team_advanced_stats["tov_opponent"]
        / team_advanced_stats["possessions_opponent"]
    )
    team_advanced_stats["dreb_percentage"] = 100 * (
        team_advanced_stats["dreb"]
        / (team_advanced_stats["oreb_opponent"] + team_advanced_stats["dreb"])
    )
    team_advanced_stats["opp_ft_per_fga"] = (
        team_advanced_stats["ftm_opponent"] / team_advanced_stats["fta_opponent"]
    )
    team_advanced_stats["off_rating"] = (
        team_advanced_stats["points_for"] / team_advanced_stats["possessions"] * 100
    )
    team_advanced_stats["def_rating"] = (
        team_advanced_stats["points_against"] / team_advanced_stats["possessions"] * 100
    )
    team_advanced_stats = team_advanced_stats[
        [
            "team_id",
            "team_abbrev",
            "season",
            "efg_percentage",
            "true_shooting_percentage",
            "tov_percentage",
            "oreb_percentage",
            "ft_per_fga",
            "opp_efg_percentage",
            "opp_tov_percentage",
            "dreb_percentage",
            "opp_ft_per_fga",
            "off_rating",
            "def_rating",
        ]
    ]
    team_advanced_stats["key_col"] = team_advanced_stats["team_id"].astype(
        str
    ) + team_advanced_stats["season"].astype(str)

    team_advanced_stats.to_sql(
        "team_advanced_stats",
        engine,
        schema="nba",
        if_exists="append",
        index=False,
        method="multi",
    )


def calc_player_advanced_stats(season, engine):
    """
    this function will calculated the stats on the team/player advanced tabs
    for the curent season. It will drop the current table based on season passed,
    recalculate the stats, and then reinsert them into the table.

    Inputs:
    season  - current season needed to be calculated
    engine  - SQLAlchemy engine

    Outputs:
    None
    """

    # delete old values
    engine.connect().execute(
        f"DELETE from nba.player_advanced_stats where season = {season};"
    )
    engine.connect().execute(
        f"DELETE from nba.team_advanced_stats where season = {season};"
    )
    player_possessions = pd.read_sql_query(
        pas.player_possession_query.format(season=season), engine
    )
    plus_minus_df = pd.read_sql_query(pas.plus_minus_sql.format(season=season), engine)
    pm_df = plus_minus_df[~plus_minus_df.isna()]
    ratings_df = pm_df.merge(player_possessions, on=["player_id", "season"])
    ratings_df["off_rating"] = (ratings_df["plus"] * 100) / ratings_df["possessions"]
    ratings_df["def_rating"] = (ratings_df["minus"] * 100) / ratings_df["possessions"]
    team_df = pd.read_sql_query(pas.team_query.format(season=season), engine)

    # calculating effective fg% and true fg%
    players_df = pd.read_sql_query(
        f"select * from nba.playerbygamestats where toc > 0 and season = {season};",
        engine,
    )
    players_df = players_df.merge(team_df, on=["game_id", "team_id"])
    player_teams = (
        players_df.groupby(["player_id", "player_name", "season"])
        .apply(
            lambda x: pd.Series({"team_abbrev": "/".join(x["team_abbrev"].unique())})
        )
        .reset_index()
    )
    player_efg = (
        players_df.groupby(["player_id", "player_name", "season"])[
            "fgm", "tpm", "fga", "points", "fta"
        ]
        .sum()
        .reset_index()
    )
    player_efg["efg_percentage"] = (
        player_efg["fgm"] + (0.5 * player_efg["tpm"])
    ) / player_efg["fga"]
    player_efg["true_shooting_percentage"] = player_efg["points"] / (
        2 * (player_efg["fga"] + (player_efg["fta"] * 0.44))
    )
    player_stats = player_teams.merge(
        player_efg[
            ["player_id", "season", "efg_percentage", "true_shooting_percentage"]
        ],
        on=["player_id", "season"],
    )
    # calculating percentage stats
    percentage_stats = (
        players_df.groupby(["player_id", "player_name", "season"])[
            "toc",
            "oreb",
            "dreb",
            "tov",
            "stl",
            "blk",
            "ast",
            "fgm",
            "fga",
            "tpm",
            "tpa",
            "ftm",
            "fta",
            "team_toc",
            "team_oreb",
            "team_dreb",
            "team_tov",
            "team_fgm",
            "team_fga",
            "team_ftm",
            "team_fta",
            "team_tpm",
            "team_tpa",
            "opp_fga",
            "opp_fgm",
            "opp_fta",
            "opp_ftm",
            "opp_tpa",
            "opp_tpm",
            "team_stl",
            "team_blk",
            "opp_toc",
            "opp_dreb",
            "opp_oreb",
            "opp_ast",
            "opp_tov",
            "opp_stl",
            "opp_blk",
            "team_possessions",
            "opp_possessions",
        ]
        .sum()
        .reset_index()
    )

    percentage_stats["oreb_percentage"] = (
        (percentage_stats["oreb"] * (percentage_stats["team_toc"] / 60))
        / (
            (percentage_stats["toc"] / 60)
            * (percentage_stats["team_oreb"] + percentage_stats["opp_dreb"])
        )
    ) * 100
    percentage_stats["dreb_percentage"] = (
        (percentage_stats["dreb"] * (percentage_stats["team_toc"] / 60))
        / (
            (percentage_stats["toc"] / 60)
            * (percentage_stats["team_dreb"] + percentage_stats["opp_oreb"])
        )
    ) * 100
    percentage_stats["ast_percentage"] = 100 * (
        percentage_stats["ast"]
        / (
            (
                ((percentage_stats["toc"]) / (percentage_stats["team_toc"]))
                * percentage_stats["team_fgm"]
            )
            - percentage_stats["fgm"]
        )
    )
    percentage_stats["stl_percentage"] = 100 * (
        (percentage_stats["stl"] * (percentage_stats["team_toc"] / 60))
        / (percentage_stats["toc"] / 60 * percentage_stats["opp_possessions"])
    )
    percentage_stats["blk_percentage"] = 100 * (
        (percentage_stats["blk"] * (percentage_stats["team_toc"] / 60))
        / (
            percentage_stats["toc"]
            / 60
            * (percentage_stats["opp_fga"] - percentage_stats["opp_tpa"])
        )
    )
    percentage_stats["tov_percentage"] = 100 * (
        percentage_stats["tov"]
        / (
            percentage_stats["fga"]
            + 0.44 * percentage_stats["fta"]
            + percentage_stats["tov"]
        )
    )
    percentage_stats["usg_percentage"] = 100 * (
        (
            (
                percentage_stats["fga"]
                + 0.44 * percentage_stats["fta"]
                + percentage_stats["tov"]
            )
            * percentage_stats["team_toc"]
            / 60
        )
        / (
            percentage_stats["toc"]
            / 60
            * (
                percentage_stats["team_fga"]
                + 0.44 * percentage_stats["team_fta"]
                + percentage_stats["team_tov"]
            )
        )
    )
    # merging the three dataframes together
    player_adv_stats = player_stats.merge(
        percentage_stats[
            [
                "player_id",
                "oreb_percentage",
                "dreb_percentage",
                "ast_percentage",
                "stl_percentage",
                "blk_percentage",
                "tov_percentage",
                "usg_percentage",
                "season",
            ]
        ],
        on=["player_id", "season"],
    )
    player_adv_stats = player_adv_stats.merge(
        ratings_df[["player_id", "season", "off_rating", "def_rating"]],
        on=["season", "player_id"],
    )
    player_adv_stats["key_col"] = player_adv_stats["player_id"].astype(
        str
    ) + player_adv_stats["season"].astype(str)

    player_adv_stats = player_adv_stats[
        [
            "player_id",
            "season",
            "team_abbrev",
            "efg_percentage",
            "true_shooting_percentage",
            "oreb_percentage",
            "dreb_percentage",
            "ast_percentage",
            "stl_percentage",
            "blk_percentage",
            "tov_percentage",
            "usg_percentage",
            "off_rating",
            "def_rating",
            "key_col",
        ]
    ]
    player_adv_stats.to_sql(
        "player_advanced_stats",
        engine,
        schema="nba",
        if_exists="append",
        index=False,
        method="multi",
    )


def calc_team_stats(game_df, engine):
    """
    calculates the team stats and inserts them into the database

    Inputs:
    game_df   - play by play dataframe
    engine    - sql alchemy database connection engine

    Ouputs:
    None
    """
    # calculating columns that will be used in the team stats
    game_df["is_assist"] = np.where(
        (game_df["event_type_de"] == "shot") & (game_df["player2_id"] != 0), True, False
    )

    game_df["fg_attempted"] = np.where(
        game_df["event_type_de"].isin(["missed_shot", "shot"]), True, False
    )
    game_df["ft_attempted"] = np.where(
        game_df["event_type_de"] == "free-throw", True, False
    )
    game_df["fg_made"] = np.where(
        (game_df["event_type_de"].isin(["missed_shot", "shot"]))
        & (game_df["points_made"] > 0),
        True,
        False,
    )
    game_df["tp_made"] = np.where(game_df["points_made"] == 3, True, False)
    game_df["ft_made"] = np.where(
        (game_df["event_type_de"] == "free-throw") & (game_df["points_made"] == 1),
        True,
        False,
    )
    game_df["is_foul"] = np.where(
        (game_df["event_type_de"] == "foul")
        & (~game_df["player1_id"].isnull())
        & (
            ~game_df["eventmsgactiontype"].isin(
                [11, 12, 13, 16, 18, 17, 18, 19, 21, 25, 30]
            )
        ),
        True,
        False,
    )
    # calculating the team stats
    home_team_id = game_df["home_team_id"].unique()[0]
    away_team_id = game_df["away_team_id"].unique()[0]
    home_team_abbrev = game_df["home_team_abbrev"].unique()[0]
    away_team_abbrev = game_df["away_team_abbrev"].unique()[0]
    teams_df = (
        game_df.groupby(["player1_team_id", "game_id"])[
            "points_made",
            "is_block",
            "is_steal",
            "is_three",
            "is_d_rebound",
            "is_o_rebound",
            "is_turnover",
            "fg_attempted",
            "ft_attempted",
            "fg_made",
            "tp_made",
            "ft_made",
            "is_assist",
            "is_foul",
        ]
        .sum()
        .reset_index()
    )
    teams_df["player1_team_id"] = teams_df["player1_team_id"].astype(int)
    teams_df.rename(
        columns={
            "player1_team_id": "team_id",
            "points_made": "points_for",
            "is_three": "tpa",
            "is_d_rebound": "dreb",
            "is_o_rebound": "oreb",
            "is_turnover": "tov",
            "is_block": "shots_blocked",
            "is_steal": "stl",
            "is_assist": "ast",
            "fg_made": "fgm",
            "fg_attempted": "fga",
            "ft_made": "ftm",
            "ft_attempted": "fta",
            "tp_made": "tpm",
            "is_foul": "pf",
        },
        inplace=True,
    )
    teams_df["team_abbrev"] = np.where(
        teams_df["team_id"] == home_team_id, home_team_abbrev, away_team_abbrev
    )
    teams_df["game_date"] = game_df["game_date"].unique()[0]
    teams_df["season"] = game_df["season"].unique()[0]
    teams_df["toc"] = game_df["seconds_elapsed"].max()
    teams_df[
        "toc_string"
    ] = f"{math.floor(game_df['seconds_elapsed'].max()/60)}:{game_df['seconds_elapsed'].max()%60}0"
    teams_df["is_home"] = np.where(teams_df["team_id"] == home_team_id, 1, 0)
    teams_df["points_against"] = np.where(
        teams_df["team_id"] == home_team_id,
        teams_df[teams_df["team_id"] == away_team_id]["points_for"],
        0,
    )
    teams_df["points_against"] = np.where(
        teams_df["team_id"] == away_team_id,
        teams_df[teams_df["team_id"] == home_team_id]["points_for"],
        teams_df["points_against"],
    )
    teams_df["pf_drawn"] = np.where(
        teams_df["team_id"] == home_team_id,
        teams_df[teams_df["team_id"] == away_team_id]["pf"],
        0,
    )
    teams_df["pf_drawn"] = np.where(
        teams_df["team_id"] == away_team_id,
        teams_df[teams_df["team_id"] == home_team_id]["pf"],
        teams_df["pf_drawn"],
    )
    teams_df["blk"] = np.where(
        teams_df["team_id"] == home_team_id,
        teams_df[teams_df["team_id"] == away_team_id]["shots_blocked"],
        0,
    )
    teams_df["blk"] = np.where(
        teams_df["team_id"] == away_team_id,
        teams_df[teams_df["team_id"] == home_team_id]["shots_blocked"],
        teams_df["blk"],
    )
    teams_df["is_win"] = np.where(
        teams_df["points_for"] > teams_df["points_against"], 1, 0
    )
    teams_df["is_assist"] = np.where
    teams_df["plus_minus"] = teams_df["points_for"] - teams_df["points_against"]
    teams_df = teams_df[
        [
            "season",
            "game_date",
            "game_id",
            "team_abbrev",
            "team_id",
            "toc",
            "toc_string",
            "points_for",
            "points_against",
            "is_win",
            "fgm",
            "fga",
            "tpm",
            "tpa",
            "ftm",
            "fta",
            "oreb",
            "dreb",
            "tov",
            "stl",
            "ast",
            "blk",
            "shots_blocked",
            "pf",
            "pf_drawn",
            "plus_minus",
            "is_home",
        ]
    ]
    teams_df["key_col"] = teams_df["game_id"].astype(str) + teams_df["team_abbrev"]

    teams_df.to_sql(
        "teambygamestats",
        engine,
        schema="nba",
        method="multi",
        if_exists="append",
        index=False,
    )


def calc_possessions(game_df, engine):
    """
    funciton to calculate possesion numbers for both team and players
    and insert into possesion tables

    Inputs:
    game_df  - dataframe of nba play by play
    engine   - sql alchemy engine

    Outputs:
    None
    """
    # calculating made shot possessions
    game_df["home_possession"] = np.where(
        (game_df.event_team == game_df.home_team_abbrev)
        & (game_df.event_type_de == "shot"),
        1,
        0,
    )
    # calculating turnover possessions
    game_df["home_possession"] = np.where(
        (game_df.event_team == game_df.home_team_abbrev)
        & (game_df.event_type_de == "turnover"),
        1,
        game_df["home_possession"],
    )
    # calculating defensive rebound possessions
    game_df["home_possession"] = np.where(
        ((game_df.event_team == game_df.away_team_abbrev) & (game_df.is_d_rebound == 1))
        | (
            (game_df.event_type_de == "rebound")
            & (game_df.is_d_rebound == 0)
            & (game_df.is_o_rebound == 0)
            & (game_df.event_team == game_df.away_team_abbrev)
            & (game_df.event_type_de.shift(1) != "free-throw")
        ),
        1,
        game_df["home_possession"],
    )
    # calculating final free throw possessions
    game_df["home_possession"] = np.where(
        (game_df.event_team == game_df.home_team_abbrev)
        & (
            (game_df.homedescription.str.contains("Free Throw 2 of 2"))
            | (game_df.homedescription.str.contains("Free Throw 3 of 3"))
        ),
        1,
        game_df["home_possession"],
    )
    # calculating made shot possessions
    game_df["away_possession"] = np.where(
        (game_df.event_team == game_df.away_team_abbrev)
        & (game_df.event_type_de == "shot"),
        1,
        0,
    )
    # calculating turnover possessions
    game_df["away_possession"] = np.where(
        (game_df.event_team == game_df.away_team_abbrev)
        & (game_df.event_type_de == "turnover"),
        1,
        game_df["away_possession"],
    )
    # calculating defensive rebound possessions
    game_df["away_possession"] = np.where(
        ((game_df.event_team == game_df.home_team_abbrev) & (game_df.is_d_rebound == 1))
        | (
            (game_df.event_type_de == "rebound")
            & (game_df.is_d_rebound == 0)
            & (game_df.is_o_rebound == 0)
            & (game_df.event_team == game_df.home_team_abbrev)
            & (game_df.event_type_de.shift(1) != "free-throw")
        ),
        1,
        game_df["away_possession"],
    )
    # calculating final free throw possessions
    game_df["away_possession"] = np.where(
        (game_df.event_team == game_df.away_team_abbrev)
        & (
            (game_df.visitordescription.str.contains("Free Throw 2 of 2"))
            | (game_df.visitordescription.str.contains("Free Throw 3 of 3"))
        ),
        1,
        game_df["away_possession"],
    )
    # calculating player possesions
    player1 = game_df[
        [
            "home_player_1",
            "home_player_1_id",
            "home_possession",
            "game_id",
            "home_team_id",
        ]
    ].rename(columns={"home_player_1": "player_name", "home_player_1_id": "player_id"})
    player2 = game_df[
        [
            "home_player_2",
            "home_player_2_id",
            "home_possession",
            "game_id",
            "home_team_id",
        ]
    ].rename(columns={"home_player_2": "player_name", "home_player_2_id": "player_id"})
    player3 = game_df[
        [
            "home_player_3",
            "home_player_3_id",
            "home_possession",
            "game_id",
            "home_team_id",
        ]
    ].rename(columns={"home_player_3": "player_name", "home_player_3_id": "player_id"})
    player4 = game_df[
        [
            "home_player_4",
            "home_player_4_id",
            "home_possession",
            "game_id",
            "home_team_id",
        ]
    ].rename(columns={"home_player_4": "player_name", "home_player_4_id": "player_id"})
    player5 = game_df[
        [
            "home_player_5",
            "home_player_5_id",
            "home_possession",
            "game_id",
            "home_team_id",
        ]
    ].rename(columns={"home_player_5": "player_name", "home_player_5_id": "player_id"})
    home_possession_df = pd.concat([player1, player2, player3, player4, player5])
    home_possession_df = (
        home_possession_df.groupby(
            ["player_id", "player_name", "game_id", "home_team_id"]
        )["home_possession"]
        .sum()
        .reset_index()
        .sort_values("home_possession")
    )
    player1 = game_df[
        [
            "away_player_1",
            "away_player_1_id",
            "away_possession",
            "game_id",
            "away_team_id",
        ]
    ].rename(columns={"away_player_1": "player_name", "away_player_1_id": "player_id"})
    player2 = game_df[
        [
            "away_player_2",
            "away_player_2_id",
            "away_possession",
            "game_id",
            "away_team_id",
        ]
    ].rename(columns={"away_player_2": "player_name", "away_player_2_id": "player_id"})
    player3 = game_df[
        [
            "away_player_3",
            "away_player_3_id",
            "away_possession",
            "game_id",
            "away_team_id",
        ]
    ].rename(columns={"away_player_3": "player_name", "away_player_3_id": "player_id"})
    player4 = game_df[
        [
            "away_player_4",
            "away_player_4_id",
            "away_possession",
            "game_id",
            "away_team_id",
        ]
    ].rename(columns={"away_player_4": "player_name", "away_player_4_id": "player_id"})
    player5 = game_df[
        [
            "away_player_5",
            "away_player_5_id",
            "away_possession",
            "game_id",
            "away_team_id",
        ]
    ].rename(columns={"away_player_5": "player_name", "away_player_5_id": "player_id"})
    away_possession_df = pd.concat([player1, player2, player3, player4, player5])
    away_possession_df = (
        away_possession_df.groupby(
            ["player_id", "player_name", "game_id", "away_team_id"]
        )["away_possession"]
        .sum()
        .reset_index()
        .sort_values("away_possession")
    )

    home_possession_df = home_possession_df.rename(
        columns={"home_team_id": "team_id", "home_possession": "possessions"}
    )
    away_possession_df = away_possession_df.rename(
        columns={"away_team_id": "team_id", "away_possession": "possessions"}
    )
    possession_df = pd.concat([home_possession_df, away_possession_df])

    row1 = [
        game_df.home_team_id.unique()[0],
        game_df.game_id.unique()[0],
        game_df.home_team_abbrev.unique()[0],
        game_df["home_possession"].sum(),
    ]
    row2 = [
        game_df.away_team_id.unique()[0],
        game_df.game_id.unique()[0],
        game_df.away_team_abbrev.unique()[0],
        game_df["away_possession"].sum(),
    ]
    team_possession_df = pd.DataFrame(
        [row1, row2], columns=["team_id", "game_id", "team_abbrev", "possessions"]
    )

    possession_df["key_col"] = possession_df["player_id"].astype(str) + possession_df[
        "game_id"
    ].astype(str)
    team_possession_df["key_col"] = team_possession_df["team_id"].astype(
        str
    ) + team_possession_df["game_id"].astype(str)

    possession_df["game_id"] = possession_df["game_id"].astype(int)
    team_possession_df["game_id"] = team_possession_df["game_id"].astype(int)

    possession_df.to_sql(
        "player_possessions",
        engine,
        schema="nba",
        if_exists="append",
        index=False,
        method="multi",
    )

    team_possession_df.to_sql(
        "team_possessions",
        engine,
        schema="nba",
        if_exists="append",
        index=False,
        method="multi",
    )


def parse_rapm_possessions(game_df, engine):
    """
    funciton to parse possessions for RAPM calculation
    Inputs:
    game_df  - pandas dataframe of play by play
    engine  - SQL ALchemy Engine

    Outputs:
    None
    """
    game_df = cgs.calc_possessions(game_df)
    points_by_second = (
        game_df.groupby(["game_id", "seconds_elapsed"])["points_made"]
        .sum()
        .reset_index()
    )
    poss_index = game_df[
        (game_df.home_possession == 1) | (game_df.away_possession == 1)
    ].index
    shift_dfs = []
    past_index = 0
    for i in poss_index:
        shift_dfs.append(game_df.iloc[past_index + 1 : i + 1, :].reset_index())
        past_index = i
    possession = [
        x.merge(points_by_second, on=["game_id", "seconds_elapsed"]) for x in shift_dfs
    ]
    poss_df = pd.concat(cgs.parse_possessions(possession))
    poss_df.to_sql(
        "rapm_shifts",
        engine,
        schema="nba",
        if_exists="append",
        index=False,
        method="multi",
    )


def get_game_ids(date):
    """
    This function gets the game ids of games returned from the api
    endpoint

    Inputs:
    date - date which to get the game ids of games played

    Ouputs:
    game_ids - list of game ids
    """

    games_api = (
        "https://stats.nba.com/stats/scoreboard?"
        f'DayOffset=0&GameDate={date.strftime("%Y-%m-%d")}&LeagueID=00'
    )
    print(games_api)
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
    scoreboard = requests.get(games_api, headers=user_agent).json()

    games = scoreboard["resultSets"][0]["rowSet"]
    game_ids = [game[2][2:] for game in games]
    return game_ids


def get_player_details(players, engine):
    """
    this function will check and see if the players in the play by play dataframe
    are in the player_details table and if not will hit the nba player api get
    them and insert them into the database
    """
    # TODO this needs to be finished at some point
    # for player in players:
    # url = f'https://stats.nba.com/stats/commonplayerinfo?LeagueID=&PlayerID={p}'

    pass


def main(days_back):
    """
    Main function to run to scrape games daily
    """

    engine = create_engine(os.environ["NBA_CONNECT_DEV"])

    # Logging stuff
    logging.basicConfig(
        level=logging.INFO,
        filename="batchprocess.logs",
        format="%(asctime)s - %(levelname)s: %(message)s",
    )

    # TODO uncomment this when testing is done
    # get the game ids of the games played yesterday
    # yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = datetime.datetime.now() - datetime.timedelta(days=days_back)
    games = get_game_ids(yesterday)
    print(games)
    # date = '2019-04-04'
    # games_api = ('https://stats.nba.com/stats/scoreboard?'
    #             f'DayOffset=0&GameDate={date}&LeagueID=00')

    if games == []:
        logging.info("No games on %s", yesterday)
        return
    # creates a list of play by play dataframes to process
    games_df_list = []
    for game in games:
        games_df_list.append(ns.scrape_game([game]))
        time.sleep(1)

    for game_df in games_df_list:
        game_df.columns = list(map(str.lower, game_df.columns))
        game_df["game_id"] = game_df["game_id"].str.slice(start=2)
        game_df["key_col"] = (
            game_df["game_id"].astype(str)
            + game_df["eventnum"].astype(str)
            + game_df["game_date"].astype(str)
            + game_df["home_team_abbrev"]
            + game_df["away_team_abbrev"]
            + game_df["seconds_elapsed"].astype(str)
            + game_df.index.astype(str)
            + str(round(random.random() * 10, 0))
        )
        game_df = game_df.astype(
            {
                "season": int,
                "is_d_rebound": bool,
                "is_o_rebound": bool,
                "is_turnover": bool,
                "is_steal": bool,
                "is_putback": bool,
                "is_block": bool,
                "is_three": bool,
                "shot_made": bool,
                "home_player_1_id": int,
                "home_player_2_id": int,
                "home_player_3_id": int,
                "home_player_4_id": int,
                "home_player_5_id": int,
                "away_player_2_id": int,
                "away_player_1_id": int,
                "away_player_3_id": int,
                "away_player_4_id": int,
                "away_player_5_id": int,
            }
        )
        game_df.drop(["video_available_flag"], axis=1, inplace=True)
        try:
            game_df.to_sql(
                "pbp",
                engine,
                schema="nba",
                if_exists="append",
                index=False,
                method="multi",
            )
            logging.info(
                "Inserting play by play for %s into nba.playerbygamestats",
                game_df.game_id.unique()[0],
            )
        except Exception as e:
            print(e)
            logging.error(
                "Play by play for %s failed to insert into nba.pbp",
                game_df.game_id.unique()[0],
            )

        # TODO rewrite this sql query i use for this into a python funciton
        # TODO to make debuggin easier in the future 9-2-2019
        logging.info(
            "Inserting player boxscore for %s into nba.playerbygamestats",
            game_df.game_id.unique()[0],
        )
        engine.connect().execute(
            sqlqueries.pbgs_calc.format(game_id=game_df.game_id.unique()[0])
        )

        logging.info(
            "Inserting team boxscore for %s into nba.teambygamestats",
            game_df.game_id.unique()[0],
        )
        calc_team_stats(game_df, engine)

        logging.info(
            "Inserting possession data for %s into nba.team_possesions and nba.player_possesions ",
            game_df.game_id.unique()[0],
        )
        calc_possessions(game_df, engine)
        # TODO pull in shots data for game and insert into database
        logging.info(
            "Inserting shot data for %s into nba.shot_locations",
            game_df.game_id.unique()[0],
        )
        parse_player_shots(game_df, engine)
        # TODO parse player details if player not in the database
        logging.info(
            "parsing new player details for %s into nba.player_details",
            game_df.game_id.unique()[0],
        )
        parse_player_details(game_df, engine)
        logging.info(
            "parsing rapm_possessions for %s and inserting into nba.rapm_shifts",
            game_df.game_id.unique()[0],
        )
        parse_rapm_possessions(game_df, engine)

    logging.info(
        "Inserting player advanced stats data for %s into nba.teambygamestats",
        game_df.season.unique()[0],
    )
    calc_player_advanced_stats(game_df["season"].unique()[0], engine)
    logging.info(
        "Inserting team advanced stats data for %s into nba.teambygamestats",
        game_df.season.unique()[0],
    )
    calc_team_advanced_stats(game_df["season"].unique()[0], engine)
    logging.info("Calculating one year rapm stats for %s", game_df.season.unique()[0])
    one_year_team_rapm_calc(game_df["season"].unique()[0], engine)
    one_year_rapm_calc(game_df["season"].unique()[0], engine)
    multi_seasons = [
        game_df["season"].unique()[0],
        game_df["season"].unique()[0] - 1,
        game_df["season"].unique()[0] - 2,
    ]
    logging.info("Calculating multi year rapm")
    multi_year_rapm_calc(multi_seasons, engine)


if __name__ == "__main__":
    main(1)
