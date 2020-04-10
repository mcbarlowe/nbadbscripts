"""
This will be the main method I run every day to get the games from yesterdays
schedule, scrape them and then insert them into the database. I will then
calculate RAPMS for single year team and player and multi season rapm for the
current season. This will use my nba_scraper, nba_parser, and new API models
I'm creating for the NBA api to streamline code
"""

import os
import argparse
import datetime
import logging
import time
import pandas as pd
import nba_scraper.nba_scraper as ns
import nba_parser as npar
from nba_api_models.scoreboard_api import ScoreBoard
from nba_api_models.player_details import PlayerDetails
from nba_api_models.shot_details_api import ShotDetails
from sqlalchemy import create_engine


def parse_shot_details(pbg_df, engine):
    """
    Inputs:
    pbg_df  - pandas dataframe of playerbygamestats
    engine  - SQL ALchemy Engine

    Outputs:
    None
    """
    shots_df_list = []
    players = pbg_df["player_id"].unique()
    game_id = pbg_df["game_id"].unique()[0][2:]

    for player in players:
        shot_details = ShotDetails(game_id, player)
        shot_details_df = shot_details.response()
        shots_df_list.append(shot_details_df)
        time.sleep(1)

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
    players = game_df["player_id"].unique()

    new_players = [p for p in players if p not in player_ids]

    if new_players == []:
        return

    for p in new_players:
        player = PlayerDetails(p)
        player_df = player.response()
        player_df.to_sql(
            "player_details",
            engine,
            schema="nba",
            method="multi",
            if_exists="append",
            index=False,
        )


def main():
    # Logging stuff
    logging.basicConfig(
        level=logging.INFO,
        filename="batchprocess.logs",
        format="%(asctime)s - %(levelname)s: %(message)s",
    )

    # Get all playerids currently in database to check against to see if I need
    # to pull the details of the player ids I scrape
    engine = create_engine(os.environ["NBA_CONNECT"])
    sql = """select distinct player_id from nba.player_details"""
    data = engine.connect().execute(sql)
    player_ids = [row.player_id for row in data]

    # parse arguments passed to script at command line
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int)
    parser.add_argument("--month", type=int)
    parser.add_argument("--day", type=int)

    args = parser.parse_args()
    year = args.year
    month = args.month
    day = args.day

    date = datetime.date(year, month, day)

    score_board = ScoreBoard(date)
    games = score_board.response()

    if games == []:
        logging.info("No games on %s", date.strftime("%Y-%m-%d"))
        return
    # creates a list of play by play dataframes to process
    games_df_list = []
    # TODO add code in here to filter out all star and playoff games or
    # to add a gametype in the nba_parser 2020-04-06
    for game in games:
        try:
            games_df_list.append(ns.scrape_game([game]))
            time.sleep(1)
        except IndexError:
            logging.error("Could not scrape game %s", game)
        except KeyError:
            logging.error("Could not scrape game %s", game)

    pbps = list(map(npar.PbP, games_df_list))

    # method to calculate and insert teambygamestats,
    # playerbygamestats, player_rapm_shifts, shot details, and player details
    # if there are players not in the database
    for pbp in pbps:
        print(pbp.df.game_id.unique())
        pbg = pbp.playerbygamestats()
        pbp.df = pbp.df.astype(
            {
                "season": int,
                "eventnum": int,
                "eventmsgtype": int,
                "eventmsgactiontype": int,
                "period": int,
                "person1type": float,
                "player1_id": float,
                "person2type": float,
                "player2_id": float,
                "person3type": float,
                "player3_id": float,
                "video_available_flag": int,
                "home_team_id": int,
                "away_team_id": int,
                "is_block": int,
                "seconds_elapsed": int,
                "is_three": int,
                "points_made": int,
                "is_o_rebound": int,
                "is_d_rebound": int,
                "is_turnover": int,
                "is_steal": int,
                "is_putback": int,
                "home_possession": int,
                "away_possession": int,
                "fgm": int,
                "fga": int,
                "tpm": int,
                "tpa": int,
                "ftm": int,
                "fta": int,
                "home_plus": int,
                "home_minus": int,
                "away_plus": int,
                "away_minus": int,
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
            },
        )
        pbp.df.drop(columns="shot_type_de", inplace=True)
        tbg = pbp.teambygamestats()
        rapm_shifts = pbp.rapm_possessions()
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
        parse_player_details(pbg, engine, player_ids)
        parse_shot_details(pbg, engine)


if __name__ == "__main__":
    main()
