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
import nba_scraper.nba_scraper as ns
import nba_parser as npar
from nba_api_models.scoreboard_api import ScoreBoard
from nba_api_models.player_details import PlayerDetails
from sqlalchemy import create_engine


def parse_shot_details(game_df, engine):
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
    for game in games:
        try:
            games_df_list.append(ns.scrape_game([game]))
            time.sleep(1)
        except IndexError:
            logging.error("Could not scrape game %s", game)
    pbps = list(map(npar.Pbp, games_df_list))

    # method to calculate and insert teambygamestats,
    # playerbygamestats, player_rapm_shifts, shot details, and player details
    # if there are players not in the database
    for pbp in pbps:
        pbg = pbp.playerbygamestats()
        tbg = pbp.teambygamestats()
        rapm_shifts = pbp.rapm_possessions()
        parse_player_details(pbg, engine, player_ids)
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


# TODO build method to get player shots for the database


if __name__ == "__main__":
    main()
