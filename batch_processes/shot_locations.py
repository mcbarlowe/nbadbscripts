import requests
import pandas as pd
import time
import os
from sqlalchemy import create_engine

engine = create_engine(os.environ['NBA_CONNECT_DEV'])

USER_AGENT = {'User-agent': 'Mozilla/5.0'}

player_ids = engine.connect().execute("SELECT distinct player_id from nba.player_details")
player_ids = [x[0] for x in list(player_ids)]



for i, player in enumerate(player_ids):
    print(f"Scraping {player} at index: {i}")
    seasons = ['2016-17', '2017-18', '2018-19']
    shots_df_list = []
    for season in seasons:
        print(f"Scraping season: {season}")
        # ContextMeasure has to be FGA to get made and missed shots
        url = ('https://stats.nba.com/stats/shotchartdetail?AheadBehind='
            '&ClutchTime=&ContextFilter=&ContextMeasure=FGA&DateFrom='
            '&DateTo=&EndPeriod=&EndRange=&GameID=&GameSegment=&LastNGames=0'
            '&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&Period=0'
            f'&PlayerID={player}&PlayerPosition=&PointDiff=&Position=&RangeType='
            f'&RookieYear=&Season={season}&SeasonSegment=&SeasonType=Regular+Season'
            '&StartPeriod=&StartRange=&TeamID=0&VsConference=&VsDivision=')
        shots = requests.get(url, headers=USER_AGENT).json()
        columns = shots['resultSets'][0]['headers']
        rows = shots['resultSets'][0]['rowSet']
        shots_df = pd.DataFrame(rows, columns=columns)
        shots_df.columns = list(map(str.lower, shots_df.columns))
        shots_df['game_id'] = shots_df['game_id'].str.slice(start=2).astype(int)
        shots_df_list.append(shots_df)
        if season != '2018-19':
            time.sleep(5)

    print("concatting dataframes")
    total_shots = pd.concat(shots_df_list)
    total_shots['key_col'] = total_shots['player_id'].astype(str) + total_shots['game_id'].astype(str) +\
                             total_shots['game_event_id'].astype(str)
    print("inserting into database")
    total_shots.to_sql('shot_locations', engine, schema='nba', if_exists='append',
                       index=False, method='multi')
