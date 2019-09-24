import requests
import os
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine(os.environ['NBA_CONNECT_DEV'])
con = engine.connect()
USER_AGENT = {'User-agent': 'Mozilla/5.0'}

sql = """select distinct player_id from nba.playerbygamestats where toc > 0"""
data = con.execute(sql)
player_ids = [row[0] for row in data]
for p in player_ids:
    url = f'https://stats.nba.com/stats/commonplayerinfo?LeagueID=&PlayerID={p}'
    info = requests.get(url, headers=USER_AGENT).json()
    headers = list(map(str.lower, info['resultSets'][0]['headers']))
    player = info['resultSets'][0]['rowSet'][0]
    player_dict = {}
    for play, head in zip(player, headers):
        player_dict[head] = [play]
    print(f"Inserting {player_dict['display_first_last']} into player database")
    player_df = pd.DataFrame.from_dict(player_dict)
    player_df = player_df.rename(columns={'person_id': 'player_id', 'season_exp': 'season_experience',
                      'jersey': 'jersey_number'})
    player_df.to_sql('player_details', engine, schema='nba', method='multi',
                     if_exists='append', index=False)

