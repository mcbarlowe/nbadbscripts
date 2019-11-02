from sqlalchemy import create_engine
import os
season = 2020
engine = create_engine(os.environ['NBA_CONNECT'])
engine.connect().execute(f'DELETE from nba.player_advanced_stats where season = {season};')
engine.connect().execute(f'DELETE from nba.pbp where season = {season};')
engine.connect().execute(f'DELETE from nba.playerbygamestats where season = {season};')
engine.connect().execute(f'DELETE from nba.teambygamestats where season = {season};')
engine.connect().execute(f'DELETE from nba.player_possessions where game_id > 21900000;')
engine.connect().execute(f'DELETE from nba.team_possessions where game_id > 21900000;')
engine.connect().execute(f'DELETE from nba.shot_locations where game_id > 21900000;')
engine.connect().execute(f'DELETE from nba.team_advanced_stats where season = {season};')
engine.connect().execute(f'DELETE from nba.team_single_year_rapm where season = 2020;')
engine.connect().execute(f'DELETE from nba.player_single_year_rapm where season = 2020;')
engine.connect().execute("DELETE from nba.player_multi_year_rapm where season = '2018-20';")
