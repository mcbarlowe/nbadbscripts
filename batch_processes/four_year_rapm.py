import os
from sqlalchemy import create_engine
import pandas as pd
import nba_parser as npar


engine = create_engine(os.environ["NBA_CONNECT"])
min_season = 2017
max_season = 2020

print(f"calculating {min_season}-{max_season} player rapm")
pbg_sql = f"select * from nba.player_rapm_shifts where season between {min_season} and {max_season} and (game_id >= 20000000 and game_id < 30000000)"
rapm_df = pd.read_sql(pbg_sql, engine)

pbg_rapm_results = npar.PlayerTotals.player_rapm_results(rapm_df)

pbg_rapm_results.to_csv('~/four_year_rapm.csv')
