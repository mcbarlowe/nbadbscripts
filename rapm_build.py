"""
This script will build the one year team, one year player, and three year player
rapm tables from the data in the database and then insert them into the database
for theseventhman.net website
"""

import os
from sqlalchemy import create_engine
import pandas as pd
import nba_parser as npar


engine = create_engine(os.environ["NBA_CONNECT"])

seasons = engine.execute(
    "select distinct season from nba.teambygamestats order by season"
)


for s in seasons:
    print(f"calculating {s.season} single season player rapm")
    pbg_sql = f"select * from nba.player_rapm_shifts where season = {s.season}"
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

    print(f"calculating {s.season} single season team rapm")
    tbg_sql = f"select * from nba.teambygamestats where season = {s.season}"
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
    if s.season < 2002:
        continue
    else:
        print(f"calculating {s.season} multi season player rapm")
        multi_pbg_sql = f"select * from nba.player_rapm_shifts where season in ({s.season}, {s.season - 1}, {s.season-2})"
        multi_pbg_df = pd.read_sql(pbg_sql, engine)

        pbg_rapm_results = npar.PlayerTotals.player_rapm_results(multi_pbg_df)

        pbg_rapm_results.to_sql(
            "player_three_year_rapm",
            con=engine,
            schema="nba",
            if_exists="append",
            index=False,
            method="multi",
        )
