import os
import time
from nba_api_models.shot_details_api import ShotDetails
from sqlalchemy import create_engine, text

engine = create_engine(os.environ["NBA_CONNECT"])
sql = """select distinct player_id, pbg.game_id from nba.playerbygamestats pbg join (select blah1.game_id from (select distinct game_id from nba.teambygamestats) blah1 left join (select distinct game_id from nba.shot_locations) blah2 on blah1.game_id = blah2.game_id where blah2.game_id is null) b on b.game_id = pbg.game_id;"""
players = engine.connect().execute(text(sql))
for player in players:
    print(f"Scraping player {player.player_id} shots from game {player.game_id}")
    shot_details = ShotDetails(player.game_id, player.player_id)
    shot_details_df = shot_details.response()

    shot_details_df["key_col"] = (
        shot_details_df["player_id"].astype(str)
        + shot_details_df["game_id"].astype(str)
        + shot_details_df["game_event_id"].astype(str)
    )
    shot_details_df.to_sql(
        "shot_locations",
        engine,
        schema="nba",
        if_exists="append",
        index=False,
        method="multi",
    )
    time.sleep(1)
