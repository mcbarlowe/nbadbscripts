"""
This script will setup tables on PostgreSQL database
"""
import argparse
import logging
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Boolean, Date, Numeric


def main():
    """
    Function to create all tables on the database
    """
    # Logging stuff
    logging.basicConfig(
        level=logging.INFO,
        filename="dbcreate.logs",
        format="%(asctime)s - %(levelname)s: %(message)s",
    )
    # Adding an arg parser here so I can pass sensitive connection strings with
    # environment variables
    parser = argparse.ArgumentParser(
        description="Program to create needed tables on nba database"
    )
    parser.add_argument("--con", help="Connection string for SQL Alchemy create_engine")
    args = parser.parse_args()

    # TODO: remove echo=True from this when ready for production
    engine = create_engine(args.con, echo=True)
    engine.execute("CREATE SCHEMA IF NOT EXISTS nba")

    Base = declarative_base()

    class Pbp(Base):
        """
        Class to create the play by play table
        """

        __tablename__ = "pbp"
        __table_args__ = {"schema": "nba"}

        key_col = Column(String, primary_key=True, nullable=False)
        game_id = Column(Integer, index=True)
        eventnum = Column(Integer)
        eventmsgtype = Column(Integer)
        eventmsgactiontype = Column(Integer)
        period = Column(Integer)
        wctimestring = Column(String)
        pctimestring = Column(String)
        homedescription = Column(String)
        neutraldescription = Column(String)
        visitordescription = Column(String)
        score = Column(String)
        scoremargin = Column(String)
        person1type = Column(Integer)
        player1_id = Column(Integer)
        player1_name = Column(String)
        player1_team_id = Column(Integer)
        player1_team_city = Column(String)
        player1_team_nickname = Column(String)
        player1_team_abbreviation = Column(String)
        person2type = Column(Integer)
        player2_id = Column(Integer)
        player2_name = Column(String)
        player2_team_id = Column(Integer)
        player2_team_city = Column(String)
        player2_team_nickname = Column(String)
        player2_team_abbreviation = Column(String)
        person3type = Column(Integer)
        player3_id = Column(Integer)
        player3_name = Column(String)
        player3_team_id = Column(Integer)
        player3_team_city = Column(String)
        player3_team_nickname = Column(String)
        player3_team_abbreviation = Column(String)
        evt = Column(Integer)
        locx = Column(Integer)
        locy = Column(Integer)
        hs = Column(Integer)
        vs = Column(Integer)
        de = Column(String)
        home_team_abbrev = Column(String)
        away_team_abbrev = Column(String)
        game_date = Column(Date)
        season = Column(Integer)
        home_team_id = Column(Integer)
        away_team_id = Column(Integer)
        event_team = Column(String)
        event_type_de = Column(String)
        shot_type_de = Column(String)
        shot_made = Column(Boolean)
        is_block = Column(Boolean)
        shot_type = Column(String)
        seconds_elapsed = Column(Integer)
        event_length = Column(Integer)
        is_three = Column(Boolean)
        points_made = Column(Integer)
        is_d_rebound = Column(Boolean)
        is_o_rebound = Column(Boolean)
        is_turnover = Column(Boolean)
        is_steal = Column(Boolean)
        foul_type = Column(String)
        is_putback = Column(Boolean)
        home_player_1 = Column(String)
        home_player_1_id = Column(Integer)
        home_player_2 = Column(String)
        home_player_2_id = Column(Integer)
        home_player_3 = Column(String)
        home_player_3_id = Column(Integer)
        home_player_4 = Column(String)
        home_player_4_id = Column(Integer)
        home_player_5 = Column(String)
        home_player_5_id = Column(Integer)
        away_player_1 = Column(String)
        away_player_1_id = Column(Integer)
        away_player_2 = Column(String)
        away_player_2_id = Column(Integer)
        away_player_3 = Column(String)
        away_player_3_id = Column(Integer)
        away_player_4 = Column(String)
        away_player_4_id = Column(Integer)
        away_player_5 = Column(String)
        away_player_5_id = Column(Integer)

    class playerbygamestats(Base):
        """
        Class to create the playerbygamestats table which is each players
        box score for every game they played in.
        """

        __tablename__ = "playerbygamestats"
        __table_args__ = {"schema": "nba"}
        key_col = Column(String, primary_key=True, nullable=False)
        season = Column(Integer)
        game_date = Column(Date)
        game_id = Column(Integer)
        player_id = Column(Integer)
        player_name = Column(String)
        team_abbrev = Column(String)
        team_id = Column(Integer)
        toc = Column(Integer)
        toc_string = Column(String)
        fgm = Column(Integer)
        fga = Column(Integer)
        tpm = Column(Integer)
        tpa = Column(Integer)
        ftm = Column(Integer)
        fta = Column(Integer)
        oreb = Column(Integer)
        dreb = Column(Integer)
        ast = Column(Integer)
        tov = Column(Integer)
        stl = Column(Integer)
        blk = Column(Integer)
        pf = Column(Integer)
        points = Column(Integer)
        plus_minus = Column(Integer)

    class teambygamestats(Base):
        """
        Class to create the teambygamestats table which is each teams
        box score for every game they played in.
        """

        __tablename__ = "teambygamestats"
        __table_args__ = {"schema": "nba"}
        key_col = Column(String, primary_key=True, nullable=False)
        season = Column(Integer)
        game_date = Column(Date)
        game_id = Column(Integer)
        team_abbrev = Column(String)
        team_id = Column(Integer)
        toc = Column(Integer)
        toc_string = Column(String)
        points_for = Column(Integer)
        points_against = Column(Integer)
        is_win = Column(Integer)
        fgm = Column(Integer)
        fga = Column(Integer)
        tpm = Column(Integer)
        tpa = Column(Integer)
        ftm = Column(Integer)
        fta = Column(Integer)
        oreb = Column(Integer)
        dreb = Column(Integer)
        ast = Column(Integer)
        tov = Column(Integer)
        stl = Column(Integer)
        blk = Column(Integer)
        shots_blocked = Column(Integer)
        pf = Column(Integer)
        pf_drawn = Column(Integer)
        points = Column(Integer)
        plus_minus = Column(Integer)
        is_home = Column(Integer)

    class team_details(Base):
        """
        Class to create table for team details
        """

        __tablename__ = "team_details"
        __table_args__ = {"schema": "nba"}
        team_id = Column(Integer, primary_key=True, nullable=False)
        abbreviation = Column(String)
        nickname = Column(String)
        yearfounded = Column(Integer)
        city = Column(String)
        arena = Column(String)
        arena_capacity = Column(Integer)
        owner = Column(String)
        generalmanager = Column(String)
        headcoach = Column(String)
        dleagueaffiliation = Column(String)

    class player_details(Base):
        """
        Class to build table with player info
        """

        __tablename__ = "player_details"
        __table_args__ = {"schema": "nba"}
        player_id = Column(Integer, primary_key=True, nullable=False)
        first_name = Column(String)
        last_name = Column(String)
        display_first_last = Column(String)
        display_last_comma_first = Column(String)
        display_fi_last = Column(String)
        birthdate = Column(Date)
        school = Column(String)
        country = Column(String)
        last_affiliation = Column(String)
        height = Column(String)
        weight = Column(String)
        season_experience = Column(Integer)
        jersey_number = Column(String)
        position = Column(String)
        rosterstatus = Column(String)
        team_id = Column(Integer)
        team_name = Column(String)
        team_abbreviation = Column(String)
        team_code = Column(String)
        team_city = Column(String)
        playercode = Column(String)
        from_year = Column(Integer)
        to_year = Column(Integer)
        dleague_flag = Column(String)
        nba_flag = Column(String)
        games_played_flag = Column(String)
        draft_year = Column(String)
        draft_round = Column(String)
        draft_number = Column(String)

    class player_possessions(Base):
        """
        creates table for player possesions totals
        """

        __tablename__ = "player_possessions"
        __table_args__ = {"schema": "nba"}
        key_col = Column(String, primary_key=True, nullable=False)
        player_id = Column(Integer)
        player_name = Column(String)
        game_id = Column(Integer)
        team_id = Column(Integer)
        possessions = Column(Integer)

    class team_possessions(Base):
        """
        creates table for team possesions totals
        """

        __tablename__ = "team_possessions"
        __table_args__ = {"schema": "nba"}
        key_col = Column(String, primary_key=True, nullable=False)
        team_id = Column(Integer)
        game_id = Column(Integer)
        team_abbrev = Column(String)
        possessions = Column(Integer)

    class player_advanced(Base):
        """
        creates table for player advanced stats
        """

        __tablename__ = "player_advanced_stats"
        __table_args__ = {"schema": "nba"}
        key_col = Column(String, primary_key=True, nullable=False)
        player_id = Column(Integer)
        season = Column(Integer)
        team_abbrev = Column(String)
        efg_percentage = Column(Numeric)
        true_shooting_percentage = Column(Numeric)
        oreb_percentage = Column(Numeric)
        dreb_percentage = Column(Numeric)
        treb_percentage = Column(Numeric)
        ast_percentage = Column(Numeric)
        stl_percentage = Column(Numeric)
        blk_percentage = Column(Numeric)
        tov_percentage = Column(Numeric)
        usg_percentage = Column(Numeric)
        off_rating = Column(Numeric)
        def_rating = Column(Numeric)

    class team_advanced(Base):
        """
        creates table for team advanced stats
        """

        __tablename__ = "team_advanced_stats"
        __table_args__ = {"schema": "nba"}
        key_col = Column(String, primary_key=True, nullable=False)
        team_id = Column(Integer)
        team_abbrev = Column(String)
        season = Column(Integer)
        efg_percentage = Column(Numeric)
        true_shooting_percentage = Column(Numeric)
        tov_percentage = Column(Numeric)
        oreb_percentage = Column(Numeric)
        ft_per_fga = Column(Numeric)
        opp_efg_percentage = Column(Numeric)
        opp_tov_percentage = Column(Numeric)
        dreb_percentage = Column(Numeric)
        opp_ft_per_fga = Column(Numeric)
        off_rating = Column(Numeric)
        def_rating = Column(Numeric)

    # TODO finish later based on rapm_shifts table in database
    # class game_shifts(Base):

    class player_single_year_rapm(Base):
        """
        creates table for player single year rapm stats
        """

        __tablename__ = "player_single_year_rapm"
        __table_args__ = {"schema": "nba"}
        key_col = Column(String, primary_key=True, nullable=False)
        player_id = Column(Integer)
        season = Column(Integer)
        rapm_off = Column(Numeric)
        rapm_def = Column(Numeric)
        rapm = Column(Numeric)
        rapm_rank = Column(Integer)
        rapm_off_rank = Column(Integer)
        rapm_def_rank = Column(Integer)
        player_name = Column(String)

    class player_multi_year_rapm(Base):
        """
        creates table for player multi year rapm stats
        """

        __tablename__ = "player_multi_year_rapm"
        __table_args__ = {"schema": "nba"}
        key_col = Column(String, primary_key=True, nullable=False)
        player_id = Column(Integer)
        season = Column(String)
        rapm_off = Column(Numeric)
        rapm_def = Column(Numeric)
        rapm = Column(Numeric)
        rapm_rank = Column(Integer)
        rapm_off_rank = Column(Integer)
        rapm_def_rank = Column(Integer)
        player_name = Column(String)

    class team_single_year_rapm(Base):
        """
        creates table for player single year rapm stats
        """

        __tablename__ = "team_single_year_rapm"
        __table_args__ = {"schema": "nba"}
        key_col = Column(String, primary_key=True, nullable=False)
        team_id = Column(Integer)
        season = Column(Integer)
        rapm_off = Column(Numeric)
        rapm_def = Column(Numeric)
        rapm = Column(Numeric)
        rapm_rank = Column(Integer)
        rapm_off_rank = Column(Integer)
        rapm_def_rank = Column(Integer)
        abbreviation = Column(String)

    class shot_locations(Base):
        """
        model for the shot locations table
        """

        __tablename__ = "shot_locations"
        __table_args__ = {"schema": "nba"}
        key_col = Column(String, primary_key=True, nullable=False)
        grid_type = Column(String)
        game_id = Column(Integer)
        game_event_id = Column(Integer)
        player_id = Column(Integer)
        player_name = Column(String)
        team_id = Column(Integer)
        team_name = Column(String)
        period = Column(Integer)
        minutes_remaining = Column(Integer)
        seconds_remaining = Column(Integer)
        event_type = Column(String)
        action_type = Column(String)
        shot_type = Column(String)
        shot_zone_basic = Column(String)
        shot_zone_area = Column(String)
        shot_zone_range = Column(String)
        shot_distance = Column(Integer)
        loc_x = Column(Integer)
        loc_y = Column(Integer)
        shot_attempted_flag = Column(Integer)
        shot_made_flag = Column(Integer)
        game_date = Column(Integer)
        htm = Column(String)
        vtm = Column(String)

    class rapm_shifts(Base):
        __tablename__ = "rapm_shifts"
        __table_args__ = {"schema": "nba"}
        key_col = Column(String, primary_key=True, nullable=False)
        away_team_id = Column(Integer)
        def_player_1 = Column(String)
        def_player_1_id = Column(Integer)
        def_player_2 = Column(String)
        def_player_2_id = Column(Integer)
        def_player_3 = Column(String)
        def_player_3_id = Column(Integer)
        def_player_4 = Column(String)
        def_player_4_id = Column(Integer)
        def_player_5 = Column(String)
        def_player_5_id = Column(Integer)
        game_date = Column(Date)
        game_id = Column(Integer)
        home_team_id = Column(Integer)
        off_player_1 = Column(String)
        off_player_1_id = Column(Integer)
        off_player_2 = Column(String)
        off_player_2_id = Column(Integer)
        off_player_3 = Column(String)
        off_player_3_id = Column(Integer)
        off_player_4 = Column(String)
        off_player_4_id = Column(Integer)
        off_player_5 = Column(String)
        off_player_5_id = Column(Integer)
        points_made = Column(Integer)
        possessions = Column(Integer)
        points_per_100_poss = Column(Numeric)
        season = Column(Integer)
        away_team_abbrev = Column(String)
        home_team_abbrev = Column(String)
        event_team_abbrev = Column(String)

    Base.metadata.create_all(engine)

if __name__ == "__main__":
    main()
