'''
This script will setup tables on PostgreSQL database
'''
import argparse
import logging
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Boolean, Date

def main():
    '''
    Function to create all tables on the database
    '''
# Logging stuff
    logging.basicConfig(level=logging.INFO, filename='dbcreate.logs',
                        format='%(asctime)s - %(levelname)s: %(message)s')
# Adding an arg parser here so I can pass sensitive connection strings with
# environment variables
    parser = argparse.ArgumentParser(description='Program to create needed tables on nba database')
    parser.add_argument('--con', help='Connection string for SQL Alchemy create_engine')
    args = parser.parse_args()

    # TODO: remove echo=True from this when ready for production
    engine = create_engine(args.con, echo=True)

    Base = declarative_base()

    class Pbp(Base):
        '''
        Class to create the play by play table
        '''
        __tablename__ = 'pbp'
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

        __table_args__ = {'schema': 'nba'}


    class playerbygamestats(Base):
        '''
        Class to create the playerbygamestats table which is each players
        box score for every game they played in.
        '''
        __tablename__ = 'playerbygamestats'
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
        __table_args__ = {'schema': 'nba'}

    class teambygamestats(Base):
        '''
        Class to create the teambygamestats table which is each teams
        box score for every game they played in.
        '''
        __tablename__ = 'teambygamestats'
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
        __table_args__ = {'schema': 'nba'}

    class team_details(Base):
        '''
        Class to create table for team details
        '''
        __tablename__ = 'team_details'
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
        __table_args__ = {'schema': 'nba'}

    class player_details(Base):
        '''
        Class to build table with player info
        '''
        __tablename__ = 'player_details'
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
        jersey_number = Column(Integer)
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
        draft_year = Column(Integer)
        draft_round = Column(Integer)
        draft_number = Column(Integer)
        __table_args__ = {'schema': 'nba'}

    Base.metadata.create_all(engine)

if __name__ == '__main__':
    main()
