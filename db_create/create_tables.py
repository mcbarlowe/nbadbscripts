'''
This script will setup tables on PostgreSQL database
'''
import argparse
import logging
import pandas as pd
import numpy as np
import sqlqueries
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, Boolean, Date

# pylint: disable=too-many-statements
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

# pylint: disable=too-few-public-methods
# pylint: disable=unused-variable
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
    # TODO: This needs to be removed once scripts are ready for production

    Base.metadata.create_all(engine)
    for year in ['16', '17', '18']:
        for x in range(int(f'2{year}00001'), int(f'2{year}01231')):
            print(x)
            test = pd.read_csv(f'/Users/MattBarlowe/nba_data/{x}.csv')
            test['key_col'] = (test['game_id'].astype(str) +
                               test['eventnum'].astype(str) +
                               test['game_date'].astype(str) +
                               test['home_team_abbrev'] + test['away_team_abbrev'])

            test = test.astype({'is_d_rebound': bool, 'is_o_rebound': bool,
                                'is_turnover': bool, 'is_steal': bool,
                                'is_putback': bool, 'is_block': bool,
                                'is_three': bool, 'shot_made': bool,
                                'home_player_1_id': int, 'home_player_2_id': int,
                                'home_player_3_id': int, 'home_player_4_id': int,
                                'home_player_5_id': int, 'away_player_2_id': int,
                                'away_player_1_id': int, 'away_player_3_id': int,
                                'away_player_4_id': int, 'away_player_5_id': int})
            # will need this cleaning in all the subsequent writes to the database
            test['points_made'] = np.where((test['shot_made'] == 0) &
                                          (test['event_type_de'] == 'free-throw'), 0, test['points_made'])
            test.columns = list(map(str.lower, test.columns))
            test.to_sql('pbp', engine, schema='nba', if_exists='append', index=False, method='multi')

            engine.connect().execute(sqlqueries.pbgs_calc.format(game_id=test.game_id.unique()[0]))

if __name__ == '__main__':
    main()
