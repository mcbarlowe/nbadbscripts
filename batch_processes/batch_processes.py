'''
This script runs the batch processes for the NBA database
'''
#import datetime
import math
import os
import logging
import nba_scraper.nba_scraper as ns
import requests
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import sqlqueries


def calc_team_stats(game_df, engine):
    '''
    calculates the team stats and inserts them into the database

    Inputs:
    game_df   - play by play dataframe
    engine    - sql alchemy database connection engine

    Ouputs:
    None
    '''
    #calculating columns that will be used in the team stats
    game_df['is_assist'] = np.where((game_df['event_type_de'] == 'shot') &
                         (game_df['player2_id'] != 0), True, False)

    game_df['fg_attempted'] = np.where(game_df['event_type_de'].isin(['missed_shot', 'shot']), True, False)
    game_df['ft_attempted'] = np.where(game_df['event_type_de'] == 'free-throw', True, False)
    game_df['fg_made'] = np.where((game_df['event_type_de'].isin(['missed_shot', 'shot'])) &
                                  (game_df['points_made'] > 0), True, False)
    game_df['tp_made'] = np.where(game_df['points_made'] == 3, True, False)
    game_df['ft_made'] = np.where((game_df['event_type_de'] == 'free-throw') &
                                  (game_df['points_made'] == 1), True, False)
    game_df['is_foul'] = np.where((game_df['event_type_de'] == 'foul') &
                                  (~game_df['player1_id'].isnull()) &
                                  (~game_df['de'].str.contains('Technical'))
                                  , True, False)
    #calculating the team stats
    home_team_id = game_df['home_team_id'].unique()[0]
    away_team_id = game_df['away_team_id'].unique()[0]
    home_team_abbrev = game_df['home_team_abbrev'].unique()[0]
    away_team_abbrev = game_df['away_team_abbrev'].unique()[0]
    teams_df = game_df.groupby(['player1_team_id', 'game_id'])['points_made', 'is_block', 'is_steal',
                                                    'is_three', 'is_d_rebound', 'is_o_rebound',
                                                    'is_turnover', 'fg_attempted', 'ft_attempted',
                                                    'fg_made', 'tp_made', 'ft_made', 'is_assist',
                                                    'is_foul'].sum().reset_index()
    teams_df['player1_team_id'] = teams_df['player1_team_id'].astype(int)
    teams_df.rename(columns={'player1_team_id': 'team_id', 'points_made': 'points_for',
                             'is_three': 'tpa', 'is_d_rebound': 'dreb', 'is_o_rebound': 'oreb',
                             'is_turnover': 'tov', 'is_block': 'shots_blocked', 'is_steal': 'stl',
                             'is_assist': 'ast', 'fg_made': 'fgm', 'fg_attempted': 'fga',
                             'ft_made': 'ftm', 'ft_attempted': 'fta', 'tp_made': 'tpm',
                             'is_foul': 'pf'}, inplace=True)
    teams_df['team_abbrev'] = np.where(teams_df['team_id'] == home_team_id, home_team_abbrev, away_team_abbrev)
    teams_df['game_date'] = game_df['game_date'].unique()[0]
    teams_df['season'] = game_df['season'].unique()[0]
    teams_df['toc'] = game_df['seconds_elapsed'].max()
    teams_df['toc_string'] = f"{math.floor(game_df['seconds_elapsed'].max()/60)}:{game_df['seconds_elapsed'].max()%60}0"
    teams_df['is_home'] = np.where(teams_df['team_id'] == home_team_id, 1, 0)
    teams_df['points_against'] = np.where(teams_df['team_id'] == home_team_id,
                                          teams_df[teams_df['team_id'] == away_team_id]['points_for'], 0)
    teams_df['points_against'] = np.where(teams_df['team_id'] == away_team_id,
                                          teams_df[teams_df['team_id'] == home_team_id]['points_for'],
                                          teams_df['points_against'])
    teams_df['pf_drawn'] = np.where(teams_df['team_id'] == home_team_id,
                                          teams_df[teams_df['team_id'] == away_team_id]['pf'], 0)
    teams_df['pf_drawn'] = np.where(teams_df['team_id'] == away_team_id,
                                          teams_df[teams_df['team_id'] == home_team_id]['pf'],
                                          teams_df['pf_drawn'])
    teams_df['blk'] = np.where(teams_df['team_id'] == home_team_id,
                                          teams_df[teams_df['team_id'] == away_team_id]['shots_blocked'], 0)
    teams_df['blk'] = np.where(teams_df['team_id'] == away_team_id,
                                          teams_df[teams_df['team_id'] == home_team_id]['shots_blocked'],
                                          teams_df['blk'])
    teams_df['is_win'] = np.where(teams_df['points_for'] > teams_df['points_against'], 1, 0)
    teams_df['is_assist'] = np.where
    teams_df['plus_minus'] = teams_df['points_for'] - teams_df['points_against']
    teams_df = teams_df[['season', 'game_date', 'game_id', 'team_abbrev', 'team_id',
                         'toc', 'toc_string', 'points_for', 'points_against', 'is_win',
                         'fgm', 'fga', 'tpm', 'tpa', 'ftm',
                         'fta', 'oreb', 'dreb', 'tov', 'stl', 'ast',
                         'blk', 'shots_blocked', 'pf',  'pf_drawn', 'plus_minus', 'is_home']]

    teams_df.to_sql('teambygamestats', engine, schema='nba', method='multi',
                   if_exists='append', index=False)

def calc_possessions(game_df, engine):
    '''
    funciton to calculate possesion numbers for both team and players
    and insert into possesion tables

    Inputs:
    game_df  - dataframe of nba play by play
    engine   - sql alchemy engine

    Outputs:
    game_df  - pbp dataframe with the possessions calculated
    '''
    #calculating made shot possessions
    game_df['home_possession'] = np.where((game_df.event_team == game_df.home_team_abbrev) &
                                         (game_df.event_type_de == 'shot'), 1, 0)
#calculating turnover possessions
    game_df['home_possession'] = np.where((game_df.event_team == game_df.home_team_abbrev) &
                                         (game_df.event_type_de == 'turnover'), 1, game_df['home_possession'])
#calculating defensive rebound possessions
    game_df['home_possession'] = np.where(((game_df.event_team == game_df.away_team_abbrev) &
                                         (game_df.is_d_rebound == 1)) |
                                          ((game_df.event_type_de == 'rebound') &
                                           (game_df.is_d_rebound == 0) &
                                           (game_df.is_o_rebound == 0) &
                                           (game_df.event_team == game_df.away_team_abbrev) &
                                           (game_df.event_type_de.shift(1) != 'free-throw')),
                                          1, game_df['home_possession'])
#calculating final free throw possessions
    game_df['home_possession'] = np.where((game_df.event_team == game_df.home_team_abbrev) &
                                         ((game_df.homedescription.str.contains('Free Throw 2 of 2')) |
                                           (game_df.homedescription.str.contains('Free Throw 3 of 3'))),
                                         1, game_df['home_possession'])
#calculating made shot possessions
    game_df['away_possession'] = np.where((game_df.event_team == game_df.away_team_abbrev) &
                                         (game_df.event_type_de == 'shot'), 1, 0)
#calculating turnover possessions
    game_df['away_possession'] = np.where((game_df.event_team == game_df.away_team_abbrev) &
                                         (game_df.event_type_de == 'turnover'), 1, game_df['away_possession'])
#calculating defensive rebound possessions
    game_df['away_possession'] = np.where(((game_df.event_team == game_df.home_team_abbrev) &
                                         (game_df.is_d_rebound == 1)) |
                                          ((game_df.event_type_de == 'rebound') &
                                           (game_df.is_d_rebound == 0) &
                                           (game_df.is_o_rebound == 0) &
                                           (game_df.event_team == game_df.home_team_abbrev) &
                                           (game_df.event_type_de.shift(1) != 'free-throw')),
                                          1, game_df['away_possession'])
#calculating final free throw possessions
    game_df['away_possession'] = np.where((game_df.event_team == game_df.away_team_abbrev) &
                                         ((game_df.visitordescription.str.contains('Free Throw 2 of 2')) |
                                           (game_df.visitordescription.str.contains('Free Throw 3 of 3'))),
                                         1, game_df['away_possession'])
    #calculating player possesions
    player1 = game_df[['home_player_1', 'home_player_1_id', 'home_possession', 'game_id', 'home_team_id']]\
              .rename(columns={'home_player_1': 'player_name', 'home_player_1_id': 'player_id'})
    player2 = game_df[['home_player_2', 'home_player_2_id', 'home_possession', 'game_id', 'home_team_id']]\
              .rename(columns={'home_player_2': 'player_name', 'home_player_2_id': 'player_id'})
    player3 = game_df[['home_player_3', 'home_player_3_id', 'home_possession', 'game_id', 'home_team_id']]\
              .rename(columns={'home_player_3': 'player_name', 'home_player_3_id': 'player_id'})
    player4 = game_df[['home_player_4', 'home_player_4_id', 'home_possession', 'game_id', 'home_team_id']]\
              .rename(columns={'home_player_4': 'player_name', 'home_player_4_id': 'player_id'})
    player5 = game_df[['home_player_5', 'home_player_5_id', 'home_possession', 'game_id', 'home_team_id']]\
              .rename(columns={'home_player_5': 'player_name', 'home_player_5_id': 'player_id'})
    home_possession_df = pd.concat([player1, player2, player3, player4, player5])
    home_possession_df = home_possession_df.groupby(['player_id', 'player_name', 'game_id', 'home_team_id'])['home_possession'].sum().reset_index().sort_values('home_possession')
    player1 = game_df[['away_player_1', 'away_player_1_id', 'away_possession', 'game_id', 'away_team_id']]\
              .rename(columns={'away_player_1': 'player_name', 'away_player_1_id': 'player_id'})
    player2 = game_df[['away_player_2', 'away_player_2_id', 'away_possession', 'game_id', 'away_team_id']]\
              .rename(columns={'away_player_2': 'player_name', 'away_player_2_id': 'player_id'})
    player3 = game_df[['away_player_3', 'away_player_3_id', 'away_possession', 'game_id', 'away_team_id']]\
              .rename(columns={'away_player_3': 'player_name', 'away_player_3_id': 'player_id'})
    player4 = game_df[['away_player_4', 'away_player_4_id', 'away_possession', 'game_id', 'away_team_id']]\
              .rename(columns={'away_player_4': 'player_name', 'away_player_4_id': 'player_id'})
    player5 = game_df[['away_player_5', 'away_player_5_id', 'away_possession', 'game_id', 'away_team_id']]\
              .rename(columns={'away_player_5': 'player_name', 'away_player_5_id': 'player_id'})
    away_possession_df = pd.concat([player1, player2, player3, player4, player5])
    away_possession_df.groupby(['player_id', 'player_name', 'game_id', 'away_team_id'])['away_possession'].sum().reset_index().sort_values('away_possession')

    home_possession_df = home_possession_df.rename(columns={'home_team_id': 'team_id',
                                                          'home_possession': 'possessions'})
    away_possession_df = away_possession_df.rename(columns={'away_team_id': 'team_id',
                                                          'away_possession': 'possessions'})
    possession_df = pd.concat([home_possession_df, away_possession_df])

    row1 = [game_df.home_team_id.unique()[0], game_df.game_id.unique()[0],
            game_df.home_team_abbrev.unique()[0], game_df['home_possession'].sum()]
    row2 = [game_df.away_team_id.unique()[0], game_df.game_id.unique()[0],
            game_df.away_team_abbrev.unique()[0], game_df['away_possession'].sum()]
    team_possession_df = pd.DataFrame([row1,row2], columns=['team_id', 'game_id', 'team_abbrev', 'possessions'])

    possession_df['key_col'] = possession_df['player_id'] + possession_df['game_id']
    team_possession_df['key_col'] = team_possession_df['team_id'] + team_possession_df['game_id']

    possession_df.to_sql('player_possessions', engine, schema='nba',
                         if_exists='append', index=False, method='multi')

    team_possession_df.to_sql('team_possessions', engine, schema='nba',
                         if_exists='append', index=False, method='multi')

    return game_df

def get_game_ids(api):
    '''
    This function gets the game ids of games returned from the api
    endpoint

    Inputs:
    api  - nba Scoreboard api endpoint

    Ouputs:
    game_ids - list of game ids
    '''

    user_agent = {'User-agent': 'Mozilla/5.0'}
    scoreboard = requests.get(api, headers=user_agent).json()

    games = scoreboard['resultSets'][0]['rowSet']
    game_ids = [game[2][2:] for game in games]
    return game_ids

def get_player_details(players, engine):
    '''
    this function will check and see if the players in the play by play dataframe
    are in the player_details table and if not will hit the nba player api get
    them and insert them into the database
    '''
    #TODO this needs to be finished at some point
    #for player in players:
    #url = f'https://stats.nba.com/stats/commonplayerinfo?LeagueID=&PlayerID={p}'

    pass
def main():
    '''
    Main function to run to scrape games daily
    '''

    engine = create_engine(os.environ['NBA_CONNECT'])

    # Logging stuff
    logging.basicConfig(level=logging.INFO, filename='batchprocess.logs',
                        format='%(asctime)s - %(levelname)s: %(message)s')

    # TODO uncomment this when testing is done
    #yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    #games_api = ('https://stats.nba.com/stats/scoreboard?'
    #             f'DayOffset=0&GameDate={DATE.strftime("%Y-%m-%d")}&LeagueID=00')
    date = '2019-04-04'
    games_api = ('https://stats.nba.com/stats/scoreboard?'
                 f'DayOffset=0&GameDate={date}&LeagueID=00')
    games = get_game_ids(games_api)
    if games == []:
        logging.info("No games on %s", date)
        return
    # creates a list of play by play dataframes to process
    games_df_list = [ns.scrape_game([game]) for game in games]
    for game_df in games_df_list:
        logging.info("Inserting %s into nba.pbp",
                     game_df.game_id.unique()[0])
        game_df.columns = list(map(str.lower, game_df.columns))
        game_df['key_col'] = (game_df['game_id'].astype(str) +
                              game_df['eventnum'].astype(str) +
                              game_df['game_date'].astype(str) +
                              game_df['home_team_abbrev'] + game_df['away_team_abbrev'])
        game_df = game_df.astype({'is_d_rebound': bool, 'is_o_rebound': bool,
                                  'is_turnover': bool, 'is_steal': bool,
                                  'is_putback': bool, 'is_block': bool,
                                  'is_three': bool, 'shot_made': bool,
                                  'home_player_1_id': int, 'home_player_2_id': int,
                                  'home_player_3_id': int, 'home_player_4_id': int,
                                  'home_player_5_id': int, 'away_player_2_id': int,
                                  'away_player_1_id': int, 'away_player_3_id': int,
                                  'away_player_4_id': int, 'away_player_5_id': int})
        game_df.drop(['video_available_flag'], axis=1, inplace=True)

        # calculating player stats here
        # TODO rewrite this sql query i use for this into a python funciton
        # TODO to make debuggin easier in the future 9-2-2019
        logging.info("Inserting boxscore for %s into nba.playerbygamestats",
                     game_df.game_id.unique()[0])

        #calc_team_stats(game_df, engine)
        logging.info("Inserting  boxscore for %s into nba.teambygamestats",
                     game_df.game_id.unique()[0])

        game_df = calc_possessions(game_df, engine)
        game_df.to_sql('pbp', engine, schema='nba',
                       if_exists='append', index=False, method='multi')
        # TODO Calculate RAPM plus any other advanced
        # TODO stats I happen to find for teams and players
        engine.connect().execute(sqlqueries.pbgs_calc.format(game_id=game_df.game_id.unique()[0]))

if __name__ == '__main__':
    main()
