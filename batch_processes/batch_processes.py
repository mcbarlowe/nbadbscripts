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
import datetime
import time
import player_advanced_stats as pas

def calc_player_advanced_stats(season, engine):
    '''
    this function will calculated the stats on the team/player advanced tabs
    for the curent season. It will drop the current table based on season passed,
    recalculate the stats, and then reinsert them into the table.

    Inputs:
    season  - current season needed to be calculated
    engine  - SQLAlchemy engine

    Outputs:
    None
    '''

    #delete old values
    #engine.connect().execute(f'DELETE from nba.player_advanced_stats where season = {season};')
    #engine.connect().execute(f'DELETE from nba.team_advanced_stats where season = {season};')
    player_possessions = pd.read_sql_query(pas.player_possession_query.format(season=season), engine)
    plus_minus_df = pd.read_sql_query(pas.plus_minus_sql.format(season=season), engine)
    pm_df = plus_minus_df[~plus_minus_df.isna()]
    ratings_df = pm_df.merge(player_possessions, on=['player_id', 'season'])
    ratings_df['off_rating'] = (ratings_df['plus'] * 100)/ratings_df['possessions']
    ratings_df['def_rating'] = (ratings_df['minus'] * 100)/ratings_df['possessions']
    team_df = pd.read_sql_query(pas.team_query.format(season=season), engine)
    print(pm_df.head)
    print(player_possessions.head)
    print(team_df.head)

#calculating effective fg% and true fg%
    players_df = pd.read_sql_query(f'select * from nba.playerbygamestats where toc > 0 and season = {season};', engine)
    players_df = players_df.merge(team_df, on=['game_id', 'team_id'])
    player_teams = players_df.groupby(['player_id', 'player_name', 'season'])\
        .apply(lambda x: pd.Series({'team_abbrev':'/'.join(x['team_abbrev'].unique())})).reset_index()
    player_efg = players_df.groupby(['player_id', 'player_name', 'season'])['fgm', 'tpm', 'fga', 'points', 'fta']\
        .sum().reset_index()
    player_efg['efg_percentage'] = (player_efg['fgm'] + (.5 * player_efg['tpm']))/player_efg['fga']
    player_efg['true_shooting_percentage'] = player_efg['points']/(2 * (player_efg['fga'] + (player_efg['fta'] * .44)))
    player_stats = player_teams.merge(player_efg[['player_id', 'season',
                                                  'efg_percentage', 'true_shooting_percentage']],
                                      on=['player_id', 'season'])
#calculating percentage stats
    percentage_stats = players_df.groupby(['player_id', 'player_name', 'season'])\
                        ['toc', 'oreb', 'dreb', 'tov', 'stl', 'blk', 'ast', 'fgm', 'fga',
                         'tpm', 'tpa', 'ftm' , 'fta',
                         'team_toc', 'team_oreb', 'team_dreb', 'team_tov',
                         'team_fgm', 'team_fga', 'team_ftm', 'team_fta',
                         'team_tpm' ,'team_tpa', 'opp_fga', 'opp_fgm',
                         'opp_fta', 'opp_ftm', 'opp_tpa', 'opp_tpm',
                         'team_stl', 'team_blk', 'opp_toc', 'opp_dreb',
                         'opp_oreb', 'opp_ast', 'opp_tov', 'opp_stl',
                         'opp_blk', 'team_possessions', 'opp_possessions']\
                        .sum().reset_index()

    percentage_stats['oreb_percentage'] = ((percentage_stats['oreb'] * (percentage_stats['team_toc']/60))/
                                           ((percentage_stats['toc']/60) *
                                            (percentage_stats['team_oreb'] + percentage_stats['opp_dreb']))) * 100
    percentage_stats['dreb_percentage'] = ((percentage_stats['dreb'] * (percentage_stats['team_toc']/60))/
                                           ((percentage_stats['toc']/60) *
                                            (percentage_stats['team_dreb'] + percentage_stats['opp_oreb']))) * 100
    percentage_stats['ast_percentage'] = 100 * (percentage_stats['ast']/
                                                ((((percentage_stats['toc'])/(percentage_stats['team_toc']))
                                                * percentage_stats['team_fgm']) - percentage_stats['fgm'])
                                               )
    percentage_stats['stl_percentage'] = 100 * ((percentage_stats['stl'] * (percentage_stats['team_toc']/60))/
                                                (percentage_stats['toc']/60 * percentage_stats['opp_possessions']))
    percentage_stats['blk_percentage'] = 100 * ((percentage_stats['blk'] * (percentage_stats['team_toc']/60))/
                                                (percentage_stats['toc']/60 * (percentage_stats['opp_fga']-
                                                                               percentage_stats['opp_tpa'])))
    percentage_stats['tov_percentage'] = 100 * (percentage_stats['tov']/
                                                (percentage_stats['fga'] + .44 * percentage_stats['fta'] +
                                                 percentage_stats['tov']))
    percentage_stats['usg_percentage'] = 100 * (((percentage_stats['fga'] + .44 * percentage_stats['fta'] +
                                                percentage_stats['tov']) * percentage_stats['team_toc']/60)/(
                                                percentage_stats['toc']/60 * (percentage_stats['team_fga'] +
                                                                              .44 * percentage_stats['team_fta'] +
                                                                              percentage_stats['team_tov'])))
#merging the three dataframes together
    player_adv_stats = player_stats.merge(percentage_stats[['player_id', 'oreb_percentage', 'dreb_percentage',
                                                            'ast_percentage', 'stl_percentage', 'blk_percentage',
                                                            'tov_percentage', 'usg_percentage', 'season']],
                                          on=['player_id', 'season'])
    player_adv_stats = player_adv_stats.merge(ratings_df[['player_id', 'season', 'off_rating', 'def_rating']],
                                              on=['season', 'player_id'])
    player_adv_stats['key_col'] = player_adv_stats['player_id'].astype(str) + player_adv_stats['season'].astype(str)



    player_adv_stats = player_adv_stats[['player_id', 'season', 'team_abbrev', 'efg_percentage',
                                         'true_shooting_percentage', 'oreb_percentage', 'dreb_percentage',
                                         'ast_percentage', 'stl_percentage', 'blk_percentage', 'tov_percentage',
                                         'usg_percentage', 'off_rating', 'def_rating', 'key_col']]
    player_adv_stats.to_sql('player_advanced_stats', engine, schema='nba',
                   if_exists='append', index=False, method='multi')


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
    teams_df['key_col'] = teams_df['game_id'].astype(str) + teams_df['team_abbrev']

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
    None
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
    away_possession_df = away_possession_df.groupby(['player_id', 'player_name', 'game_id', 'away_team_id'])['away_possession'].sum().reset_index().sort_values('away_possession')

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

    possession_df['key_col'] = possession_df['player_id'].astype(str) + possession_df['game_id'].astype(str)
    team_possession_df['key_col'] = team_possession_df['team_id'].astype(str) + team_possession_df['game_id'].astype(str)

    possession_df['game_id'] = possession_df['game_id'].astype(int)
    team_possession_df['game_id'] = team_possession_df['game_id'].astype(int)

    possession_df.to_sql('player_possessions', engine, schema='nba',
                         if_exists='append', index=False, method='multi')

    team_possession_df.to_sql('team_possessions', engine, schema='nba',
                         if_exists='append', index=False, method='multi')

def get_game_ids(date):
    '''
    This function gets the game ids of games returned from the api
    endpoint

    Inputs:
    date - date which to get the game ids of games played

    Ouputs:
    game_ids - list of game ids
    '''

    games_api = ('https://stats.nba.com/stats/scoreboard?'
                 f'DayOffset=0&GameDate={date.strftime("%Y-%m-%d")}&LeagueID=00')
    user_agent = {'User-agent': 'Mozilla/5.0'}
    scoreboard = requests.get(games_api, headers=user_agent).json()

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
    # get the game ids of the games played yesterday
    #yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = datetime.datetime.now() - datetime.timedelta(days=7)
    games = get_game_ids(yesterday)
    #date = '2019-04-04'
    #games_api = ('https://stats.nba.com/stats/scoreboard?'
    #             f'DayOffset=0&GameDate={date}&LeagueID=00')

    if games == []:
        logging.info("No games on %s", yesterday)
        return
    # creates a list of play by play dataframes to process
    games_df_list = []
    for game in games:
        games_df_list.append(ns.scrape_game([game]))
        time.sleep(1)

    for game_df in games_df_list:
        game_df.columns = list(map(str.lower, game_df.columns))
        game_df['game_id'] = game_df['game_id'].str.slice(start=2)
        game_df['key_col'] = (game_df['game_id'].astype(str) +
                              game_df['eventnum'].astype(str) +
                              game_df['game_date'].astype(str) +
                              game_df['home_team_abbrev'] + game_df['away_team_abbrev'] +
                              game_df['seconds_elapsed'].astype(str) +
                              game_df.index.astype(str))
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
        try:
            game_df.to_sql('pbp', engine, schema='nba',
                           if_exists='append', index=False, method='multi')
            logging.info("Inserting play by play for %s into nba.playerbygamestats",
                         game_df.game_id.unique()[0])
        except Exception as e:
            print(e)
            logging.error("Play by play for %s failed to insert into nba.pbp",
                         game_df.game_id.unique()[0])


        # TODO rewrite this sql query i use for this into a python funciton
        # TODO to make debuggin easier in the future 9-2-2019
        logging.info("Inserting player boxscore for %s into nba.playerbygamestats",
                game_df.game_id.unique()[0])
        engine.connect().execute(sqlqueries.pbgs_calc.format(game_id=game_df.game_id.unique()[0]))

        logging.info("Inserting team boxscore for %s into nba.teambygamestats",
                     game_df.game_id.unique()[0])
        calc_team_stats(game_df, engine)

        logging.info("Inserting possession data for %s into nba.teambygamestats",
                     game_df.game_id.unique()[0])
        calc_possessions(game_df, engine)

    logging.info("Inserting player advanced stats data for %s into nba.teambygamestats",
                 game_df.game_id.unique()[0])
    calc_player_advanced_stats(game_df['season'].unique()[0], engine)
    # TODO Calculate RAPM plus any other advanced
    # TODO stats I happen to find for teams and players

if __name__ == '__main__':
    main()
