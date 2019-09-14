import os
from sqlalchemy import create_engine
import sqlqueries
import pandas as pd
import numpy as np
import math

def calc_points_made(row):
    '''
    function to calculate the points earned by a team with each shot made

    Inputs:
    row - row of pbp dataframe

    Outputs - value of shot made
    '''

    if row['is_three'] == 1 and row['shot_made'] == 1:
        return 3
    elif row['is_three'] == 0 and row['shot_made'] == 1 and row['shot_type'] != 'free':
        return 2
    elif row['shot_type'] == 'free' and row['shot_made'] == 1:
        return 1
    else:
        return 0

def calc_team_stats(game_df, sqlalchemy_engine):
    '''
    calculates the team stats and inserts them into the database

    Inputs:
    game_df   - play by play dataframe
    sqlalchemy_engine   - database connection

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
    teams_df['key_col'] = teams_df['team_id'] + teams_df['game_id'] + teams_df['game_date']
    teams_df.to_sql('teambygamestats', sqlalchemy_engine, schema='nba', method='multi',
                   if_exists='append', index=False)

def main():
    engine = create_engine(os.environ['NBA_CONNECT'])
    seasons = [2016, 2017, 2018]
    for season in seasons:
        for game in range(int(f'2{str(season)[2:]}00001'), int(f'2{str(season)[2:]}01231')):
            # if season == 2018 and game <= 21801112:
            #     continue
            print(game)
            game_df = pd.read_csv(f'/Users/MattBarlowe/nba_data/{game}.csv')
            game_df['points_made'] = game_df.apply(calc_points_made, axis=1)
            game_df.columns = list(map(str.lower, game_df.columns))
            game_df['key_col'] = (game_df['game_id'].astype(str) +
                                  game_df['eventnum'].astype(str) +
                                  game_df['game_date'].astype(str) +
                                  game_df['home_team_abbrev'] + game_df['away_team_abbrev'])

            #this fixes all the stuff in the old data where the scraper was wrong
            game_df['event_team'] = np.where(game_df['homedescription'].isnull(),
                                 game_df['away_team_abbrev'],
                                 np.where(game_df['visitordescription'].isnull(),
                                          game_df['home_team_abbrev'],
                                          np.where((game_df['homedescription'].str.contains('Turnover')) |
                                                   (game_df['homedescription'].str.contains('MISS')),
                                                   game_df['home_team_abbrev'],
                                                   game_df['away_team_abbrev'])))


            game_df['is_o_rebound'] = np.where((game_df['event_type_de'] == 'rebound') &
                                   (game_df['event_team'] == game_df['event_team'].shift(1)) &
                                   (~game_df['player1_id'].isin([game_df.home_team_id.unique()[0],
                                                                 game_df.away_team_id.unique()[0]])),
                                   1, 0)
            game_df['is_d_rebound'] = np.where((game_df['event_type_de'] == 'rebound') &
                                   (game_df['event_team'] != game_df['event_team'].shift(1)) &
                                   (~game_df['player1_id'].isin([game_df.home_team_id.unique()[0],
                                                                 game_df.away_team_id.unique()[0]])),
                                   1, 0)

            game_df = game_df.astype({'is_d_rebound': bool, 'is_o_rebound': bool,
                                      'is_turnover': bool, 'is_steal': bool,
                                      'is_putback': bool, 'is_block': bool,
                                      'is_three': bool, 'shot_made': bool,
                                      'home_player_1_id': int, 'home_player_2_id': int,
                                      'home_player_3_id': int, 'home_player_4_id': int,
                                      'home_player_5_id': int, 'away_player_2_id': int,
                                      'away_player_1_id': int, 'away_player_3_id': int,
                                      'away_player_4_id': int, 'away_player_5_id': int})
            # insert play by play data
            game_df.to_sql('pbp', engine, schema='nba',
                           if_exists='append', index=False, method='multi')
            # calculating team stats and inserting in the database
            calc_team_stats(game_df, engine)
            # calculating player stats
            engine.connect().execute(sqlqueries.pbgs_calc.format(game_id=game_df.game_id.unique()[0]))
            game_df.to_csv(f'/Users/MattBarlowe/new_data/{game}.csv')
if __name__ == '__main__':
    main()
