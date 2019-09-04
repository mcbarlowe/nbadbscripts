import os
from sqlalchemy import create_engine
import sqlqueries
import pandas as pd
import numpy as np

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

            game_df['is_o_rebound'] = np.where((game_df['event_type_de'] == 'rebound') &
                                               (game_df['event_team'] == game_df['event_team'].shift(1))
                                                , 1, 0)

            game_df = game_df.astype({'is_d_rebound': bool, 'is_o_rebound': bool,
                                      'is_turnover': bool, 'is_steal': bool,
                                      'is_putback': bool, 'is_block': bool,
                                      'is_three': bool, 'shot_made': bool,
                                      'home_player_1_id': int, 'home_player_2_id': int,
                                      'home_player_3_id': int, 'home_player_4_id': int,
                                      'home_player_5_id': int, 'away_player_2_id': int,
                                      'away_player_1_id': int, 'away_player_3_id': int,
                                      'away_player_4_id': int, 'away_player_5_id': int})

            game_df.to_sql('pbp', engine, schema='nba', method='multi',
                           if_exists='append', index=False)

            engine.connect().execute(sqlqueries.pbgs_calc.format(game_id=game_df.game_id.unique()[0]))
            game_df.to_csv(f'/Users/MattBarlowe/new_data/{game}.csv')
if __name__ == '__main__':
    main()
