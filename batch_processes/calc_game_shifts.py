'''
this extracts shifts from the pbp to use for the calculation of RAPM
stats
'''
import os
import numpy as np
import pandas as pd
from sqlalchemy import create_engine

def parse_possessions(poss_list):
    '''
    a function to parse each possession and create one row for offense team
    and defense team

    Inputs:
    poss_list   - list of dataframes each one representing one possession

    Outputs:
    parsed_list  - list of dataframes where each list inside represents the players on
                   off and def and points score for each possession
    '''
    parsed_list = []

    for df in poss_list:
        if df.loc[df.index[-1], 'event_type_de'] in ['rebound', 'turnover']:
            if df.loc[df.index[-1], 'event_type_de'] == 'turnover':
                if df.loc[df.index[-1], 'event_team'] == df.loc[df.index[-1], 'home_team_abbrev']:
                    row_df = pd.concat([df.loc[df.index[-1], 'home_player_1':'away_player_5_id'],
                                        df.loc[df.index[-1], ['points_made_y', 'home_team_abbrev',
                                                              'event_team', 'away_team_abbrev',
                                                              'home_team_id', 'away_team_id', 'game_id',
                                                              'game_date', 'season']]])

                    parsed_list.append(pd.DataFrame([list(row_df)], columns=['off_player_1', 'off_player_1_id',
                                                                           'off_player_2', 'off_player_2_id',
                                                                           'off_player_3', 'off_player_3_id',
                                                                           'off_player_4', 'off_player_4_id',
                                                                           'off_player_5', 'off_player_5_id',
                                                                           'def_player_1', 'def_player_1_id',
                                                                           'def_player_2', 'def_player_2_id',
                                                                           'def_player_3', 'def_player_3_id',
                                                                           'def_player_4', 'def_player_4_id',
                                                                           'def_player_5', 'def_player_5_id',
                                                                           'points_made', 'home_team_abbrev',
                                                                           'event_team_abbrev', 'away_team_abbrev',
                                                                           'home_team_id', 'away_team_id', 'game_id',
                                                                           'game_date', 'season']))
                elif df.loc[df.index[-1], 'event_team'] == df.loc[df.index[-1], 'away_team_abbrev']:
                    row_df = pd.concat([df.loc[df.index[-1], 'home_player_1':'away_player_5_id'],
                                        df.loc[df.index[-1], ['points_made_y', 'home_team_abbrev',
                                                              'event_team', 'away_team_abbrev',
                                                              'home_team_id', 'away_team_id', 'game_id',
                                                              'game_date', 'season']]])

                    parsed_list.append(pd.DataFrame([list(row_df)], columns=['def_player_1', 'def_player_1_id',
                                                                           'def_player_2', 'def_player_2_id',
                                                                           'def_player_3', 'def_player_3_id',
                                                                           'def_player_4', 'def_player_4_id',
                                                                           'def_player_5', 'def_player_5_id',
                                                                           'off_player_1', 'off_player_1_id',
                                                                           'off_player_2', 'off_player_2_id',
                                                                           'off_player_3', 'off_player_3_id',
                                                                           'off_player_4', 'off_player_4_id',
                                                                           'off_player_5', 'off_player_5_id',
                                                                           'points_made', 'home_team_abbrev',
                                                                           'event_team_abbrev', 'away_team_abbrev',
                                                                           'home_team_id', 'away_team_id', 'game_id',
                                                                           'game_date', 'season']))
            if df.loc[df.index[-1], 'event_type_de'] == 'rebound':
                if df.loc[df.index[-1], 'event_team'] == df.loc[df.index[-1], 'away_team_abbrev']:
                    row_df = pd.concat([df.loc[df.index[-1], 'home_player_1':'away_player_5_id'],
                                        df.loc[df.index[-1], ['points_made_y', 'home_team_abbrev',
                                                              'event_team', 'away_team_abbrev',
                                                              'home_team_id', 'away_team_id', 'game_id',
                                                              'game_date', 'season']]])

                    parsed_list.append(pd.DataFrame([list(row_df)], columns=['off_player_1', 'off_player_1_id',
                                                                           'off_player_2', 'off_player_2_id',
                                                                           'off_player_3', 'off_player_3_id',
                                                                           'off_player_4', 'off_player_4_id',
                                                                           'off_player_5', 'off_player_5_id',
                                                                           'def_player_1', 'def_player_1_id',
                                                                           'def_player_2', 'def_player_2_id',
                                                                           'def_player_3', 'def_player_3_id',
                                                                           'def_player_4', 'def_player_4_id',
                                                                           'def_player_5', 'def_player_5_id',
                                                                           'points_made', 'home_team_abbrev',
                                                                           'event_team_abbrev', 'away_team_abbrev',
                                                                           'home_team_id', 'away_team_id', 'game_id',
                                                                           'game_date', 'season']))

                elif df.loc[df.index[-1], 'event_team'] == df.loc[df.index[-1], 'home_team_abbrev']:
                    row_df = pd.concat([df.loc[df.index[-1], 'home_player_1':'away_player_5_id'],
                                        df.loc[df.index[-1], ['points_made_y', 'home_team_abbrev',
                                                              'event_team', 'away_team_abbrev',
                                                              'home_team_id', 'away_team_id', 'game_id',
                                                              'game_date', 'season']]])

                    parsed_list.append(pd.DataFrame([list(row_df)], columns=['def_player_1', 'def_player_1_id',
                                                                           'def_player_2', 'def_player_2_id',
                                                                           'def_player_3', 'def_player_3_id',
                                                                           'def_player_4', 'def_player_4_id',
                                                                           'def_player_5', 'def_player_5_id',
                                                                           'off_player_1', 'off_player_1_id',
                                                                           'off_player_2', 'off_player_2_id',
                                                                           'off_player_3', 'off_player_3_id',
                                                                           'off_player_4', 'off_player_4_id',
                                                                           'off_player_5', 'off_player_5_id',
                                                                           'points_made', 'home_team_abbrev',
                                                                           'event_team_abbrev', 'away_team_abbrev',
                                                                           'home_team_id', 'away_team_id', 'game_id',
                                                                           'game_date', 'season']))

        elif df.loc[df.index[-1], 'event_type_de'] in ['shot', 'free-throw']:
            if df.loc[df.index[-1], 'event_team'] == df.loc[df.index[-1], 'home_team_abbrev']:
                row_df = pd.concat([df.loc[df.index[-1], 'home_player_1':'away_player_5_id'],
                                    df.loc[df.index[-1], ['points_made_y', 'home_team_abbrev',
                                                          'event_team', 'away_team_abbrev',
                                                          'home_team_id', 'away_team_id', 'game_id',
                                                          'game_date', 'season']]])

                parsed_list.append(pd.DataFrame([list(row_df)], columns=['off_player_1', 'off_player_1_id',
                                                                       'off_player_2', 'off_player_2_id',
                                                                       'off_player_3', 'off_player_3_id',
                                                                       'off_player_4', 'off_player_4_id',
                                                                       'off_player_5', 'off_player_5_id',
                                                                       'def_player_1', 'def_player_1_id',
                                                                       'def_player_2', 'def_player_2_id',
                                                                       'def_player_3', 'def_player_3_id',
                                                                       'def_player_4', 'def_player_4_id',
                                                                       'def_player_5', 'def_player_5_id',
                                                                       'points_made', 'home_team_abbrev',
                                                                       'event_team_abbrev', 'away_team_abbrev',
                                                                       'home_team_id', 'away_team_id', 'game_id',
                                                                       'game_date', 'season']))
            elif df.loc[df.index[-1], 'event_team'] == df.loc[df.index[-1], 'away_team_abbrev']:
                row_df = pd.concat([df.loc[df.index[-1], 'home_player_1':'away_player_5_id'],
                                    df.loc[df.index[-1], ['points_made_y', 'home_team_abbrev',
                                                          'event_team', 'away_team_abbrev',
                                                          'home_team_id', 'away_team_id', 'game_id',
                                                          'game_date', 'season']]])

                parsed_list.append(pd.DataFrame([list(row_df)], columns=['def_player_1', 'def_player_1_id',
                                                                       'def_player_2', 'def_player_2_id',
                                                                       'def_player_3', 'def_player_3_id',
                                                                       'def_player_4', 'def_player_4_id',
                                                                       'def_player_5', 'def_player_5_id',
                                                                       'off_player_1', 'off_player_1_id',
                                                                       'off_player_2', 'off_player_2_id',
                                                                       'off_player_3', 'off_player_3_id',
                                                                       'off_player_4', 'off_player_4_id',
                                                                       'off_player_5', 'off_player_5_id',
                                                                       'points_made', 'home_team_abbrev',
                                                                       'event_team_abbrev', 'away_team_abbrev',
                                                                       'home_team_id', 'away_team_id', 'game_id',
                                                                       'game_date', 'season']))

    return parsed_list

def calc_possessions(game_df):
    '''
    funciton to calculate possesion numbers for both team and players
    and insert into possesion tables. This will only be used for pbp that doesn't
    have home/away possession columns calculated will be deprecated in the future
    and calculated on the initial scrape

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
    return game_df

def extract_shifts(pbp_df, engine):
        #this is done to assign proper points to players who may be subbed of during free throw shots
    points_by_second = pbp_df.groupby('seconds_elapsed')['points_made'].sum().reset_index()

#seperate each shift into a list of dataframes where a shift is anything that happens
#between subs
    shift_dfs = []
    past_index = 0
    for i in pbp_df[pbp_df.event_type_de == 'substitution'].index:
        shift_dfs.append(pbp_df.iloc[past_index+1: i+1, :])
        past_index = i

#removing any shift where a possesion doesn't happen
    shift_dfs = [df for df in shift_dfs if df.home_possession.sum() + df.away_possession.sum() != 0]

    calc_shifts = []

    for i in range(len(shift_dfs)):
        test_df = shift_dfs[i]
        test_df = test_df.merge(points_by_second, on='seconds_elapsed', how='left')
        test_df = test_df[test_df.event_type_de != 'free-throw']
        test_df['points_made'] = np.where(test_df.event_type_de == 'foul',
                                          np.where((test_df.event_type_de.shift(1) == 'shot') &
                                                  (test_df.seconds_elapsed == test_df.seconds_elapsed.shift(1)) &
                                                   ((test_df.homedescription.str.contains('S.FOUL')) |
                                                    (test_df.visitordescription.str.contains('S.FOUL'))),
                                                   test_df.points_made_y-test_df.points_made_x.shift(1),
                                                   test_df.points_made_y),
                                          test_df.points_made_x)
        #compiling shift into summed points and possessions for each team in the shift
        groupby_columns = ['game_id', 'game_date', 'home_team_id', 'away_team_id',
                           'home_player_1', 'home_player_1_id', 'home_player_2',
                           'home_player_2_id', 'home_player_3', 'home_player_3_id',
                           'home_player_4', 'home_player_4_id', 'home_player_5',
                           'home_player_5_id', 'away_player_1', 'away_player_1_id',
                           'away_player_2', 'away_player_2_id', 'away_player_3',
                           'away_player_3_id', 'away_player_4', 'away_player_4_id',
                           'away_player_5', 'away_player_5_id']
        home_rename_dict = {'home_player_1': 'off_player_1', 'home_player_1_id': 'off_player_1_id',
                           'home_player_2': 'off_player_2', 'home_player_2_id': 'off_player_2_id',
                           'home_player_3': 'off_player_3', 'home_player_3_id': 'off_player_3_id',
                           'home_player_4': 'off_player_4', 'home_player_4_id': 'off_player_4_id',
                           'home_player_5': 'off_player_5', 'home_player_5_id': 'off_player_5_id',
                           'away_player_1': 'def_player_1', 'away_player_1_id': 'def_player_1_id',
                           'away_player_2': 'def_player_2', 'away_player_2_id': 'def_player_2_id',
                           'away_player_3': 'def_player_3', 'away_player_3_id': 'def_player_3_id',
                           'away_player_4': 'def_player_4', 'away_player_4_id': 'def_player_4_id',
                           'away_player_5': 'def_player_5', 'away_player_5_id': 'def_player_5_id'}
        away_rename_dict = {'away_player_1': 'off_player_1', 'away_player_1_id': 'off_player_1_id',
                           'away_player_2': 'off_player_2', 'away_player_2_id': 'off_player_2_id',
                           'away_player_3': 'off_player_3', 'away_player_3_id': 'off_player_3_id',
                           'away_player_4': 'off_player_4', 'away_player_4_id': 'off_player_4_id',
                           'away_player_5': 'off_player_5', 'away_player_5_id': 'off_player_5_id',
                           'home_player_1': 'def_player_1', 'home_player_1_id': 'def_player_1_id',
                           'home_player_2': 'def_player_2', 'home_player_2_id': 'def_player_2_id',
                           'home_player_3': 'def_player_3', 'home_player_3_id': 'def_player_3_id',
                           'home_player_4': 'def_player_4', 'home_player_4_id': 'def_player_4_id',
                           'home_player_5': 'def_player_5', 'home_player_5_id': 'def_player_5_id'}

        home_stats = test_df[((test_df.event_team == test_df.home_team_abbrev) &
                              (test_df.event_type_de != 'foul')) |
                             ((test_df.event_team != test_df.home_team_abbrev) &
                              (test_df.event_type_de == 'foul'))].groupby(groupby_columns)['points_made']\
                    .sum().reset_index()
        away_stats = test_df[((test_df.event_team == test_df.away_team_abbrev) &
                              (test_df.event_type_de != 'foul')) |
                             ((test_df.event_team != test_df.away_team_abbrev) &
                              (test_df.event_type_de == 'foul'))].groupby(groupby_columns)['points_made']\
                    .sum().reset_index()
        home_stats = home_stats.rename(columns=home_rename_dict)
        away_stats = away_stats.rename(columns=away_rename_dict)
        home_stats['possessions'] = shift_dfs[i][['home_possession']].sum().iloc[0]
        away_stats['possessions'] = shift_dfs[i][['away_possession']].sum().iloc[0]
        final_shift_df = pd.concat([home_stats, away_stats])
        calc_shifts.append(final_shift_df)

    game_shift_df = pd.concat(calc_shifts)
    game_shift_df['season'] = pbp_df.season.unique()[0]
    game_shift_df.to_sql('rapm_shifts', engine, schema='nba', method='multi',
                         if_exists='append', index=False)

def main():
    engine = create_engine(os.environ['NBA_CONNECT_DEV'])
    seasons = [2019, 2018, 2017]
    for season in seasons:
        print(f"Processing season: {season}")
        pbp_df = pd.read_sql_query(f'select * from nba.pbp where season = {season};', engine)\
            .sort_values(by=['game_id', 'seconds_elapsed', 'eventnum']).reset_index(drop=True)
        pbp_df = calc_possessions(pbp_df)
        points_by_second = pbp_df.groupby(['game_id', 'seconds_elapsed'])['points_made'].sum().reset_index()
        for game in range(int(f'2{str(season-1)[2:]}00001'), int(f'2{str(season-1)[2:]}01231')):
            print(f'Processing game: {game}')
            game_df = pbp_df[pbp_df.game_id == game].reset_index(drop=True)
            poss_index = game_df[(game_df.home_possession == 1) | (game_df.away_possession == 1)].index
            shift_dfs = []
            past_index = 0
            for i in poss_index:
                shift_dfs.append(game_df.iloc[past_index+1: i+1, :].reset_index())
                past_index = i
            possession = [x.merge(points_by_second, on=['game_id', 'seconds_elapsed']) for x in shift_dfs]
            print(f"Combining {game} possessions into one dataframe and inserting into database")
            poss_df = pd.concat(parse_possessions(possession))
            poss_df.to_sql('rapm_shifts', engine, schema='nba',
                           if_exists='append', index=False, method='multi')



    pass
if __name__ == '__main__':
    main()

