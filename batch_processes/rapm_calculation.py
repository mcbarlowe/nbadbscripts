import os
import numpy as np
import pandas as pd
from sklearn.linear_model import RidgeCV
from sqlalchemy import create_engine
# Convert lambda value to alpha needed for ridge CV
def lambda_to_alpha(lambda_value, samples):
    return (lambda_value * samples) / 2.0

def map_players(row_in, players):
    p1 = row_in[0]
    p2 = row_in[1]
    p3 = row_in[2]
    p4 = row_in[3]
    p5 = row_in[4]
    p6 = row_in[5]
    p7 = row_in[6]
    p8 = row_in[7]
    p9 = row_in[8]
    p10 = row_in[9]

    rowOut = np.zeros([len(players) * 2])

    rowOut[players.index(p1)] = 1
    rowOut[players.index(p2)] = 1
    rowOut[players.index(p3)] = 1
    rowOut[players.index(p4)] = 1
    rowOut[players.index(p5)] = 1

    rowOut[players.index(p6) + len(players)] = -1
    rowOut[players.index(p7) + len(players)] = -1
    rowOut[players.index(p8) + len(players)] = -1
    rowOut[players.index(p9) + len(players)] = -1
    rowOut[players.index(p10) + len(players)] = -1

    return rowOut

def one_year_rapm_calc(season, sa_engine):
    '''
    this function will calculate the season RAPM values for players of the
    season given.
    '''

    # pull shifts from table
    #shifts_df = pd.read_sql_query(f'select * from nba.rapm_shifts where season = {season};', sa_engine)

    #shifts_df = shifts_df[shifts_df.possessions != 0]

    # pull out unique player ids
    shifts_df = pd.read_csv('possessions_data.csv')
    players = list(set(list(shifts_df['off_player_1_id'].unique()) +
                       list(shifts_df['off_player_2_id'].unique()) +
                       list(shifts_df['off_player_3_id'].unique()) +
                       list(shifts_df['off_player_4_id'].unique()) +
                       list(shifts_df['off_player_5_id'].unique()) +
                       list(shifts_df['def_player_1_id'].unique()) +
                       list(shifts_df['def_player_2_id'].unique()) +
                       list(shifts_df['def_player_3_id'].unique()) +
                       list(shifts_df['def_player_4_id'].unique()) +
                       list(shifts_df['def_player_5_id'].unique())))

    players.sort()
    shifts_df['points_per_100_poss'] = shifts_df['points_made'] * 100
    train_x = shifts_df.as_matrix(columns=['off_player_1_id', 'off_player_2_id',
                                                 'off_player_3_id', 'off_player_4_id', 'off_player_5_id',
                                                 'def_player_1_id', 'def_player_2_id',
                                                 'def_player_3_id', 'def_player_4_id', 'def_player_5_id'])

    train_x = np.apply_along_axis(map_players, 1, train_x, players)
    train_y = shifts_df.as_matrix(['points_per_100_poss'])
    #possessions = shifts_df['possessions']

    lambdas_rapm = [.01, .05, .1 ]
    alphas = [lambda_to_alpha(l, train_x.shape[0]) for l in lambdas_rapm]
    clf = RidgeCV(alphas=alphas, cv=5, fit_intercept=True, normalize=False)
    model = clf.fit(train_x, train_y )
    player_arr = np.transpose(np.array(players).reshape(1, len(players)))

    # extract our coefficients into the offensive and defensive parts
    coef_offensive_array = np.transpose(model.coef_[:, 0:len(players)])
    coef_defensive_array = np.transpose(model.coef_[:, len(players):])

    # concatenate the offensive and defensive values with the playey ids into a mx3 matrix
    player_id_with_coef = np.concatenate([player_arr, coef_offensive_array, coef_defensive_array], axis=1)
    # build a dataframe from our matrix
    players_coef = pd.DataFrame(player_id_with_coef)
    intercept = model.intercept_
    name = 'rapm'
  # apply new column names
    players_coef.columns = ['player_id', '{0}__Off'.format(name), '{0}__Def'.format(name)]
    # Add the offesnive and defensive components together (we should really be weighing this to the number of offensive and defensive possession played as they are often not equal).
    players_coef[name] = players_coef['{0}__Off'.format(name)] + players_coef['{0}__Def'.format(name)]

    # rank the values
    players_coef['{0}_Rank'.format(name)] = players_coef[name].rank(ascending=False)
    players_coef['{0}__Off_Rank'.format(name)] = players_coef['{0}__Off'.format(name)].rank(ascending=False)
    players_coef['{0}__Def_Rank'.format(name)] = players_coef['{0}__Def'.format(name)].rank(ascending=False)

    # add the intercept for reference
    players_coef['{0}__intercept'.format(name)] = intercept[0]

    print(f'This is the intercept of the model: {intercept}')
    print(players_coef.head())
    #player_df = pd.read_sql_query(f'select * from nba.player_details;', sa_engine)

    #results_df = players_coef.merge(player_df[['player_id', 'display_first_last']], on='player_id')
    #results_df.to_csv('rapm_results.csv')
    players_coef.to_csv('rapm_results.csv')


def main():
    season = 2019
    engine = create_engine(os.environ['NBA_CONNECT_DEV'])
    one_year_rapm_calc(season, engine)

if __name__ == '__main__':
    main()
