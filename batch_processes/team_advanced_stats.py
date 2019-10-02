import pandas as pd
import os
from sqlalchemy import create_engine
import numpy as np

engine = create_engine(os.environ['NBA_CONNECT_DEV'])

teams_df = pd.read_sql_query('select * from nba.teambygamestats;', engine)
possessions_df = pd.read_sql_query('select * from nba.team_possessions', engine)
teams_df = teams_df.merge(possessions_df[['game_id', 'team_id', 'possessions']], on=['game_id', 'team_id'])
teams_df = teams_df.merge(teams_df,on='game_id',suffixes =['','_opponent'])
teams_df = teams_df[teams_df.team_id != teams_df.team_id_opponent]
team_advanced_stats = teams_df.groupby(['team_id', 'team_abbrev', 'season'])\
            ['fgm', 'tpm', 'fga', 'points_for', 'points_against','fta',
             'tov', 'dreb', 'oreb', 'ftm',
             'dreb_opponent', 'oreb_opponent', 'fgm_opponent', 'fga_opponent',
             'tpm_opponent', 'tpa_opponent', 'fta_opponent', 'ftm_opponent',
             'tov_opponent',
             'possessions', 'possessions_opponent'
             ]\
    .sum().reset_index()
team_advanced_stats['efg_percentage'] = (team_advanced_stats['fgm'] + (.5 * team_advanced_stats['tpm']))/team_advanced_stats['fga']
team_advanced_stats['true_shooting_percentage'] = team_advanced_stats['points_for']/(2 * (team_advanced_stats['fga'] + (team_advanced_stats['fta'] * .44)))
team_advanced_stats['tov_percentage'] = 100 *(team_advanced_stats['tov']/team_advanced_stats['possessions'])
team_advanced_stats['oreb_percentage'] = 100 * (team_advanced_stats['oreb']/(team_advanced_stats['oreb'] + team_advanced_stats['dreb_opponent']))
team_advanced_stats['ft_per_fga'] = team_advanced_stats['ftm']/team_advanced_stats['fta']
team_advanced_stats['opp_efg_percentage'] = (team_advanced_stats['fgm_opponent'] + (.5 * team_advanced_stats['tpm_opponent']))/team_advanced_stats['fga_opponent']
team_advanced_stats['opp_tov_percentage'] = 100 *(team_advanced_stats['tov_opponent']/team_advanced_stats['possessions_opponent'])
team_advanced_stats['dreb_percentage'] = 100 * (team_advanced_stats['dreb']/(team_advanced_stats['oreb_opponent'] + team_advanced_stats['dreb']))
team_advanced_stats['opp_ft_per_fga'] = team_advanced_stats['ftm_opponent']/team_advanced_stats['fta_opponent']
team_advanced_stats['off_rating'] = team_advanced_stats['points_for']/team_advanced_stats['possessions'] * 100
team_advanced_stats['def_rating'] = team_advanced_stats['points_against']/team_advanced_stats['possessions'] * 100
team_advanced_stats = team_advanced_stats[['team_id', 'team_abbrev', 'season', 'efg_percentage',
                                           'true_shooting_percentage', 'tov_percentage', 'oreb_percentage',
                                           'ft_per_fga', 'opp_efg_percentage', 'opp_tov_percentage',
                                           'dreb_percentage', 'opp_ft_per_fga', 'off_rating', 'def_rating']]
team_advanced_stats['key_col'] = team_advanced_stats['team_id'].astype(str) + team_advanced_stats['season'].astype(str)
team_advanced_stats.to_sql('team_advanced_stats', engine, schema='nba',
                     if_exists='append', index=False, method='multi')
