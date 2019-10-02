import pandas as pd
import os
from sqlalchemy import create_engine
import numpy as np

#TODO refactor this into something that takes a season parameter because i
#TODO need to calculate past seasons since they are inserted
engine = create_engine(os.environ['NBA_CONNECT_DEV'])
team_query = """
             select
                 tbg.game_id
                 ,tbg.team_id
                 ,tbg.season team_season
                 ,tbg.fgm team_fgm
                 ,tbg.fga team_fga
                 ,tbg.fta team_fta
                 ,tbg.ftm team_ftm
                 ,tbg.tpm team_tpm
                 ,tbg.tpa team_tpa
                 ,tbg.toc team_toc
                 ,tbg.dreb team_dreb
                 ,tbg.oreb team_oreb
                 ,tbg.ast team_ast
                 ,tbg.tov team_tov
                 ,tbg.stl team_stl
                 ,tbg.blk team_blk
                 ,tbg1.team_abbrev opp_team_abbrev
                 ,tbg1.team_id opp_team_id
                 ,tbg1.fgm opp_fgm
                 ,tbg1.fga opp_fga
                 ,tbg1.fta opp_fta
                 ,tbg1.ftm opp_ftm
                 ,tbg1.tpm opp_tpm
                 ,tbg1.tpa opp_tpa
                 ,tbg1.toc opp_toc
                 ,tbg1.dreb opp_dreb
                 ,tbg1.oreb opp_oreb
                 ,tbg1.ast opp_ast
                 ,tbg1.tov opp_tov
                 ,tbg1.stl opp_stl
                 ,tbg1.blk opp_blk
                 ,tp.possessions team_possessions
                 ,tp1.possessions opp_possessions
            from nba.teambygamestats tbg
            join nba.teambygamestats tbg1
                on tbg.game_id = tbg1.game_id
                and tbg.team_id != tbg1.team_id
            join nba.team_possessions tp
                on tp.game_id = tbg.game_id
                and tp.team_id = tbg.team_id
            join nba.team_possessions tp1
                on tp1.game_id = tbg.game_id
                and tp1.team_id != tbg.team_id
            """

plus_minus_sql = '''
select
        player_id
        ,season
        ,sum(plus) plus
        ,sum(minus) minus
    from
        (select
            home_player_1_id player_id
            ,home_player_1 player_name
            ,season
            ,coalesce(sum(case when event_team = home_team_abbrev then points_made end),0) plus
            ,coalesce(sum(case when event_team != home_team_abbrev then points_made end), 0) minus
        from nba.pbp
        where event_type_de != 'free-throw'
        group by home_player_1_id, home_player_1, season
        union all
        select
            home_player_2_id player_id
            ,home_player_2 player_name
             ,season
            ,coalesce(sum(case when event_team = home_team_abbrev then points_made end),0) plus
            ,coalesce(sum(case when event_team != home_team_abbrev then points_made end), 0) minus
        from nba.pbp
        where event_type_de != 'free-throw'
        group by home_player_2_id, home_player_2, season
        union all
        select
            home_player_3_id player_id
            ,home_player_3 player_name
            ,season
            ,coalesce(sum(case when event_team = home_team_abbrev then points_made end),0) plus
            ,coalesce(sum(case when event_team != home_team_abbrev then points_made end), 0) minus
        from nba.pbp
        where event_type_de != 'free-throw'
        group by home_player_3_id, home_player_3, season
        union all
        select
            home_player_4_id player_id
            ,home_player_4 player_name
            ,season
            ,coalesce(sum(case when event_team = home_team_abbrev then points_made end),0) plus
            ,coalesce(sum(case when event_team != home_team_abbrev then points_made end), 0) minus
        from nba.pbp
        where event_type_de != 'free-throw'
        group by home_player_4_id, home_player_4, season
        union all
        select
            home_player_5_id player_id
            ,home_player_5 player_name
            ,season
             ,coalesce(sum(case when event_team = home_team_abbrev then points_made end),0) plus
            ,coalesce(sum(case when event_team != home_team_abbrev then points_made end), 0) minus
        from nba.pbp
        where event_type_de != 'free-throw'
        group by home_player_5_id, home_player_5, season
        union all
		select
			player_id
			,player_name
                        ,season
		        ,coalesce(sum(case when event_team = home_team_abbrev then points_made end), 0) plus
			 ,coalesce(sum(case when event_team != home_team_abbrev then points_made end), 0) minus
		from
			(select distinct
					p1.home_player_1_id player_id
					,p1.home_player_1 player_name
					,pbp.event_team
					, pbp.game_id
                    ,pbp.season
					,pbp.home_team_abbrev
					,pbp.de
					,pbp.points_made
				from nba.pbp pbp
				join nba.pbp p1
					on p1.period = pbp.period
					and p1.pctimestring = pbp.pctimestring
					and p1.event_type_de = 'foul'
					and p1.game_id = pbp.game_id
				where pbp.event_type_de = 'free-throw' ) foul
				group by player_id, player_name, season
        union all
		select
			player_id
			,player_name
            ,season
			 ,coalesce(sum(case when event_team = home_team_abbrev then points_made end), 0) plus
			 ,coalesce(sum(case when event_team != home_team_abbrev then points_made end), 0) minus
		from
			(select distinct
					p1.home_player_2_id player_id
					,p1.home_player_2 player_name
					,pbp.event_team
                    ,pbp.season
					, pbp.game_id
					,pbp.home_team_abbrev
					,pbp.de
					,pbp.points_made
				from nba.pbp pbp
				join nba.pbp p1
					on p1.period = pbp.period
					and p1.pctimestring = pbp.pctimestring
					and p1.event_type_de = 'foul'
					and p1.game_id = pbp.game_id
				where pbp.event_type_de = 'free-throw') foul
				group by player_id, player_name, season
        union all
			select
				player_id
				,player_name
                ,season
				 ,coalesce(sum(case when event_team = home_team_abbrev then points_made end), 0) plus
				 ,coalesce(sum(case when event_team != home_team_abbrev then points_made end), 0) minus
			from
				(select distinct
						p1.home_player_3_id player_id
						,p1.home_player_3 player_name
						,pbp.event_team
						, pbp.game_id
                        ,pbp.season
						,pbp.home_team_abbrev
						,pbp.de
						,pbp.points_made
					from nba.pbp pbp
					join nba.pbp p1
						on p1.period = pbp.period
						and p1.pctimestring = pbp.pctimestring
						and p1.event_type_de = 'foul'
						and p1.game_id = pbp.game_id
					where pbp.event_type_de = 'free-throw' ) foul
					group by player_id, player_name, season
        union all
			select
				player_id
				,player_name
                ,season
				 ,coalesce(sum(case when event_team = home_team_abbrev then points_made end), 0) plus
				 ,coalesce(sum(case when event_team != home_team_abbrev then points_made end), 0) minus
			from
				(select distinct
						p1.home_player_4_id player_id
						,p1.home_player_4 player_name
						,pbp.event_team
						, pbp.game_id
                        ,pbp.season
						,pbp.home_team_abbrev
						,pbp.de
						,pbp.points_made
					from nba.pbp pbp
					join nba.pbp p1
						on p1.period = pbp.period
						and p1.pctimestring = pbp.pctimestring
						and p1.event_type_de = 'foul'
						and p1.game_id = pbp.game_id
					where pbp.event_type_de = 'free-throw' ) foul
					group by player_id, player_name, season
        union all
			select
				player_id
				,player_name
                ,season
				 ,coalesce(sum(case when event_team = home_team_abbrev then points_made end), 0) plus
				 ,coalesce(sum(case when event_team != home_team_abbrev then points_made end), 0) minus
			from
				(select distinct
						p1.home_player_5_id player_id
						,p1.home_player_5 player_name
						,pbp.event_team
						, pbp.game_id
                        ,pbp.season
						,pbp.home_team_abbrev
						,pbp.de
						,pbp.points_made
					from nba.pbp pbp
					join nba.pbp p1
						on p1.period = pbp.period
						and p1.pctimestring = pbp.pctimestring
						and p1.event_type_de = 'foul'
						and p1.game_id = pbp.game_id
					where pbp.event_type_de = 'free-throw' ) foul
					group by player_id, player_name, season
        union all
        select
            away_player_1_id player_id
            ,away_player_1 player_name
            ,season
            ,coalesce(sum(case when event_team = away_team_abbrev then points_made end),0) plus
            ,coalesce(sum(case when event_team != away_team_abbrev then points_made end), 0) minus
        from nba.pbp
        where pbp.event_type_de != 'free-throw'
        group by away_player_1_id, away_player_1, season
        union all
        select
            away_player_2_id player_id
            ,away_player_2 player_name
            ,season
            ,coalesce(sum(case when event_team = away_team_abbrev then points_made end),0) plus
            ,coalesce(sum(case when event_team != away_team_abbrev then points_made end), 0) minus
        from nba.pbp
        where pbp.event_type_de != 'free-throw'
        group by away_player_2_id, away_player_2, season
        union all
        select
            away_player_3_id player_id
            ,away_player_3 player_name
            ,season
            ,coalesce(sum(case when event_team = away_team_abbrev then points_made end),0) plus
            ,coalesce(sum(case when event_team != away_team_abbrev then points_made end), 0) minus
        from nba.pbp
        where pbp.event_type_de != 'free-throw'
        group by away_player_3_id, away_player_3, season
        union all
        select
            away_player_4_id player_id
            ,away_player_4 player_name
            ,season
            ,coalesce(sum(case when event_team = away_team_abbrev then points_made end),0) plus
            ,coalesce(sum(case when event_team != away_team_abbrev then points_made end), 0) minus
        from nba.pbp
        where pbp.event_type_de != 'free-throw'
        group by away_player_4_id, away_player_4, season
        union all
        select
            away_player_5_id player_id
            ,away_player_5 player_name
            ,season
            ,coalesce(sum(case when event_team = away_team_abbrev then points_made end),0) plus
            ,coalesce(sum(case when event_team != away_team_abbrev then points_made end), 0) minus
        from nba.pbp
        where pbp.event_type_de != 'free-throw'
        group by away_player_5_id, away_player_5, season
        union all
		select
			player_id
			,player_name
            ,season
			 ,coalesce(sum(case when event_team = away_team_abbrev then points_made end), 0) plus
			 ,coalesce(sum(case when event_team != away_team_abbrev then points_made end), 0) minus
		from
			(select distinct
					p1.away_player_1_id player_id
					,p1.away_player_1 player_name
					,pbp.event_team
					, pbp.game_id
                    ,pbp.season
					,pbp.away_team_abbrev
					,pbp.de
					,pbp.points_made
				from nba.pbp pbp
				join nba.pbp p1
					on p1.period = pbp.period
					and p1.pctimestring = pbp.pctimestring
					and p1.event_type_de = 'foul'
					and p1.game_id = pbp.game_id
				where pbp.event_type_de = 'free-throw' ) foul
				group by player_id, player_name, season
        union all
		select
			player_id
			,player_name
            ,season
			 ,coalesce(sum(case when event_team = away_team_abbrev then points_made end), 0) plus
			 ,coalesce(sum(case when event_team != away_team_abbrev then points_made end), 0) minus
		from
			(select distinct
					p1.away_player_2_id player_id
					,p1.away_player_2 player_name
					,pbp.event_team
					, pbp.game_id
                    ,pbp.season
					,pbp.away_team_abbrev
					,pbp.de
					,pbp.points_made
				from nba.pbp pbp
				join nba.pbp p1
					on p1.period = pbp.period
					and p1.pctimestring = pbp.pctimestring
					and p1.event_type_de = 'foul'
					and p1.game_id = pbp.game_id
				where pbp.event_type_de = 'free-throw') foul
				group by player_id, player_name, season
        union all
		select
			player_id
			,player_name
            ,season
			 ,coalesce(sum(case when event_team = away_team_abbrev then points_made end), 0) plus
			 ,coalesce(sum(case when event_team != away_team_abbrev then points_made end), 0) minus
		from
			(select distinct
					p1.away_player_3_id player_id
					,p1.away_player_3 player_name
					,pbp.event_team
					, pbp.game_id
                    ,pbp.season
					,pbp.away_team_abbrev
					,pbp.de
					,pbp.points_made
				from nba.pbp pbp
				join nba.pbp p1
					on p1.period = pbp.period
					and p1.pctimestring = pbp.pctimestring
					and p1.event_type_de = 'foul'
					and p1.game_id = pbp.game_id
				where pbp.event_type_de = 'free-throw' ) foul
				group by player_id, player_name, season
        union all
		select
			player_id
			,player_name
            ,season
			 ,coalesce(sum(case when event_team = away_team_abbrev then points_made end), 0) plus
			 ,coalesce(sum(case when event_team != away_team_abbrev then points_made end), 0) minus
		from
			(select distinct
					p1.away_player_4_id player_id
					,p1.away_player_4 player_name
					,pbp.event_team
					, pbp.game_id
                    ,pbp.season
					,pbp.away_team_abbrev
					,pbp.de
					,pbp.points_made
				from nba.pbp pbp
				join nba.pbp p1
					on p1.period = pbp.period
					and p1.pctimestring = pbp.pctimestring
					and p1.event_type_de = 'foul'
					and p1.game_id = pbp.game_id
				where pbp.event_type_de = 'free-throw') foul
				group by player_id, player_name, season
        union all
		select
			player_id
			,player_name
            ,season
			 ,coalesce(sum(case when event_team = away_team_abbrev then points_made end), 0) plus
			 ,coalesce(sum(case when event_team != away_team_abbrev then points_made end), 0) minus
		from
			(select distinct
					p1.away_player_5_id player_id
					,p1.away_player_5 player_name
					,pbp.event_team
					, pbp.game_id
                    ,pbp.season
					,pbp.away_team_abbrev
					,pbp.de
					,pbp.points_made
				from nba.pbp pbp
				join nba.pbp p1
					on p1.period = pbp.period
					and p1.pctimestring = pbp.pctimestring
					and p1.event_type_de = 'foul'
					and p1.game_id = pbp.game_id
				where pbp.event_type_de = 'free-throw' ) foul
				group by player_id, player_name, season) pm
        group by player_id, player_name, season
        '''
player_possession_query = '''
    select
        pbg.player_id
        ,pbg.season
        ,sum(poss.possessions) possessions
    from nba.playerbygamestats pbg
    join nba.player_possessions poss
        on poss.game_id = pbg.game_id
        and poss.player_id = pbg.player_id
    group by pbg.player_id, pbg.season
'''
player_possessions = pd.read_sql_query(player_possession_query, engine)
plus_minus_df = pd.read_sql_query(plus_minus_sql, engine)
pm_df = plus_minus_df[~plus_minus_df.isna()]
ratings_df = pm_df.merge(player_possessions, on=['player_id', 'season'])
ratings_df['off_rating'] = (ratings_df['plus'] * 100)/ratings_df['possessions']
ratings_df['def_rating'] = (ratings_df['minus'] * 100)/ratings_df['possessions']
team_df = pd.read_sql_query(team_query, engine)

#calculating effective fg% and true fg%
players_df = pd.read_sql_query('select * from nba.playerbygamestats where toc > 0;', engine)
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
