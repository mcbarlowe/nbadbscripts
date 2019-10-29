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
                and tbg1.season = {season}
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
        where event_type_de != 'free-throw' and season = {season}
        group by home_player_1_id, home_player_1, season
        union all
        select
            home_player_2_id player_id
            ,home_player_2 player_name
             ,season
            ,coalesce(sum(case when event_team = home_team_abbrev then points_made end),0) plus
            ,coalesce(sum(case when event_team != home_team_abbrev then points_made end), 0) minus
        from nba.pbp
        where event_type_de != 'free-throw' and season = {season}
        group by home_player_2_id, home_player_2, season
        union all
        select
            home_player_3_id player_id
            ,home_player_3 player_name
            ,season
            ,coalesce(sum(case when event_team = home_team_abbrev then points_made end),0) plus
            ,coalesce(sum(case when event_team != home_team_abbrev then points_made end), 0) minus
        from nba.pbp
        where event_type_de != 'free-throw' and season = {season}
        group by home_player_3_id, home_player_3, season
        union all
        select
            home_player_4_id player_id
            ,home_player_4 player_name
            ,season
            ,coalesce(sum(case when event_team = home_team_abbrev then points_made end),0) plus
            ,coalesce(sum(case when event_team != home_team_abbrev then points_made end), 0) minus
        from nba.pbp
        where event_type_de != 'free-throw' and season = {season}
        group by home_player_4_id, home_player_4, season
        union all
        select
            home_player_5_id player_id
            ,home_player_5 player_name
            ,season
             ,coalesce(sum(case when event_team = home_team_abbrev then points_made end),0) plus
            ,coalesce(sum(case when event_team != home_team_abbrev then points_made end), 0) minus
        from nba.pbp
        where event_type_de != 'free-throw' and season = {season}
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
				where pbp.event_type_de = 'free-throw' and pbp.season = {season} ) foul
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
				where pbp.event_type_de = 'free-throw' and pbp.season = {season} ) foul
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
					where pbp.event_type_de = 'free-throw' and pbp.season = {season}  ) foul
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
					where pbp.event_type_de = 'free-throw' and pbp.season = {season}  ) foul
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
					where pbp.event_type_de = 'free-throw' and pbp.season = {season}  ) foul
					group by player_id, player_name, season
        union all
        select
            away_player_1_id player_id
            ,away_player_1 player_name
            ,season
            ,coalesce(sum(case when event_team = away_team_abbrev then points_made end),0) plus
            ,coalesce(sum(case when event_team != away_team_abbrev then points_made end), 0) minus
        from nba.pbp
        where pbp.event_type_de != 'free-throw' and pbp.season = {season}
        group by away_player_1_id, away_player_1, season
        union all
        select
            away_player_2_id player_id
            ,away_player_2 player_name
            ,season
            ,coalesce(sum(case when event_team = away_team_abbrev then points_made end),0) plus
            ,coalesce(sum(case when event_team != away_team_abbrev then points_made end), 0) minus
        from nba.pbp
        where pbp.event_type_de != 'free-throw' and pbp.season = {season}
        group by away_player_2_id, away_player_2, season
        union all
        select
            away_player_3_id player_id
            ,away_player_3 player_name
            ,season
            ,coalesce(sum(case when event_team = away_team_abbrev then points_made end),0) plus
            ,coalesce(sum(case when event_team != away_team_abbrev then points_made end), 0) minus
        from nba.pbp
        where pbp.event_type_de != 'free-throw' and pbp.season = {season}
        group by away_player_3_id, away_player_3, season
        union all
        select
            away_player_4_id player_id
            ,away_player_4 player_name
            ,season
            ,coalesce(sum(case when event_team = away_team_abbrev then points_made end),0) plus
            ,coalesce(sum(case when event_team != away_team_abbrev then points_made end), 0) minus
        from nba.pbp
        where pbp.event_type_de != 'free-throw' and pbp.season = {season}
        group by away_player_4_id, away_player_4, season
        union all
        select
            away_player_5_id player_id
            ,away_player_5 player_name
            ,season
            ,coalesce(sum(case when event_team = away_team_abbrev then points_made end),0) plus
            ,coalesce(sum(case when event_team != away_team_abbrev then points_made end), 0) minus
        from nba.pbp
        where pbp.event_type_de != 'free-throw' and pbp.season = {season}
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
				where pbp.event_type_de = 'free-throw' and pbp.season = {season}  ) foul
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
				where pbp.event_type_de = 'free-throw' and pbp.season = {season} ) foul
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
				where pbp.event_type_de = 'free-throw' and pbp.season = {season}  ) foul
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
				where pbp.event_type_de = 'free-throw' and pbp.season = {season} ) foul
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
				where pbp.event_type_de = 'free-throw' and pbp.season = {season}  ) foul
				group by player_id, player_name, season) pm
        where season = {season}
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
    where pbg.season = {season}
    group by pbg.player_id, pbg.season
'''
