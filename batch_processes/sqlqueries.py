pbgs_calc = '''
    insert into nba.playerbygamestats
    (key_col, game_date, game_id, player_id, player_name, team_abbrev, team_id,
     toc, toc_string, fgm, fga, tpm, tpa, ftm, fta, oreb, dreb, ast, tov, stl,
     blk, pf, points, plus_minus)
    select
        cast(pbp.game_id as text) || cast(pbp.player1_id as text) key_col
        , pbp.game_date
        , pbp.game_id
        , pbp.player1_id player_id
        , pbp.player1_name player_name
        , pbp.player1_team_abbreviation team_abbrev
        , pbp.player1_team_id team_id
        , time_on_court.toc
        , time_on_court.toc_string
        , sum(case when pbp.shot_made and pbp.event_type_de = 'shot' then 1 else 0 end) fgm
        , sum(case when pbp.event_type_de in ('shot', 'missed_shot') then 1 else 0 end) fga
        , sum(case when pbp.shot_made and pbp.is_three then 1 else 0 end) tpm
        , sum(case when pbp.is_three then 1 else 0 end) tpa
        , sum(case when pbp.shot_made and pbp.event_type_de = 'free-throw' then 1 else 0 end) ftm
        , sum(case when pbp.event_type_de in ('free-throw') then 1 else 0 end) fta
        , sum(case when pbp.is_o_rebound then 1 else 0 end) oreb
        , sum(case when pbp.is_d_rebound then 1 else 0 end) dreb
        , coalesce(ast.ast, 0) ast
        , sum(case when pbp.is_turnover then 1 else 0 end) tov
        , coalesce(steal.stl, 0) stl
        , coalesce(block.blk, 0) blk
        , sum(case when pbp.event_type_de = 'foul' then 1 else 0 end) pf
        , sum(pbp.points_made) points
        , coalesce(plusminus.plus_minus, 0) plus_minus
    from nba.pbp pbp
    left join (
            select
                player3_id player_id
                , game_id
                , sum(case when is_block then 1 else 0 end) blk
            from nba.pbp
            group by player3_id, game_id) block
            on block.player_id = pbp.player1_id
            and block.game_id = pbp.game_id
    left join (
            select
                player2_id player_id
                ,game_id
                ,sum(case when shot_made and event_type_de in ('shot') then 1 else 0 end) ast
            from nba.pbp
            group by player2_id, game_id) ast
            on ast.player_id = pbp.player1_id
            and ast.game_id = pbp.game_id
    left join (
            select
                player2_id player_id
                ,game_id
                ,sum(case when is_steal then 1 else 0 end) stl
            from nba.pbp
            group by player2_id, game_id) steal
            on steal.player_id = pbp.player1_id
            and steal.game_id = pbp.game_id
    left join (
    select
        player_id
        ,game_id
        ,sum(plus) - sum(minus) plus_minus
    from
        (select
            home_player_1_id player_id
            ,home_player_1 player_name
            ,game_id
            ,sum(case when event_team = home_team_abbrev then points_made end) plus
            ,sum(case when event_team != home_team_abbrev then points_made end) minus
        from nba.pbp
        where event_type_de != 'free-throw'
        group by home_player_1_id, home_player_1, game_id
        union all
        select
            home_player_2_id player_id
            ,home_player_2 player_name
            ,game_id
            ,sum(case when event_team = home_team_abbrev then points_made end) plus
            ,sum(case when event_team != home_team_abbrev then points_made end) minus
        from nba.pbp
        where event_type_de != 'free-throw'
        group by home_player_2_id, home_player_2, game_id
        union all
        select
            home_player_3_id player_id
            ,home_player_3 player_name
            ,game_id
            ,sum(case when event_team = home_team_abbrev then points_made end) plus
            ,sum(case when event_team != home_team_abbrev then points_made end) minus
        from nba.pbp
        where event_type_de != 'free-throw'
        group by home_player_3_id, home_player_3, game_id
        union all
        select
            home_player_4_id player_id
            ,home_player_4 player_name
            ,game_id
            ,sum(case when event_team = home_team_abbrev then points_made end) plus
            ,sum(case when event_team != home_team_abbrev then points_made end) minus
        from nba.pbp
        where event_type_de != 'free-throw'
        group by home_player_4_id, home_player_4, game_id
        union all
        select
            home_player_5_id player_id
            ,home_player_5 player_name
            ,game_id
            ,sum(case when event_team = home_team_abbrev then points_made end) plus
            ,sum(case when event_team != home_team_abbrev then points_made end) minus
        from nba.pbp
        where event_type_de != 'free-throw'
        group by home_player_5_id, home_player_5, game_id
        union all
        select
            p1.home_player_1_id player_id
            ,p1.home_player_1 player_name
            ,pbp.game_id
            ,sum(case when pbp.event_team = pbp.home_team_abbrev then pbp.points_made end) plus
            ,sum(case when pbp.event_team != pbp.home_team_abbrev then pbp.points_made end) minus
        from nba.pbp pbp
        join nba.pbp p1
            on p1.period = pbp.period
            and p1.pctimestring = pbp.pctimestring
            and p1.event_type_de = 'foul'
        where pbp.event_type_de = 'free-throw'
        group by p1.home_player_1_id, p1.home_player_1, pbp.game_id
        union all
        select
            p1.home_player_2_id player_id
            ,p1.home_player_2 player_name
            ,pbp.game_id
            ,sum(case when pbp.event_team = pbp.home_team_abbrev then pbp.points_made end) plus
            ,sum(case when pbp.event_team != pbp.home_team_abbrev then pbp.points_made end) minus
        from nba.pbp pbp
        join nba.pbp p1
            on p1.period = pbp.period
            and p1.pctimestring = pbp.pctimestring
            and p1.event_type_de = 'foul'
        where pbp.event_type_de = 'free-throw'
        group by p1.home_player_2_id, p1.home_player_2, pbp.game_id
        union all
        select
            p1.home_player_3_id player_id
            ,p1.home_player_3 player_name
            ,pbp.game_id
            ,sum(case when pbp.event_team = pbp.home_team_abbrev then pbp.points_made end) plus
            ,sum(case when pbp.event_team != pbp.home_team_abbrev then pbp.points_made end) minus
        from nba.pbp pbp
        join nba.pbp p1
            on p1.period = pbp.period
            and p1.pctimestring = pbp.pctimestring
            and p1.event_type_de = 'foul'
        where pbp.event_type_de = 'free-throw'
        group by p1.home_player_3_id, p1.home_player_3, pbp.game_id
        union all
        select
            p1.home_player_4_id player_id
            ,p1.home_player_4 player_name
            , pbp.game_id
            ,sum(case when pbp.event_team = pbp.home_team_abbrev then pbp.points_made end) plus
            ,sum(case when pbp.event_team != pbp.home_team_abbrev then pbp.points_made end) minus
        from nba.pbp pbp
        join nba.pbp p1
            on p1.period = pbp.period
            and p1.pctimestring = pbp.pctimestring
            and p1.event_type_de = 'foul'
        where pbp.event_type_de = 'free-throw'
        group by p1.home_player_4_id, p1.home_player_4, pbp.game_id
        union all
        select
            p1.home_player_5_id player_id
            ,p1.home_player_5 player_name
            , pbp.game_id
            ,sum(case when pbp.event_team = pbp.home_team_abbrev then pbp.points_made end) plus
            ,sum(case when pbp.event_team != pbp.home_team_abbrev then pbp.points_made end) minus
        from nba.pbp pbp
        join nba.pbp p1
            on p1.period = pbp.period
            and p1.pctimestring = pbp.pctimestring
            and p1.event_type_de = 'foul'
        where pbp.event_type_de = 'free-throw'
        group by p1.home_player_5_id, p1.home_player_5, pbp.game_id
        union all
        select
            away_player_1_id player_id
            ,away_player_1 player_name
            , game_id
            ,sum(case when event_team = away_team_abbrev then points_made end) plus
            ,sum(case when event_team != away_team_abbrev then points_made end) minus
        from nba.pbp
        where event_type_de != 'free-throw'
        group by away_player_1_id, away_player_1, game_id
        union all
        select
            away_player_2_id player_id
            ,away_player_2 player_name
            , game_id
            ,sum(case when event_team = away_team_abbrev then points_made end) plus
            ,sum(case when event_team != away_team_abbrev then points_made end) minus
        from nba.pbp
        where event_type_de != 'free-throw'
        group by away_player_2_id, away_player_2, game_id
        union all
        select
            away_player_3_id player_id
            ,away_player_3 player_name
            , game_id
            ,sum(case when event_team = away_team_abbrev then points_made end) plus
            ,sum(case when event_team != away_team_abbrev then points_made end) minus
        from nba.pbp
        where event_type_de != 'free-throw'
        group by away_player_3_id, away_player_3, game_id
        union all
        select
            away_player_4_id player_id
            ,away_player_4 player_name
            , game_id
            ,sum(case when event_team = away_team_abbrev then points_made end) plus
            ,sum(case when event_team != away_team_abbrev then points_made end) minus
        from nba.pbp
        where event_type_de != 'free-throw'
        group by away_player_4_id, away_player_4, game_id
        union all
        select
            away_player_5_id player_id
            ,away_player_5 player_name
            , game_id
            ,sum(case when event_team = away_team_abbrev then points_made end) plus
            ,sum(case when event_team != away_team_abbrev then points_made end) minus
        from nba.pbp
        where event_type_de != 'free-throw'
        group by away_player_5_id, away_player_5, game_id
        union all
        select
            p1.away_player_1_id player_id
            ,p1.away_player_1 player_name
            , pbp.game_id
            ,sum(case when pbp.event_team = pbp.away_team_abbrev then pbp.points_made end) plus
            ,sum(case when pbp.event_team != pbp.away_team_abbrev then pbp.points_made end) minus
        from nba.pbp pbp
        join nba.pbp p1
            on p1.period = pbp.period
            and p1.pctimestring = pbp.pctimestring
            and p1.event_type_de = 'foul'
        where pbp.event_type_de = 'free-throw'
        group by p1.away_player_1_id, p1.away_player_1, pbp.game_id
        union all
        select
            p1.away_player_2_id player_id
            ,p1.away_player_2 player_name
            , pbp.game_id
            ,sum(case when pbp.event_team = pbp.away_team_abbrev then pbp.points_made end) plus
            ,sum(case when pbp.event_team != pbp.away_team_abbrev then pbp.points_made end) minus
        from nba.pbp pbp
        join nba.pbp p1
            on p1.period = pbp.period
            and p1.pctimestring = pbp.pctimestring
            and p1.event_type_de = 'foul'
        where pbp.event_type_de = 'free-throw'
        group by p1.away_player_2_id, p1.away_player_2, pbp.game_id
        union all
        select
            p1.away_player_3_id player_id
            ,p1.away_player_3 player_name
            , pbp.game_id
            ,sum(case when pbp.event_team = pbp.away_team_abbrev then pbp.points_made end) plus
            ,sum(case when pbp.event_team != pbp.away_team_abbrev then pbp.points_made end) minus
        from nba.pbp pbp
        join nba.pbp p1
            on p1.period = pbp.period
            and p1.pctimestring = pbp.pctimestring
            and p1.event_type_de = 'foul'
        where pbp.event_type_de = 'free-throw'
        group by p1.away_player_3_id, p1.away_player_3, pbp.game_id
        union all
        select
            p1.away_player_4_id player_id
            ,p1.away_player_4 player_name
            , pbp.game_id
            ,sum(case when pbp.event_team = pbp.away_team_abbrev then pbp.points_made end) plus
            ,sum(case when pbp.event_team != pbp.away_team_abbrev then pbp.points_made end) minus
        from nba.pbp pbp
        join nba.pbp p1
            on p1.period = pbp.period
            and p1.pctimestring = pbp.pctimestring
            and p1.event_type_de = 'foul'
        where pbp.event_type_de = 'free-throw'
        group by p1.away_player_4_id, p1.away_player_4, pbp.game_id
        union all
        select
            p1.away_player_5_id player_id
            ,p1.away_player_5 player_name
            , pbp.game_id
            ,sum(case when pbp.event_team = pbp.away_team_abbrev then pbp.points_made end) plus
            ,sum(case when pbp.event_team != pbp.away_team_abbrev then pbp.points_made end) minus
        from nba.pbp pbp
        join nba.pbp p1
            on p1.period = pbp.period
            and p1.pctimestring = pbp.pctimestring
            and p1.event_type_de = 'foul'
        where pbp.event_type_de = 'free-throw'
        group by p1.away_player_5_id, p1.away_player_5, pbp.game_id) pm
        group by player_id, player_name, game_id
        ) plusminus
        on plusminus.player_id = pbp.player1_id
        and plusminus.game_id = pbp.game_id
    left join (
        select
            player_id
            ,player_name
            ,game_id
            ,sum(toc) toc
            ,to_char(sum(toc) * interval '1 second', 'MI:SS') toc_string
        from(
            select
                home_player_1_id player_id
                ,home_player_1 player_name
                ,game_id
                ,sum(event_length) toc
            from nba.pbp
            group by home_player_1_id, home_player_1, game_id
            union all
            select
                home_player_2_id player_id
                ,home_player_2 player_name
                ,game_id
                ,sum(event_length) toc
            from nba.pbp
            group by home_player_2_id, home_player_2, game_id
            union all
            select
                home_player_3_id player_id
                ,home_player_3 player_name
                ,game_id
                ,sum(event_length) toc
            from nba.pbp
            group by home_player_3_id, home_player_3, game_id
            union all
            select
                home_player_4_id player_id
                ,home_player_4 player_name
                ,game_id
                ,sum(event_length) toc
            from nba.pbp
            group by home_player_4_id, home_player_4, game_id
            union all
            select
                home_player_5_id player_id
                ,home_player_5 player_name
                ,game_id
                ,sum(event_length) toc
            from nba.pbp
            group by home_player_5_id, home_player_5, game_id
            union all
            select
                away_player_1_id player_id
                ,away_player_1 player_name
                ,game_id
                ,sum(event_length) toc
            from nba.pbp
            group by away_player_1_id, away_player_1, game_id
            union all
            select
                away_player_2_id player_id
                ,away_player_2 player_name
                ,game_id
                ,sum(event_length) toc
            from nba.pbp
            group by away_player_2_id, away_player_2, game_id
            union all
            select
                away_player_3_id player_id
                ,away_player_3 player_name
                ,game_id
                ,sum(event_length) toc
            from nba.pbp
            group by away_player_3_id, away_player_3, game_id
            union all
            select
                away_player_4_id player_id
                ,away_player_4 player_name
                ,game_id
                ,sum(event_length) toc
            from nba.pbp
            group by away_player_4_id, away_player_4, game_id
            union all
            select
                away_player_5_id player_id
                ,away_player_5 player_name
                ,game_id
                ,sum(event_length) toc
            from nba.pbp
            group by away_player_5_id, away_player_5, game_id) toc
            group by player_id, player_name, game_id) time_on_court
            on time_on_court.player_id = pbp.player1_id
            and time_on_court.game_id = pbp.game_id
    where pbp.player1_id > 0 and pbp.player1_name != '' and pbp.game_id={game_id}
    group by
            pbp.player1_id
            , pbp.player1_name
            , pbp.game_date
            , pbp.game_id
            , pbp.player1_team_abbreviation
            , pbp.player1_team_id
            , block.blk
            , steal.stl
            , plusminus.plus_minus
            , ast.ast
            , time_on_court.toc
            , time_on_court.toc_string;
    '''