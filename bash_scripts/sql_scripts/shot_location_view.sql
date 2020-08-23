drop materialized view if exists nba.shot_locations_view;
create materialized view nba.shot_locations_view as
select
    sls.player_id
    ,sls.player_name
    ,sls.team_id
    ,tbg.season
    ,sls.loc_x x
    ,sls.loc_y y
    ,lg_avg.lg_made
    ,lg_avg.lg_attempted
    ,lg_avg.lg_x
    ,lg_avg.lg_y
    ,sum(sls.shot_made_flag) made
    ,count(sls.shot_made_flag) attempted

from nba.shot_locations sls
join nba.teambygamestats tbg on tbg.game_id = sls.game_id and tbg.team_id = sls.team_id
join
    (select
        sl.loc_x lg_x
        ,sl.loc_y lg_y
        ,tbg.season
        ,sum(sl.shot_made_flag) lg_made
        ,count(sl.shot_made_flag) lg_attempted
    from nba.shot_locations sl
    join nba.teambygamestats tbg
        on tbg.team_id = sl.team_id and tbg.game_id = sl.game_id
    group by
        tbg.season,
        sl.loc_x,
        sl.loc_y) lg_avg
    on lg_avg.lg_x = sls.loc_x and lg_avg.lg_y = sls.loc_y and tbg.season = lg_avg.season

group by
    sls.player_id,
    sls.player_name,
    sls.team_id,
    tbg.season,
    sls.loc_x,
    sls.loc_y,
    lg_avg.lg_made,
    lg_avg.lg_attempted,
    lg_avg.lg_x,
    lg_avg.lg_y;

create index sl_player on nba.shot_locations_view (player_id);
create index sl_season on nba.shot_locations_view (season);
create index sl_team on nba.shot_locations_view (team_id);


