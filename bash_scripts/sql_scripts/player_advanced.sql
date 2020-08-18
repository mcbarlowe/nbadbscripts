drop materialized view if exists nba.pa_stats_view;
create materialized view nba.pa_stats_view as
select pbg.*, pd.position

from nba.player_advanced_stats pbg
join nba.player_details pd
    on pd.player_id = pbg.player_id;


create index pa_player on nba.pa_stats_view (player_id);
create index pa_season on nba.pa_stats_view (min_season);

