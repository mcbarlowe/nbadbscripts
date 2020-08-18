drop materialized view if exists nba.player_multi_rapm_view;
create materialized view nba.player_multi_rapm_view as
select pbg.*, pd.gp, pd.teams

from nba.player_three_year_rapm pbg
join nba.player_seasons pd
    on pd.player_id = pbg.player_id
    and pd.min_season = pbg.min_season;


create index rapm_multi_player on nba.player_single_rapm_view (player_id);
create index rapm_multi_season on nba.player_single_rapm_view (min_season);

