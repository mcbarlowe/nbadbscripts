drop materialized view if exists nba.player_single_rapm_view;
create materialized view nba.player_single_rapm_view as
select pbg.*, pd.gp, pd.teams

from nba.player_single_year_rapm pbg
join (select player_id, season, count(*) gp, string_agg(distinct pbg.team_abbrev, '/') teams from nba.playerbygamestats pbg group by pbg.player_id, season) pd
    on pd.player_id = pbg.player_id and pd.season = pbg.min_season;


create index rapm_single_player on nba.player_single_rapm_view (player_id);
create index rapm_single_season on nba.player_single_rapm_view (min_season);

