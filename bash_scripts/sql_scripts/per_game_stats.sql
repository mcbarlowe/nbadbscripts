drop materialized view if exists nba.per_game_stats;
create materialized view nba.per_game_stats as
select
    pbg.player_name
     ,pbg.player_id
     ,pd.position
     ,pbg.season
     ,string_agg(distinct team_abbrev, '/') teams
     ,count(*) gp
     ,round(avg(pbg.toc::numeric / 60), 1) mins
     ,round(avg(pbg.fgm), 1) fgm
     ,round(avg(pbg.fga), 1) fga
     ,round(avg(pbg.tpm), 1) tpm
     ,round(avg(pbg.tpa), 1) tpa
     ,round(avg(pbg.ftm), 1) ftm
     ,round(avg(pbg.fta), 1) fta
     ,round(avg(pbg.oreb), 1) oreb
     ,round(avg(pbg.dreb), 1) dreb
     ,round(avg(pbg.ast), 1) ast
     ,round(avg(pbg.tov), 1) tov
     ,round(avg(pbg.stl), 1) stl
     ,round(avg(pbg.blk), 1) blk
     ,round(avg(pbg.pf), 1) pf
     ,round(avg(pbg.points), 1) points
     ,round(avg(pbg.plus_minus), 1) plus_minus
from nba.playerbygamestats pbg
join nba.player_details pd on pd.player_id = pbg.player_id
where toc > 0
group by
    pbg.player_id,
    pbg.season,
    pd.position,
    pbg.player_name;

create index pgs_player on nba.per_game_stats (player_id);
create index pgs_season on nba.per_game_stats (season);

