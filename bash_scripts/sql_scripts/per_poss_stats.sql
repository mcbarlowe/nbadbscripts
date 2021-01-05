drop materialized view if exists nba.per_poss_stats;
create materialized view nba.per_poss_stats as
select pbg.player_id
    ,pd.position
    ,pbg.player_name
    ,pbg.season
    ,string_agg(distinct pbg.team_abbrev, '/') teams
    ,count(*) gp
    ,round(avg(pbg.toc::numeric / 60), 1) mins
    ,round(sum(pbg.fgm::numeric)/sum(pbg.possessions::numeric) * 100, 1) fgm
    ,round(sum(pbg.fga::numeric)/sum(pbg.possessions::numeric) * 100, 1) fga
    ,round(sum(pbg.tpm::numeric)/sum(pbg.possessions::numeric) * 100, 1) tpm
    ,round(sum(pbg.tpa::numeric)/sum(pbg.possessions::numeric) * 100, 1) tpa
    ,round(sum(pbg.ftm::numeric)/sum(pbg.possessions::numeric) * 100, 1) ftm
    ,round(sum(pbg.fta::numeric)/sum(pbg.possessions::numeric) * 100, 1) fta
    ,round(sum(pbg.oreb::numeric)/sum(pbg.possessions::numeric) * 100, 1) oreb
    ,round(sum(pbg.dreb::numeric)/sum(pbg.possessions::numeric) * 100, 1) dreb
    ,round(sum(pbg.ast::numeric)/sum(pbg.possessions::numeric) * 100, 1) ast
    ,round(sum(pbg.tov::numeric)/sum(pbg.possessions::numeric) * 100, 1) tov
    ,round(sum(pbg.stl::numeric)/sum(pbg.possessions::numeric) * 100, 1) stl
    ,round(sum(pbg.blk::numeric)/sum(pbg.possessions::numeric) * 100, 1) blk
    ,round(sum(pbg.points::numeric)/sum(pbg.possessions::numeric) * 100, 1) points

from nba.playerbygamestats pbg
join nba.player_details pd on pd.player_id = pbg.player_id
where pbg.toc > 0 and (game_id >= 20000000 and game_id < 30000000)
group by
pbg.player_name,
pd.position,
pbg.player_id,
pbg.season
having sum(pbg.possessions) > 0;

create index ppp_player on nba.per_poss_stats (player_id);
create index ppp_season on nba.per_poss_stats (season);

