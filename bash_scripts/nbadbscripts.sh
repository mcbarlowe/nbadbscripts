#! /bin/bash
month=`date -v-1d +"%-m"`
day=`date -v-1d +"%-d"`
year=`date -v-1d +"%Y"`

echo "`date`[INFO] Running Daily Scraping of NBA games and inserting into Database"
cd $HOME/code/python/nbadbscripts
$HOME/.virtualenvs/nbadb/bin/python -m batch_processes.daily_scrape --year $year --month $month --day $day

echo "`date`[INFO] Building view of Player stats per game"
/usr/local/bin/psql $NBA_CONNECT -f bash_scripts/sql_scripts/per_game_stats.sql
echo "`date`[INFO] Building view of Player stats per possession"
/usr/local/bin/psql $NBA_CONNECT -f bash_scripts/sql_scripts/per_poss_stats.sql
