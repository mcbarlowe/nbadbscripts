#! /bin/bash
echo "`date`[INFO] Running Weekly Calculations of RAPM stats"
cd $HOME/code/python/nbadbscripts
$HOME/.virtualenvs/nbadb/bin/python -m batch_processes.weekly_stat_calcs --season 2020

echo "`date`[INFO] Building Player Single Year RAPM View"
/usr/local/bin/psql $NBA_CONNECT -f bash_scripts/sql_scripts/player_single_year_rapm_view.sql
echo "`date`[INFO] Building Player Three Year RAPM View"
/usr/local/bin/psql $NBA_CONNECT -f bash_scripts/sql_scripts/player_multi_year_rapm_view.sql
echo "`date`[INFO] Building Player Advanced Stats View"
/usr/local/bin/psql $NBA_CONNECT -f bash_scripts/sql_scripts/player_advanced.sql
echo "`date`[INFO] Building Shot Locations View"
/usr/local/bin/psql $NBA_CONNECT -f bash_scripts/sql_scripts/shot_location_view.sql
