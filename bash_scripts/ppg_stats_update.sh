#! /bin/bash
# This script updates the material views after new data has been added to the database

psql $NBA_CONNECT -f sql_scripts/per_game_stats.sql
psql $NBA_CONNECT -f sql_scripts/per_poss_stats.sql
psql $NBA_CONNECT -f sql_scripts/player_advanced.sql
psql $NBA_CONNECT -f sql_scripts/player_single_year_rapm_view.sql
