#! /bin/bash
month=`date -v-1d +"%-m"`
day=`date -v-1d +"%-d"`
year=`date -v-1d +"%Y"`

$HOME/.virtualenvs/nbadb/bin/python -m batch_processes.daily_scrape --year $year --month $month --day $day
