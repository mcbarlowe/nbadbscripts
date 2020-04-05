"""
This will be the main method I run every day to get the games from yesterdays
schedule, scrape them and then insert them into the database. I will then
calculate RAPMS for single year team and player and multi season rapm for the
current season. This will use my nba_scraper, nba_parser, and new API models
I'm creating for the NBA api to streamline code
"""

import nba_scraper.nba_scraper as ns
import nba_parser as npar


# TODO
# Build method that gets schedule based on date passed to it

# TODO build method to get player shots for the database

# TODO build method to calculate and insert teambygamestats,
# playerbygamestats, player_rapm_shifts could be part of main method

# TODO Build method to scrape games based on output of the schedule API
# method

# TODO build method to incorporate getting player details if they don't alreayd
# exist in the database


#TODO not sure if i want this to run with command line args or not probably will so that i can pass a date and then have it do everything for that date
