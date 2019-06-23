'''
This script runs the batch processes for the NBA database
'''
import datetime
import os
import logging
import nba_scraper.nba_scraper as ns
import requests
from sqlalchemy import create_engine
import sqlqueries


def get_game_ids(api):
    '''
    This function gets the game ids of games returned from the api
    endpoint

    Inputs:
    api  - nba Scoreboard api endpoint

    Ouputs:
    game_ids - list of game ids
    '''

    user_agent = {'User-agent': 'Mozilla/5.0'}
    scoreboard = requests.get(api, headers=user_agent).json()

    games = scoreboard['resultSets'][0]['rowSet']
    game_ids = [game[2][2:] for game in games]
    return game_ids


def main():
    '''
    Main function to run to scrape games daily
    '''

    engine = create_engine(os.environ['NBA_CONNECT'])

    # Logging stuff
    logging.basicConfig(level=logging.INFO, filename='batchprocess.logs',
                        format='%(asctime)s - %(levelname)s: %(message)s')

    # TODO uncomment this when testing is done
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    #games_api = ('https://stats.nba.com/stats/scoreboard?'
    #             f'DayOffset=0&GameDate={DATE.strftime("%Y-%m-%d")}&LeagueID=00')
    date = '2019-04-04'
    games_api = ('https://stats.nba.com/stats/scoreboard?'
                 f'DayOffset=0&GameDate={date}&LeagueID=00')
    games = get_game_ids(games_api)
    if games == []:
        logging.info("No games on %s", date)
        return
    games_df_list = [ns.scrape_game([game]) for game in games]
    for game_df in games_df_list:
        logging.info("Inserting %s into nba.pbp",
                     game_df.game_id.unique()[0])
        game_df.columns = list(map(str.lower, game_df.columns))
        game_df['key_col'] = (game_df['game_id'].astype(str) +
                              game_df['eventnum'].astype(str) +
                              game_df['game_date'].astype(str) +
                              game_df['home_team_abbrev'] + game_df['away_team_abbrev'])
        game_df = game_df.astype({'is_d_rebound': bool, 'is_o_rebound': bool,
                                  'is_turnover': bool, 'is_steal': bool,
                                  'is_putback': bool, 'is_block': bool,
                                  'is_three': bool, 'shot_made': bool,
                                  'home_player_1_id': int, 'home_player_2_id': int,
                                  'home_player_3_id': int, 'home_player_4_id': int,
                                  'home_player_5_id': int, 'away_player_2_id': int,
                                  'away_player_1_id': int, 'away_player_3_id': int,
                                  'away_player_4_id': int, 'away_player_5_id': int})

        game_df.to_sql('pbp', engine, schema='nba',
                       if_exists='append', index=False)

        logging.info("Inserting boxscore for %s into nba.playerbygamestats",
                     game_df.game_id.unique()[0])

        engine.connect().execute(sqlqueries.pbgs_calc.format(game_id=game_df.game_id.unique()[0]))

if __name__ == '__main__':
    main()
