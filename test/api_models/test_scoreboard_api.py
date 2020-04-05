from nba_api_models.scoreboard_api import ScoreBoard
from datetime import date


def test_ScoreBoard_creation():
    """
    method to test that a ScoreBoard API object is being created
    succesfully
    """
    game_date = date(2020, 4, 4)
    score_board = ScoreBoard(game_date)
    assert score_board.leagueid == "00"
    assert score_board.day_offset == "0"
    assert score_board.game_date == game_date
    assert score_board.game_date_str == "2020-04-04"
    assert (
        score_board.url
        == "https://stats.nba.com/stats/scoreboard?DayOffset=0&GameDate=2020-04-04&LeagueID=00"
    )


def test_ScoreBoard_response():
    """
    test to make sure the correct data is being returned from ScoreBoard API
    endpoint
    """

    game_date = date(2020, 4, 4)
    score_board = ScoreBoard(game_date)

    games = score_board.response()

    assert type(games) == list
    assert 21901141 in games
    assert 21901142 in games
