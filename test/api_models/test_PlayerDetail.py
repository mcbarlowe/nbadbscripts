from nba_api_models.player_details import PlayerDetails


def test_PlayerDetails_creation():
    """
    function to test the creation of the PlayerDetails class funciton
    """

    player1 = PlayerDetails(907)
    player2 = PlayerDetails(779)
    player3 = PlayerDetails(779, leagueid=1)

    assert player1.PlayerID == 907
    assert player2.PlayerID == 779
    assert player1.url == "https://stats.nba.com/stats/commonplayerinfo"
    assert player2.url == "https://stats.nba.com/stats/commonplayerinfo"
    assert player1.LeagueID == ""
    assert player2.LeagueID == ""
    assert player3.LeagueID == 1


def test_build_parameters_dict():

    player1 = PlayerDetails(907)
    url_parameters = player1.build_parameters_dict()

    assert url_parameters == {
        "LeagueID": player1.LeagueID,
        "PlayerID": player1.PlayerID,
    }


def test_get_player_data():
    """
    function to test if the function that returns the player data is returning
    accurate data
    """
    player1 = PlayerDetails(907)
    player2 = PlayerDetails(779)

    player1_details = player1.response()
    player2_details = player2.response()

    assert player1_details["playercode"].values[0] == "malik_sealy"
    assert player2_details["playercode"].values[0] == "glen_rice"
