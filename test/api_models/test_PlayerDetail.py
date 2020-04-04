from nba_api_models.player_details import PlayerDetails


def test_PlayerDetails_creation():
    """
    function to test the creation of the PlayerDetails class funciton
    """

    player1 = PlayerDetails(playerid=907)
    player2 = PlayerDetails(playerid=779)
    player3 = PlayerDetails(playerid=779, leagueid=1)

    assert player1.playerid == 907
    assert player2.playerid == 779
    assert (
        player1.url
        == "https://stats.nba.com/stats/commonplayerinfo?LeagueID=&PlayerID=907"
    )
    assert (
        player2.url
        == "https://stats.nba.com/stats/commonplayerinfo?LeagueID=&PlayerID=779"
    )
    assert player1.leagueid == ""
    assert player2.leagueid == ""
    assert player3.leagueid == 1
