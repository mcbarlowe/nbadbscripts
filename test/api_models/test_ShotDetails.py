from nba_api_models.shot_details_api import ShotDetails


def test_ShotDetails_creation():

    shot_detail = ShotDetails(21900001, 200768)
    assert shot_detail.GameID == "0021900001"
    assert shot_detail.PlayerID == 200768
    assert shot_detail.AheadBehind == ""
    assert shot_detail.ClutchTime == ""
    assert shot_detail.ContextFilter == ""
    assert shot_detail.ContextMeasure == "FGM"
    assert shot_detail.DateFrom == ""
    assert shot_detail.DateTo == ""
    assert shot_detail.EndPeriod == ""
    assert shot_detail.EndRange == ""
    assert shot_detail.GameSegment == ""
    assert shot_detail.LastNGames == 0
    assert shot_detail.LeagueID == "00"
    assert shot_detail.Location == ""
    assert shot_detail.Month == 0
    assert shot_detail.OpponentTeamID == 0
    assert shot_detail.Outcome == ""
    assert shot_detail.Period == 0
    assert shot_detail.PlayerPosition == ""
    assert shot_detail.PointDiff == ""
    assert shot_detail.Position == ""
    assert shot_detail.RangeType == ""
    assert shot_detail.RookieYear == ""
    assert shot_detail.Season == ""
    assert shot_detail.SeasonSegment == ""
    assert shot_detail.SeasonType == "Regular Season"
    assert shot_detail.StartPeriod == ""
    assert shot_detail.StartRange == ""
    assert shot_detail.TeamID == 0
    assert shot_detail.VsConference == ""
    assert shot_detail.VsDivision == ""
    assert shot_detail.url == "https://stats.nba.com/stats/shotchartdetail"


def test_ShotDetails_response():
    """
    test for the response method to make sure the data its pulling is correct
    """

    shot_detail = ShotDetails(21900001, 200768)
    shot_detail_df = shot_detail.response()
    assert shot_detail_df["player_name"].unique()[0] == "Kyle Lowry"
    assert shot_detail_df["player_id"].unique()[0] == 200768
    assert shot_detail_df["team_name"].unique()[0] == "Toronto Raptors"
    assert shot_detail_df["team_id"].unique()[0] == 1610612761
    assert shot_detail_df["htm"].unique()[0] == "TOR"
    assert shot_detail_df["vtm"].unique()[0] == "NOP"
