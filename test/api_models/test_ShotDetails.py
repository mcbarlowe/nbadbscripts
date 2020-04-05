from nba_api_models.shot_details_api import ShotDetails


def test_ShotDetails_creation():

    shot_detail = ShotDetails(21900001, 200768)
    assert shot_detail.game_id == 21900001
    assert shot_detail.player_id == 200768
    assert shot_detail.ahead_behind == ""
    assert shot_detail.clutch_time == ""
    assert shot_detail.context_filter == ""
    assert shot_detail.context_measure == "FG"
    assert shot_detail.date_from == ""
    assert shot_detail.date_to == ""
    assert shot_detail.end_period == ""
    assert shot_detail.end_range == ""
    assert shot_detail.game_segment == ""
    assert shot_detail.last_n_games == 0
    assert shot_detail.leagueid == "00"
    assert shot_detail.location == ""
    assert shot_detail.month == 0
    assert shot_detail.opponent_team_id == 0
    assert shot_detail.outcome == ""
    assert shot_detail.period == 0
    assert shot_detail.player_position == ""
    assert shot_detail.point_diff == ""
    assert shot_detail.position == ""
    assert shot_detail.range_type == ""
    assert shot_detail.rookie_year == ""
    assert shot_detail.season == ""
    assert shot_detail.season_segment == ""
    assert shot_detail.season_type == "Regular+Season"
    assert shot_detail.start_period == ""
    assert shot_detail.start_range == ""
    assert shot_detail.team_id == 0
    assert shot_detail.vs_conference == ""
    assert shot_detail.vs_division == ""
    assert shot_detail.url == (
        "https://stats.nba.com/stats/shotchartdetail?AheadBehind="
        "&ClutchTime=&ContextFilter=&ContextMeasure=FGA&DateFrom="
        "&DateTo=&EndPeriod=&EndRange=&GameID=0021900001&GameSegment=&LastNGames=0"
        "&LeagueID=00&Location=&Month=0&OpponentTeamID=0&Outcome=&Period=0"
        "&PlayerID=200768&PlayerPosition=&PointDiff=&Position=&RangeType="
        "&RookieYear=&Season=&SeasonSegment=&SeasonType=Regular+Season"
        "&StartPeriod=&StartRange=&TeamID=0&VsConference=&VsDivision="
    )
