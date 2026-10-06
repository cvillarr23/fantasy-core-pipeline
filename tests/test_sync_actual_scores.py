from copy import deepcopy
import subprocess
import sys
from unittest.mock import MagicMock, patch

import pytest

from src.ingestion import sync_actual_scores as sync


def player(espn_id=100, points=10, slot=0):
    return {
        "playerId": espn_id,
        "lineupSlotId": slot,
        "playerPoolEntry": {
            "player": {
                "id": espn_id,
                "fullName": "Player",
                "defaultPositionId": 1,
                "stats": [
                    {
                        "seasonId": 2026,
                        "scoringPeriodId": 4,
                        "statSourceId": 0,
                        "statSplitTypeId": 1,
                        "appliedTotal": points,
                    }
                ],
            }
        },
    }


@pytest.fixture(autouse=True)
def environment(monkeypatch):
    monkeypatch.setenv("ESPN_LEAGUE_ID", "123")
    monkeypatch.setattr(sync, "build_league", MagicMock())


@pytest.fixture
def snapshot():
    game = {"matchupPeriodId": 4, "winner": "HOME"}
    for side, team in (("home", 1), ("away", 2)):
        game[side] = {
            "teamId": team,
            "totalPoints": 10,
            "pointsByScoringPeriod": {"4": 10},
            "rosterForCurrentScoringPeriod": {"entries": [player(team * 100)]},
        }
    return {
        "id": 123,
        "seasonId": 2026,
        "scoringPeriodId": 5,
        "status": {"finalScoringPeriod": 18},
        "settings": {
            "scheduleSettings": {
                "matchupPeriodCount": 14,
                "matchupPeriods": {str(w): [w] for w in range(1, 19)},
            }
        },
        "teams": [{"id": 1, "name": "Home"}, {"id": 2, "name": "Away"}],
        "schedule": [game],
    }


def scoreboard(week=4, completed=True):
    return {
        "season": {"year": 2026, "type": 2},
        "week": {"number": week},
        "events": [{"id": "game", "status": {"type": {"completed": completed}}}],
    }


def box_for(snapshot):
    box = deepcopy(snapshot)
    box["scoringPeriodId"] = 4
    return box


def test_week_four_is_final_but_week_five_is_upcoming(snapshot):
    assert sync.completed_weeks(snapshot) == list(range(1, 5))
    data = sync.shape_week(snapshot, box_for(snapshot), scoreboard(), "123", 2026, 4)
    assert data["teams"][0]["score"] == 10
    with pytest.raises(sync.IncompleteScoresError):
        sync.shape_week(snapshot, snapshot, scoreboard(5), "123", 2026, 5)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda s: s.update(id=999),
        lambda s: s.update(seasonId=2025),
        lambda s: s["schedule"][0].update(winner="UNDECIDED"),
        lambda s: s["schedule"][0]["home"].update(totalPoints=float("nan")),
        lambda s: s["schedule"][0]["home"].pop("rosterForCurrentScoringPeriod"),
        lambda s: s["schedule"].append(deepcopy(s["schedule"][0])),
    ],
)
def test_bad_week_never_becomes_a_completed_snapshot(snapshot, mutate):
    mutate(snapshot)
    with pytest.raises(sync.IncompleteScoresError):
        sync.shape_week(snapshot, box_for(snapshot), scoreboard(), "123", 2026, 4)


@pytest.mark.parametrize(
    "board",
    [
        scoreboard(completed=False),
        scoreboard(3),
        {"season": {"year": 2026, "type": 2}, "week": {"number": 4}, "events": []},
    ],
)
def test_all_games_must_be_final(snapshot, board):
    with pytest.raises(sync.IncompleteScoresError):
        sync.shape_week(snapshot, box_for(snapshot), board, "123", 2026, 4)


def test_multiweek_score_is_not_the_matchup_total(snapshot):
    periods = snapshot["settings"]["scheduleSettings"]["matchupPeriods"]
    periods["4"] = [4, 5]
    for p in range(5, 18):
        periods[str(p)] = [p + 1]
    del periods["18"]
    snapshot["schedule"][0]["winner"] = "UNDECIDED"
    snapshot["schedule"][0]["home"]["totalPoints"] = 99
    data = sync.shape_week(snapshot, box_for(snapshot), scoreboard(), "123", 2026, 4)
    assert data["teams"][0]["score"] == 10
    with pytest.raises(sync.IncompleteScoresError):
        del snapshot["schedule"][0]["home"]["pointsByScoringPeriod"]
        sync.shape_week(snapshot, box_for(snapshot), scoreboard(), "123", 2026, 4)


def test_missing_actual_is_unknown_and_negative_dst_is_preserved(snapshot):
    entries = snapshot["schedule"][0]["home"]["rosterForCurrentScoringPeriod"][
        "entries"
    ]
    entries.append(player(-1, -2, 20))
    entries.append(player(101, 0, 21))
    entries[-1]["playerPoolEntry"]["player"]["stats"] = []
    data = sync.shape_week(snapshot, box_for(snapshot), scoreboard(), "123", 2026, 4)
    by_id = {p["espn_id"]: p for p in data["players"]}
    assert by_id["-1"]["actual_points"] == -2
    assert by_id["101"]["actual_points"] is None
    assert by_id["-1"]["slot"] == "BE"


def test_missing_starter_actual_fails_without_inventing_zero(snapshot):
    snapshot["schedule"][0]["home"]["rosterForCurrentScoringPeriod"]["entries"][0][
        "playerPoolEntry"
    ]["player"]["stats"] = []
    with pytest.raises(sync.IncompleteScoresError):
        sync.shape_week(snapshot, box_for(snapshot), scoreboard(), "123", 2026, 4)


def test_idempotent_persistence_only_mirrors_actuals(snapshot):
    data = sync.shape_week(snapshot, box_for(snapshot), scoreboard(), "123", 2026, 4)
    conn = MagicMock()
    cursor = conn.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = [
        ("100", "internal"),
        ("200", "other"),
        ("200", "ambiguous"),
    ]
    sync.persist_week(conn, data)
    statements = [c.args[0] for c in cursor.execute.call_args_list] + [
        c.args[0] for c in cursor.executemany.call_args_list
    ]
    assert any(
        "ON CONFLICT" in sql and "fantasy_team_week_scores" in sql for sql in statements
    )
    mirror = next(sql for sql in statements if "UPDATE weekly_stats SET" in sql)
    assert "vorp_score" not in mirror and "projected_points" not in mirror
    assert not any("INSERT INTO weekly_stats" in sql for sql in statements)
    assert any("DELETE FROM fantasy_player_week_scores" in sql for sql in statements)


def test_dry_run_never_opens_database(snapshot):
    with patch.object(sync, "fetch_snapshot", return_value=snapshot), patch.object(
        sync, "fetch_week", return_value=box_for(snapshot)
    ), patch.object(sync, "fetch_scoreboard", return_value=scoreboard()), patch.object(
        sync, "write_snapshot"
    ) as write:
        assert sync.main(["--season", "2026", "--week", "4", "--dry-run"]) == 0
    write.assert_not_called()


def test_read_only_module_does_not_import_database_pool():
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import src.ingestion.sync_actual_scores; assert 'src.database' not in sys.modules",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_commit_precedes_cache_invalidation(snapshot):
    data = sync.shape_week(snapshot, box_for(snapshot), scoreboard(), "123", 2026, 4)
    from contextlib import contextmanager

    events = []

    @contextmanager
    def connection():
        yield MagicMock()
        events.append("commit")

    with patch("src.database.get_db_connection", connection), patch.object(
        sync, "persist_week"
    ), patch(
        "src.services.read_cache.invalidate_league",
        side_effect=lambda league: events.append("invalidate"),
    ):
        sync.write_snapshot(data)
    assert events == ["commit", "invalidate"]


def test_explicit_upcoming_week_fails_instead_of_wrapper_substitution(snapshot):
    with patch.object(sync, "fetch_snapshot", return_value=snapshot), patch.object(
        sync, "fetch_week"
    ) as fetch:
        assert sync.main(["--season", "2026", "--week", "5", "--dry-run"]) == 1
    fetch.assert_not_called()


def test_substituted_box_period_is_rejected(snapshot):
    with pytest.raises(sync.IncompleteScoresError):
        sync.shape_week(snapshot, snapshot, scoreboard(), "123", 2026, 4)


def test_scoreboard_must_cover_full_professional_schedule(snapshot):
    snapshot["_nfl_game_ids_by_week"] = {"4": ["game", "missing"]}
    with pytest.raises(sync.IncompleteScoresError):
        sync.shape_week(snapshot, box_for(snapshot), scoreboard(), "123", 2026, 4)


def test_professional_stats_must_be_official(snapshot):
    snapshot["_nfl_official_weeks"] = []
    with pytest.raises(sync.IncompleteScoresError):
        sync.shape_week(snapshot, box_for(snapshot), scoreboard(), "123", 2026, 4)


def test_missing_professional_week_cannot_bypass_coverage(snapshot):
    snapshot["_nfl_game_ids_by_week"] = {"3": ["game"]}
    with pytest.raises(sync.IncompleteScoresError):
        sync.shape_week(snapshot, box_for(snapshot), scoreboard(), "123", 2026, 4)


def test_explicit_bye_does_not_become_unknown(snapshot):
    first = snapshot["schedule"][0]
    second = {"matchupPeriodId": 4, "home": first.pop("away"), "winner": "UNDECIDED"}
    first["winner"] = "UNDECIDED"
    snapshot["schedule"].append(second)
    data = sync.shape_week(snapshot, box_for(snapshot), scoreboard(), "123", 2026, 4)
    assert all(t["opponent_team_id"] is None for t in data["teams"])


def test_true_zero_and_negative_scores(snapshot):
    home = snapshot["schedule"][0]["home"]
    home.update(totalPoints=0, pointsByScoringPeriod={"4": 0})
    home["rosterForCurrentScoringPeriod"]["entries"][0]["playerPoolEntry"]["player"][
        "stats"
    ][0]["appliedTotal"] = 0
    data = sync.shape_week(snapshot, box_for(snapshot), scoreboard(), "123", 2026, 4)
    assert data["teams"][0]["score"] == 0
    assert data["players"][0]["actual_points"] == 0


def test_final_season_week_can_complete_without_rollover(snapshot):
    snapshot["scoringPeriodId"] = 18
    snapshot["schedule"][0]["matchupPeriodId"] = 18
    assert 18 in sync.completed_weeks(snapshot)


def test_invalid_range_and_zero_week_fail(snapshot):
    with patch.object(sync, "fetch_snapshot", return_value=snapshot), patch.object(
        sync, "fetch_week"
    ) as fetch:
        assert sync.main(["--season", "2026", "--week", "0", "--dry-run"]) == 1
        assert (
            sync.main(
                [
                    "--season",
                    "2026",
                    "--start-week",
                    "0",
                    "--end-week",
                    "4",
                    "--dry-run",
                ]
            )
            == 1
        )
    fetch.assert_not_called()


def test_preseason_persists_future_schedule(monkeypatch, snapshot):
    snapshot["scoringPeriodId"] = 1
    with patch.object(sync, "fetch_snapshot", return_value=snapshot), patch.object(
        sync, "write_schedule_snapshot"
    ) as write:
        assert sync.main(["--season", "2026"]) == 0
    write.assert_called_once()
