from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from src.services import waiver_service


@contextmanager
def _fake_db_cursor(cursor):
    yield cursor


def _players_rows(*rows):
    """rows: (player_id, espn_id, name, team, injury_status)"""
    return list(rows)


def test_compute_waiver_recommendations_sorted_by_vorp_delta_desc(monkeypatch):
    cursor = MagicMock()
    cursor.fetchall.return_value = _players_rows(
        ("rb_fa", "101", "FA RB", "SEA", None),
        ("rb_bench", "102", "Bench RB", "NYJ", "Questionable"),
        ("wr_fa", "201", "FA WR", "DAL", None),
        ("wr_bench", "202", "Bench WR", "MIA", None),
    )

    monkeypatch.setattr(
        waiver_service.waiver_strategy,
        "load_bench_by_position",
        lambda cur, team_id, season, week: {"RB": []},
    )
    monkeypatch.setattr(
        waiver_service.waiver_strategy,
        "load_best_free_agent_by_position",
        lambda cur, season, week: {"placeholder": True},
    )
    monkeypatch.setattr(
        waiver_service.waiver_strategy,
        "propose_waiver_moves",
        lambda bench, free_agents, margin: [
            {
                "position": "RB",
                "add_player_id": "rb_fa",
                "drop_player_id": "rb_bench",
                "add_vorp_score": 5.0,
                "drop_vorp_score": 4.0,
            },
            {
                "position": "WR",
                "add_player_id": "wr_fa",
                "drop_player_id": "wr_bench",
                "add_vorp_score": 9.0,
                "drop_vorp_score": 1.0,
            },
        ],
    )

    with patch("src.services.waiver_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        result = waiver_service.compute_waiver_recommendations(season=2026, week=3)

    assert result["season"] == 2026
    assert result["week"] == 3
    positions = [r["position"] for r in result["recommendations"]]
    assert positions == ["WR", "RB"]
    assert result["recommendations"][0]["vorp_delta"] == 8.0
    assert result["recommendations"][0]["free_agent"] == {
        "espn_id": "201",
        "name": "FA WR",
        "pro_team": "DAL",
        "injury_status": None,
        "net_vorp": 9.0,
    }
    assert result["recommendations"][1]["bench_player"]["injury_status"] == "Questionable"
    assert result["recommendations"][0]["rumor_sentiment"] is None
    assert result["recommendations"][0]["rumor_confidence"] is None
    assert result["recommendations"][0]["rumor_suggested_action"] is None


def test_compute_waiver_recommendations_empty_when_no_moves(monkeypatch):
    cursor = MagicMock()
    cursor.fetchall.return_value = []

    monkeypatch.setattr(
        waiver_service.waiver_strategy, "load_bench_by_position", lambda cur, team_id, season, week: {"RB": [{"player_id": "rb_bench", "vorp_score": 1.0}]}
    )
    monkeypatch.setattr(
        waiver_service.waiver_strategy, "load_best_free_agent_by_position", lambda cur, season, week: {"RB": {"player_id": "rb_fa", "vorp_score": 1.0}}
    )
    monkeypatch.setattr(
        waiver_service.waiver_strategy, "propose_waiver_moves", lambda bench, free_agents, margin: []
    )

    with patch("src.services.waiver_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        result = waiver_service.compute_waiver_recommendations(season=2026, week=3)

    assert result["recommendations"] == []


def test_compute_waiver_recommendations_uses_display_margin_not_live_threshold(monkeypatch):
    """Regression guard: display recommendations must pass margin=0.0, not
    waiver_strategy.MARGIN_THRESHOLD - a marginal upgrade that wouldn't auto-publish
    live should still surface here for a manager to decide on."""
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    captured_margin = {}

    monkeypatch.setattr(
        waiver_service.waiver_strategy, "load_bench_by_position", lambda cur, team_id, season, week: {"RB": [{"player_id": "rb_bench", "vorp_score": 1.0}]}
    )
    monkeypatch.setattr(
        waiver_service.waiver_strategy, "load_best_free_agent_by_position", lambda cur, season, week: {"RB": {"player_id": "rb_fa", "vorp_score": 1.0}}
    )

    def _capture_margin(bench, free_agents, margin):
        captured_margin["value"] = margin
        return []

    monkeypatch.setattr(waiver_service.waiver_strategy, "propose_waiver_moves", _capture_margin)

    with patch("src.services.waiver_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        waiver_service.compute_waiver_recommendations(season=2026, week=3)

    assert captured_margin["value"] == 0.0


# ---------------------------------------------------------------------------
# Pregame fallback (#185): before the refresher writes the target week, fill the page
# from prior same-season weeks instead of returning an empty list.
# ---------------------------------------------------------------------------

TEAMS_PLAYING = {"SEA", "NYJ", "DAL", "MIA", "KC"}


def _fallback_rows():
    """(player_id, position, team, week, vorp_score, on_our_bench, espn_injury_status)
    for Weeks 2-3."""
    return [
        ("rb_bench", "RB", "NYJ", 2, 2.0, True, None), ("rb_bench", "RB", "NYJ", 3, 4.0, True, None),
        ("wr_bench", "WR", "MIA", 3, 1.0, True, "OUT"),
        ("rb_fa", "RB", "SEA", 2, 6.0, False, None), ("rb_fa", "RB", "SEA", 3, 10.0, False, None),
        ("rb_fa_worse", "RB", "DAL", 3, 5.0, False, None),
        ("wr_fa_on_bye", "WR", "BUF", 3, 50.0, False, None),
        ("wr_fa", "WR", "KC", 3, 3.0, False, None),
    ]


def _run_with_empty_target_week(monkeypatch, fallback_rows, week=4, teams_playing=TEAMS_PLAYING):
    cursor = MagicMock()
    cursor.fetchall.side_effect = [fallback_rows, []]
    captured = {}
    monkeypatch.setattr(waiver_service.waiver_strategy, "load_bench_by_position", lambda cur, team_id, season, week: {})
    monkeypatch.setattr(waiver_service.waiver_strategy, "load_best_free_agent_by_position", lambda cur, season, week: {})

    def _capture(bench, free_agents, margin):
        captured.update(bench=bench, free_agents=free_agents)
        return []

    monkeypatch.setattr(waiver_service.waiver_strategy, "propose_waiver_moves", _capture)
    monkeypatch.setenv("ESPN_TEAM_ID", "8")
    schedule = (
        patch("src.services.waiver_service.load_teams_playing", side_effect=teams_playing)
        if isinstance(teams_playing, Exception)
        else patch("src.services.waiver_service.load_teams_playing", return_value=teams_playing)
    )
    with patch("src.services.waiver_service.get_db_cursor", lambda: _fake_db_cursor(cursor)), schedule:
        try:
            result = waiver_service.compute_waiver_recommendations(season=2026, week=week)
        except waiver_service.WaiverInputsUnavailableError as exc:
            result = exc
    return result, captured, cursor


def test_target_week_rows_are_used_without_fallback(monkeypatch):
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    monkeypatch.setattr(waiver_service.waiver_strategy, "load_bench_by_position",
                        lambda cur, team_id, season, week: {"RB": [{"player_id": "b", "vorp_score": 1.0}]})
    monkeypatch.setattr(waiver_service.waiver_strategy, "load_best_free_agent_by_position",
                        lambda cur, season, week: {"RB": {"player_id": "f", "vorp_score": 2.0}})
    monkeypatch.setattr(waiver_service.waiver_strategy, "propose_waiver_moves", lambda bench, free_agents, margin: [])

    with patch("src.services.waiver_service.get_db_cursor", lambda: _fake_db_cursor(cursor)), \
         patch("src.services.waiver_service.load_teams_playing") as mock_schedule:
        result = waiver_service.compute_waiver_recommendations(season=2026, week=4)

    assert result["source_mode"] == "target_week"
    assert result["source_weeks"] == [4]
    # Only the IR slot reads ran, never the prior-week fallback query.
    assert not any("weekly_stats" in c.args[0] for c in cursor.execute.call_args_list)
    mock_schedule.assert_not_called()


def test_empty_target_week_falls_back_to_prior_week_means(monkeypatch):
    result, captured, _ = _run_with_empty_target_week(monkeypatch, _fallback_rows())

    assert result["source_mode"] == "rolling_prior_weeks"
    assert result["source_weeks"] == [2, 3]
    assert captured["bench"] == {
        "RB": [{"player_id": "rb_bench", "vorp_score": 3.0, "espn_injury_status": None}],
        "WR": [{"player_id": "wr_bench", "vorp_score": 1.0, "espn_injury_status": "OUT"}],
    }
    # Best free agent per position by mean VORP; the BUF player is on bye in Week 4.
    assert captured["free_agents"] == {
        "RB": {"player_id": "rb_fa", "vorp_score": 8.0},
        "WR": {"player_id": "wr_fa", "vorp_score": 3.0},
    }


def test_fallback_query_stays_in_season_and_excludes_other_teams_rosters(monkeypatch):
    _, _, cursor = _run_with_empty_target_week(monkeypatch, _fallback_rows())

    query, params = next(c.args for c in cursor.execute.call_args_list if "weekly_stats" in c.args[0])
    assert "s.season = %s" in query
    assert "vorp_score IS NOT NULL" in query
    assert "r.player_id IS NULL OR (r.team_id = %s" in query
    # A player stashed in IR is never a drop candidate.
    assert "r.slot_position <> 'IR'" in query
    assert params == (2026, 1, 3, "8")


def test_fallback_fails_when_prior_data_is_stale(monkeypatch):
    stale = [row for row in _fallback_rows() if row[3] == 2]

    result, _, _ = _run_with_empty_target_week(monkeypatch, stale)

    assert isinstance(result, waiver_service.WaiverInputsUnavailableError)
    assert "stale" in str(result)


def test_fallback_fails_without_any_usable_rows(monkeypatch):
    result, _, _ = _run_with_empty_target_week(monkeypatch, [])

    assert isinstance(result, waiver_service.WaiverInputsUnavailableError)
    assert "Season 2026 Week 4" in str(result)


def test_week_one_never_reads_the_prior_season(monkeypatch):
    result, _, cursor = _run_with_empty_target_week(monkeypatch, _fallback_rows(), week=1)

    assert isinstance(result, waiver_service.WaiverInputsUnavailableError)
    assert "Week 1" in str(result)
    assert not any("weekly_stats" in c.args[0] for c in cursor.execute.call_args_list)


def test_fallback_fails_when_schedule_unavailable(monkeypatch):
    result, _, _ = _run_with_empty_target_week(monkeypatch, _fallback_rows(), teams_playing=RuntimeError("nflverse down"))

    assert isinstance(result, waiver_service.WaiverInputsUnavailableError)
    assert "schedule" in str(result)


def test_fallback_fails_when_a_side_is_empty_after_bye_exclusion(monkeypatch):
    only_bench = [row for row in _fallback_rows() if row[5]]

    result, _, _ = _run_with_empty_target_week(monkeypatch, only_bench)

    assert isinstance(result, waiver_service.WaiverInputsUnavailableError)
    assert "free-agent" in str(result)



# ---------------------------------------------------------------------------
# An IR-eligible drop target becomes "IR X + add Y" while an IR slot is open.
# ---------------------------------------------------------------------------

def _run_ir_case(monkeypatch, bench, free_agents, open_ir_slots, display_rows):
    cursor = MagicMock()
    cursor.fetchall.return_value = display_rows
    monkeypatch.setattr(waiver_service.waiver_strategy, "load_bench_by_position", lambda cur, team_id, season, week: bench)
    monkeypatch.setattr(waiver_service.waiver_strategy, "load_best_free_agent_by_position", lambda cur, season, week: free_agents)
    monkeypatch.setenv("ESPN_TEAM_ID", "8")
    with patch("src.services.waiver_service.get_db_cursor", lambda: _fake_db_cursor(cursor)), \
         patch("src.services.waiver_service.load_open_ir_slots", return_value=open_ir_slots) as mock_slots:
        result = waiver_service.compute_waiver_recommendations(season=2026, week=5)
    return result, mock_slots


IR_DISPLAY_ROWS = [
    ("hurt_rb", "e-hurt", "Hurt Back", "NYJ", "Out"),
    ("healthy_rb", "e-healthy", "Healthy Back", "MIA", None),
    ("fa_rb", "e-fa", "FA Back", "SEA", None),
]
IR_BENCH = {"RB": [
    {"player_id": "hurt_rb", "vorp_score": -2.0, "espn_injury_status": "OUT"},
    {"player_id": "healthy_rb", "vorp_score": 1.0, "espn_injury_status": "ACTIVE"},
]}
IR_FREE_AGENTS = {"RB": {"player_id": "fa_rb", "vorp_score": 4.0}}


def test_ir_eligible_drop_becomes_ir_and_add_when_a_slot_is_open(monkeypatch):
    result, _ = _run_ir_case(monkeypatch, IR_BENCH, IR_FREE_AGENTS, 1, IR_DISPLAY_ROWS)

    [rec] = result["recommendations"]
    assert rec["kind"] == "ir_and_add"
    assert rec["bench_player"] is None
    assert rec["ir_player"]["name"] == "Hurt Back"
    assert rec["ir_player"]["net_vorp"] == -2.0
    assert rec["free_agent"]["name"] == "FA Back"
    # Nobody leaves the roster, so the gain is the add's whole value.
    assert rec["vorp_delta"] == 4.0
    assert result["open_ir_slots"] == 1
    assert [m["player"]["name"] for m in result["ir_moves"]] == ["Hurt Back"]
    assert result["ir_moves"][0]["action"] == "move_to_ir"


def test_ir_eligible_player_is_dropped_normally_when_ir_is_full(monkeypatch):
    result, _ = _run_ir_case(monkeypatch, IR_BENCH, IR_FREE_AGENTS, 0, IR_DISPLAY_ROWS)

    [rec] = result["recommendations"]
    assert rec["kind"] == "swap"
    assert rec["bench_player"]["name"] == "Hurt Back"
    assert rec["ir_player"] is None
    assert result["ir_moves"] == []


def test_open_ir_slots_is_reported_even_with_nobody_to_stash(monkeypatch):
    bench = {"RB": [{"player_id": "healthy_rb", "vorp_score": 1.0, "espn_injury_status": "ACTIVE"}]}

    result, mock_slots = _run_ir_case(monkeypatch, bench, IR_FREE_AGENTS, 2, IR_DISPLAY_ROWS)

    mock_slots.assert_called_once()
    assert result["open_ir_slots"] == 2
    assert result["recommendations"][0]["kind"] == "swap"
    assert result["ir_moves"] == []


def test_injured_star_is_stashed_rather_than_a_healthy_player_dropped(monkeypatch):
    bench = {"RB": [
        {"player_id": "hurt_rb", "vorp_score": 8.0, "espn_injury_status": "OUT"},
        {"player_id": "healthy_rb", "vorp_score": 1.0, "espn_injury_status": "ACTIVE"},
    ]}

    result, _ = _run_ir_case(monkeypatch, bench, IR_FREE_AGENTS, 1, IR_DISPLAY_ROWS)

    [rec] = result["recommendations"]
    assert rec["kind"] == "ir_and_add"
    assert rec["ir_player"]["name"] == "Hurt Back"
    assert rec["bench_player"] is None


@pytest.fixture(autouse=True)
def full_roster_by_default(monkeypatch):
    monkeypatch.setattr(waiver_service.waiver_strategy, "load_rostered_positions", lambda *args: {"K", "D/ST"})
    monkeypatch.setattr(waiver_service.waiver_strategy, "load_open_roster_slots", lambda *args: 0)


@pytest.mark.parametrize("fallback", [False, True])
def test_open_slot_recommends_add_without_a_bench(monkeypatch, fallback):
    cursor = MagicMock()
    monkeypatch.setattr(waiver_service.waiver_strategy, "load_open_roster_slots", lambda *args: 2)
    monkeypatch.setattr(waiver_service.waiver_strategy, "load_bench_by_position", lambda *args: {})
    monkeypatch.setattr(waiver_service.waiver_strategy, "load_best_free_agent_by_position",
                        lambda *args: {} if fallback else {"WR": {"player_id": "fa1", "vorp_score": 8.0}})
    candidates = [{"player_id": "fa1", "position": "WR", "vorp_score": 8.0},
                  {"player_id": "fa2", "position": "WR", "vorp_score": 7.0}]
    monkeypatch.setattr(waiver_service.waiver_strategy, "load_add_candidates", lambda *args: candidates)
    display = [("fa1", "1", "First", "SEA", None), ("fa2", "2", "Second", "SEA", None)]
    if fallback:
        cursor.fetchall.side_effect = [[("fa1", "WR", "SEA", 3, 8.0, False, None),
                                        ("fa2", "WR", "SEA", 3, 7.0, False, None)], display]
    else:
        cursor.fetchall.return_value = display
    with patch.object(waiver_service, "get_db_cursor", lambda: _fake_db_cursor(cursor)), \
         patch.object(waiver_service, "load_open_ir_slots", return_value=0), \
         patch.object(waiver_service, "load_teams_playing", return_value={"SEA"}):
        result = waiver_service.compute_waiver_recommendations(2026, 4)
    assert result["open_roster_slots"] == 2
    assert result["source_mode"] == ("rolling_prior_weeks" if fallback else "target_week")
    assert [r["kind"] for r in result["recommendations"]] == ["add", "add"]
    assert [r["vorp_delta"] for r in result["recommendations"]] == [8.0, 7.0]
    assert all(r["bench_player"] is None and r["ir_player"] is None for r in result["recommendations"])


def test_add_recommendations_precede_larger_swap_gains(monkeypatch):
    cursor = MagicMock()
    cursor.fetchall.return_value = [("fa1", "1", "First", "SEA", None),
                                   ("fa2", "2", "Second", "SEA", None),
                                   ("bench", "3", "Bench", "SEA", None)]
    monkeypatch.setattr(waiver_service.waiver_strategy, "load_open_roster_slots", lambda *args: 1)
    monkeypatch.setattr(waiver_service.waiver_strategy, "load_bench_by_position",
                        lambda *args: {"WR": [{"player_id": "bench", "vorp_score": -10}]})
    monkeypatch.setattr(waiver_service.waiver_strategy, "load_best_free_agent_by_position",
                        lambda *args: {"WR": {"player_id": "fa1", "vorp_score": 8}})
    monkeypatch.setattr(waiver_service.waiver_strategy, "load_add_candidates", lambda *args: [
        {"player_id": "fa1", "position": "WR", "vorp_score": 8},
        {"player_id": "fa2", "position": "WR", "vorp_score": 7}])
    with patch.object(waiver_service, "get_db_cursor", lambda: _fake_db_cursor(cursor)), \
         patch.object(waiver_service, "load_open_ir_slots", return_value=0):
        result = waiver_service.compute_waiver_recommendations(2026, 4)
    assert [(r["kind"], r["vorp_delta"]) for r in result["recommendations"]] == [("add", 8), ("swap", 17)]


@pytest.mark.parametrize("fallback", [False, True])
def test_negative_vorp_open_slot_recommends_healthy_skill_add(monkeypatch, fallback):
    cursor = MagicMock()
    candidates = [
        {"player_id": "k", "position": "K", "vorp_score": -0.1},
        {"player_id": "dst", "position": "D/ST", "vorp_score": -0.2},
        {"player_id": "hurt", "position": "WR", "vorp_score": -1, "injury_status": "Out"},
        {"player_id": "worthy", "position": "WR", "vorp_score": -4.78},
    ]
    monkeypatch.setattr(waiver_service.waiver_strategy, "load_open_roster_slots", lambda *args: 1)
    monkeypatch.setattr(waiver_service.waiver_strategy, "load_bench_by_position", lambda *args: {})
    monkeypatch.setattr(waiver_service.waiver_strategy, "load_best_free_agent_by_position",
                        lambda *args: {} if fallback else {"WR": {"player_id": "worthy", "vorp_score": -4.78}})
    monkeypatch.setattr(waiver_service.waiver_strategy, "load_add_candidates", lambda *args: candidates)
    display = [("worthy", "1", "Xavier Worthy", "KC", None)]
    if fallback:
        rows = [(p["player_id"], p["position"], "KC", 5, p["vorp_score"], False,
                 p.get("injury_status")) for p in candidates]
        cursor.fetchall.side_effect = [rows, display]
    else:
        cursor.fetchall.return_value = display
    with patch.object(waiver_service, "get_db_cursor", lambda: _fake_db_cursor(cursor)), \
         patch.object(waiver_service, "load_open_ir_slots", return_value=0), \
         patch.object(waiver_service, "load_teams_playing", return_value={"KC"}):
        result = waiver_service.compute_waiver_recommendations(2026, 6)
    assert result["open_roster_slots"] == 1
    assert [(r["kind"], r["position"], r["free_agent"]["name"]) for r in result["recommendations"]] == [
        ("add", "WR", "Xavier Worthy")]
    assert result["recommendations"][0]["bench_player"] is None
