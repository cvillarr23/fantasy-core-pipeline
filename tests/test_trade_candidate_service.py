import random
from contextlib import contextmanager
from copy import deepcopy
from unittest.mock import MagicMock, patch

import pytest

from src.optimization import roster_move_scoring
from src.services import trade_candidate_service as service
from src.services.trade_score_service import shape_trade_score
from src.services.trade_service import summarize_raw_vorp
from tests.test_trade_score_service import ALL_TEAMS, WEEKS, player, team, week_row

SLOTS = {"QB": 1, "RB": 1, "WR": 1, "RB/WR/TE": 1, "BE": 1, "IR": 1}
PLAYING = {week: set(ALL_TEAMS) for week in WEEKS}


def league(teams):
    """Three teams by default; rosters sized to SLOTS' limit of 5 non-IR players."""
    return {
        "league_id": "123",
        "season": 2026,
        "as_of_week": 5,
        "start_week": 5,
        "end_week": 6,
        "regular_season_end_week": 14,
        "lineup_slot_counts": dict(SLOTS),
        "roster_snapshot": {
            "kind": "current", "source": "ESPN mRoster",
            "fetched_at": "2026-10-06T12:00:00+00:00", "observed_week": 5,
            "matches_as_of_week": True,
        },
        "strength_metric": {
            "name": "completed_fantasy_points_per_week_z", "source": "test",
            "league_mean": 100.0, "league_stddev": 10.0, "teams_with_strength": 3,
        },
        "coverage": {"unmapped_espn_ids": [], "missing_metric_count": 0},
        "teams": teams,
    }


def schedule(team_id):
    others = [t for t in ("1", "2", "3") if t != team_id]
    return [week_row(5, others[0], 0.5), week_row(6, others[1], -0.5)]


@pytest.fixture
def context():
    """Team 1 (us) is thin at RB; team 2 is deep there; team 3 is balanced. Team 2's
    bench RB 203 beats our RB starter, and our bench RB 105 is no use to them: a
    bench-for-bench swap they break even on (I = 0)."""
    return league({
        "1": team("1", [
            player("101", "QB", 5.0, slot="QB"),
            player("102", "RB", 2.0, slot="RB"),
            player("103", "WR", 9.0, slot="WR"),
            player("104", "WR", 3.0, slot="RB/WR/TE"),
            player("105", "RB", 1.0),
        ], schedule("1")),
        "2": team("2", [
            player("201", "QB", 6.0, nfl_team="BUF", slot="QB"),
            player("202", "RB", 9.0, nfl_team="BUF", slot="RB"),
            player("204", "RB", 7.0, nfl_team="BUF", slot="RB/WR/TE"),
            player("205", "WR", 2.5, nfl_team="BUF", slot="WR"),
            player("203", "RB", 4.0, nfl_team="BUF"),
        ], schedule("2")),
        "3": team("3", [
            player("301", "QB", 4.0, nfl_team="DAL", slot="QB"),
            player("302", "RB", 5.0, nfl_team="DAL", slot="RB"),
            player("303", "WR", 5.0, nfl_team="DAL", slot="WR"),
            player("304", "TE", 3.0, nfl_team="DAL", slot="RB/WR/TE"),
            player("305", "WR", {5: 6.0, 6: None}, nfl_team="DAL"),
        ], schedule("3")),
    })


def full_score(context, counterparty, send, receive):
    send_ids = [p["espn_id"] for p in send]
    receive_ids = [p["espn_id"] for p in receive]
    raw = summarize_raw_vorp({}, [p["player_id"] for p in receive], [p["player_id"] for p in send])
    return shape_trade_score(context, "1", counterparty, send_ids, receive_ids, raw, PLAYING)


def brute_force(context):
    """Every package in the universe, with the prefilter's verdict and full scoring's.
    The solve cache only saves time; test_solve_cache_returns_the_uncached_score covers it."""
    prefilter = service.Prefilter(context, PLAYING)
    results = []
    with roster_move_scoring.lineup_solve_cache():
        for counterparty, send, receive in service.enumerate_packages(context, "1"):
            keep, _, _ = prefilter.check("1", counterparty, send, receive)
            score = full_score(context, counterparty, send, receive)
            status = service.classify(service.eligibility_reasons(score))
            results.append((counterparty, [p["espn_id"] for p in send], [p["espn_id"] for p in receive], keep, status))
    return results


def random_league(seed):
    rng = random.Random(seed)
    positions = ["QB", "RB", "WR", "RB"]
    teams = {}
    for team_id, nfl_team in (("1", "KC"), ("2", "BUF"), ("3", "DAL")):
        roster = []
        for index, position in enumerate(positions):
            vorp = {week: round(rng.uniform(-2, 10), 1) for week in WEEKS}
            if rng.random() < 0.15:
                vorp[rng.choice(WEEKS)] = None
            roster.append(player(
                f"{team_id}0{index}", position, vorp,
                nfl_team=rng.choice(sorted(ALL_TEAMS)) if rng.random() < 0.5 else nfl_team,
            ))
        teams[team_id] = team(team_id, roster, schedule(team_id))
    return league(teams)


@pytest.fixture
def fixture_results(context):
    return brute_force(context)


def test_prefilter_never_drops_a_proposable_package(fixture_results):
    results = fixture_results
    proposable = [r for r in results if r[4] == "proposable"]
    assert proposable, "fixture should have proposable packages"
    assert all(r[3] for r in proposable)
    # The prefilter does real work: it drops packages full scoring rejects anyway.
    assert any(not r[3] for r in results)


def test_prefilter_keeps_a_break_even_package_for_the_counterparty(context, fixture_results):
    """We get their bench RB 203, they get our bench RB 105: their lineup is unchanged,
    I = 0 passes the worker's gate, so "both sides beat a starter" would be too strict."""
    by_package = {(r[0], tuple(r[1]), tuple(r[2])): r for r in fixture_results}
    score = full_score(context, "2", [context["teams"]["1"]["roster"][4]], [context["teams"]["2"]["roster"][4]])
    assert score["score"]["counterparty_lineup_change"] == 0
    _, _, _, keep, status = by_package[("2", ("105",), ("203",))]
    assert status == "proposable" and keep


def test_prefilter_skips_weeks_a_team_can_never_be_scored_in(context):
    """Week 6 has no projections for team 3 yet and its week 5 lineup has no QB, so no
    package with team 3 can score a week: the prefilter drops them all, soundly."""
    for p in context["teams"]["3"]["roster"]:
        p["metrics"]["6"]["vorp_score"] = None
    context["teams"]["3"]["roster"][0]["metrics"]["5"]["vorp_score"] = None
    results = [r for r in brute_force(context) if r[0] == "3"]
    assert results and not any(r[3] for r in results)
    assert not any(r[4] == "proposable" for r in results)
    assert all(r[3] for r in brute_force(context) if r[4] == "proposable")


@pytest.mark.parametrize("seed", range(3))
def test_prefilter_is_sound_on_random_leagues(seed):
    results = brute_force(random_league(seed))
    assert all(r[3] for r in results if r[4] == "proposable")


def test_classification_covers_every_status(context):
    rows, counts = service.build_candidates(context, "1", PLAYING, {}, max_candidates=1000)
    statuses = {row["status"] for row in rows}
    assert statuses == {"proposable", "needs_drop", "one_sided"}
    needs_drop = next(row for row in rows if row["status"] == "needs_drop")
    assert needs_drop["reasons"] == ["exceeds_roster_limit"]
    assert needs_drop["shape"] in ("2for1", "1for2")
    one_sided = next(row for row in rows if row["status"] == "one_sided")
    assert set(one_sided["reasons"]) & {"no_gain_for_us", "hurts_counterparty", "no_scored_weeks"}
    assert counts["scored"] == len(rows) == counts["prefiltered"]
    assert counts["proposable"] + counts["needs_drop"] + counts["one_sided"] == len(rows)
    assert [t["team_id"] for t in counts["teams"]] == ["2", "3"]
    assert sum(t["proposable"] for t in counts["teams"]) == counts["proposable"]


def test_eligibility_reasons_match_the_worker_gates():
    def score(gain, change, weeks=(5,), over=False, unmapped=()):
        return {
            "score": {"our_lineup_gain": gain, "counterparty_lineup_change": change},
            "horizon": {"scored_weeks": list(weeks)},
            "roster_impact": {"us": {"exceeds_roster_limit": over}, "counterparty": {"exceeds_roster_limit": False}},
            "coverage": {"unmapped_traded_espn_ids": list(unmapped)},
        }

    assert service.eligibility_reasons(score(1.0, 0.0)) == []
    assert service.eligibility_reasons(score(1.0, 0.0, over=True)) == ["exceeds_roster_limit"]
    assert service.eligibility_reasons(score(0.0, -1.0)) == ["no_gain_for_us", "hurts_counterparty"]
    assert service.eligibility_reasons(score(None, None, weeks=())) == [
        "no_scored_weeks", "no_gain_for_us", "hurts_counterparty",
    ]
    assert service.eligibility_reasons(score(1.0, 1.0, unmapped=["9"])) == ["unmapped_traded_player"]
    assert service.classify([]) == "proposable"
    assert service.classify(["exceeds_roster_limit"]) == "needs_drop"
    assert service.classify(["exceeds_roster_limit", "hurts_counterparty"]) == "one_sided"


def test_out_and_ir_players_are_never_traded(context):
    context["teams"]["2"]["roster"][4]["injury_status"] = "OUT"
    context["teams"]["3"]["roster"][4]["slot"] = "IR"
    context["teams"]["1"]["roster"][0]["player_id"] = None
    traded = {
        p["espn_id"]
        for _, send, receive in service.enumerate_packages(context, "1")
        for p in send + receive
    }
    assert not traded & {"203", "305", "101"}


def test_shortlist_is_capped_in_bound_order_and_keys_are_stable(context):
    rows, counts = service.build_candidates(context, "1", PLAYING, {}, max_candidates=3)
    assert len(rows) == counts["scored"] == 3
    assert counts["prefiltered"] > 3
    again, _ = service.build_candidates(deepcopy(context), "1", PLAYING, {}, max_candidates=3)
    assert [r["candidate_key"] for r in rows] == [r["candidate_key"] for r in again]
    key = service.candidate_key("123", 2026, "1", "2", ["b", "a"], ["c"])
    assert key == service.candidate_key("123", 2026, "1", "2", ["a", "b"], ["c"])
    assert key != service.candidate_key("123", 2026, "1", "2", ["c"], ["a", "b"])


def test_candidate_rows_carry_players_and_the_full_score(context):
    for p in context["teams"]["2"]["roster"]:
        p["nfl_bye_week"] = 7
    rows, _ = service.build_candidates(context, "1", PLAYING, {"s203": 1.0}, max_candidates=1000)
    row = next(r for r in rows if [p["espn_id"] for p in r["receive"]] == ["203"] and r["shape"] == "1for1")
    assert row["receive"][0] == {
        "espn_id": "203", "player_id": "s203", "name": "Player 203", "position": "RB",
        "nfl_team": "BUF", "vorp": 4.0, "nfl_bye_week": 7,
    }
    assert row["counterparty_name"] == "Team 2"
    assert row["score"]["raw_vorp"]["incoming_total"] == 1.0
    assert row["score"]["scoring_version"] == "trade-score-v1"


def test_solve_cache_returns_the_uncached_score(context):
    send, receive = [context["teams"]["1"]["roster"][3]], [context["teams"]["2"]["roster"][2]]
    uncached = full_score(context, "2", send, receive)
    with roster_move_scoring.lineup_solve_cache():
        full_score(context, "2", send, receive)
        cached = full_score(context, "2", send, receive)
    assert cached == uncached


def test_generate_reads_context_schedule_and_vorp_once_each(context):
    with patch.object(service, "get_league_trade_context", return_value=context) as get_context, \
         patch.object(service, "load_teams_playing_by_week", return_value=PLAYING) as playing, \
         patch.object(service, "load_vorp_by_player", return_value={}) as vorp:
        _, rows, counts = service.generate_trade_candidates("123", 2026, 5, 2, "1")
    get_context.assert_called_once_with("123", 2026, 5, 2)
    playing.assert_called_once_with(2026, 5, 6)
    vorp.assert_called_once()
    assert vorp.call_args.args[:2] == (2026, 5)
    assert counts["scored"] == len(rows)


def test_generate_rejects_an_unknown_team(context):
    with patch.object(service, "get_league_trade_context", return_value=context):
        with pytest.raises(ValueError):
            service.generate_trade_candidates("123", 2026, 5, 2, "9")


@contextmanager
def fake_cursor(cursor):
    yield cursor


def test_store_run_replaces_the_week_in_one_transaction(context):
    rows, counts = service.build_candidates(context, "1", PLAYING, {}, max_candidates=2)
    cursor = MagicMock()
    with patch.object(service, "get_db_cursor", lambda: fake_cursor(cursor)):
        run_id, _ = service.store_run("123", 2026, 5, 2, "1", rows, counts, run_id="run-1")
    assert run_id == "run-1"
    statements = [c.args[0] for c in cursor.execute.call_args_list]
    assert "INSERT INTO trade_candidate_runs" in statements[0]
    assert "DELETE FROM trade_candidates" in statements[1]
    assert cursor.execute.call_args_list[1].args[1] == ("123", 2026, 5)
    params = cursor.executemany.call_args.args[1]
    assert len(params) == 2
    assert params[0][4] == "run-1" and params[0][7] == rows[0]["candidate_key"]


def test_store_run_with_no_rows_still_clears_the_week():
    cursor = MagicMock()
    with patch.object(service, "get_db_cursor", lambda: fake_cursor(cursor)):
        run_id, _ = service.store_run("123", 2026, 5, 2, "1", [], {"teams": []})
    assert run_id
    assert cursor.execute.call_count == 2
    cursor.executemany.assert_not_called()
