from unittest.mock import MagicMock, patch

from src.optimization import waiver_evaluator as we


def test_resolve_live_waiver_moves_translates_to_espn_identity():
    cursor = MagicMock()
    # add player_id lookup, then drop player_id lookup.
    cursor.fetchone.side_effect = [("e-4046", "Some Back", "KC"), ("e-9999", "Bench Guy", "SEA")]
    moves = [{
        "position": "RB", "drop_player_id": "bench1", "add_player_id": "4046",
        "add_vorp_score": 5.0, "drop_vorp_score": 1.0,
    }]

    resolved = we.resolve_live_waiver_moves(cursor, moves, season=2023, week=5)

    assert resolved == [{
        "player_id": "e-4046",
        "drop_player_id": "e-9999",
        "name": "Some Back",
        "team": "KC",
        "decision_inputs": {
            "add_player_vorp_score": 5.0,
            "drop_player_vorp_score": 1.0,
            "season": 2023,
            "week": 5,
        },
    }]


def test_resolve_live_waiver_moves_includes_vorp_scores_and_season_week_in_decision_inputs():
    """CORE-17 (#35): /v1/trigger/waiver's decision_inputs field needs the VORP delta that
    justified the claim, so a caller with no season/week (e.g. an older test fixture)
    still gets a decision_inputs dict with None for the missing pieces rather than
    crashing on a missing key."""
    cursor = MagicMock()
    cursor.fetchone.side_effect = [("e-4046", "Some Back", "KC")]
    moves = [{
        "position": "RB", "drop_player_id": None, "add_player_id": "4046",
        "add_vorp_score": 7.5, "drop_vorp_score": -2.0,
    }]

    resolved = we.resolve_live_waiver_moves(cursor, moves)

    assert resolved[0]["decision_inputs"] == {
        "add_player_vorp_score": 7.5,
        "drop_player_vorp_score": -2.0,
        "season": None,
        "week": None,
    }


def test_resolve_live_waiver_moves_skips_when_add_player_has_no_espn_id():
    cursor = MagicMock()
    cursor.fetchone.side_effect = [(None, "Some Back", "KC")]
    moves = [{"position": "RB", "drop_player_id": "bench1", "add_player_id": "4046"}]

    resolved = we.resolve_live_waiver_moves(cursor, moves)

    assert resolved == []


def test_resolve_live_waiver_moves_skips_when_drop_player_has_no_espn_id():
    cursor = MagicMock()
    cursor.fetchone.side_effect = [("e-4046", "Some Back", "KC"), (None, "Bench Guy", "SEA")]
    moves = [{"position": "RB", "drop_player_id": "bench1", "add_player_id": "4046"}]

    resolved = we.resolve_live_waiver_moves(cursor, moves)

    assert resolved == []


def test_resolve_live_waiver_moves_handles_a_move_with_no_drop_player():
    cursor = MagicMock()
    cursor.fetchone.side_effect = [("e-4046", "Some Back", "KC")]
    moves = [{
        "position": "RB", "drop_player_id": None, "add_player_id": "4046",
        "add_vorp_score": 5.0, "drop_vorp_score": None,
    }]

    resolved = we.resolve_live_waiver_moves(cursor, moves, season=2023, week=5)

    assert resolved == [{
        "player_id": "e-4046",
        "drop_player_id": None,
        "name": "Some Back",
        "team": "KC",
        "decision_inputs": {
            "add_player_vorp_score": 5.0,
            "drop_player_vorp_score": None,
            "season": 2023,
            "week": 5,
        },
    }]


WEEK4_TARGET = {
    "player_id": "e-4046", "drop_player_id": "e-9999", "name": "Some Back", "team": "KC",
    "decision_inputs": {"add_player_vorp_score": 5.0, "drop_player_vorp_score": 1.0, "season": 2026, "week": 4},
}


def test_waiver_idempotency_key_is_stable_per_claim_and_week_not_per_job():
    """A queued claim is invisible to the worker's roster check until waivers process, so
    any re-run (new Job) proposing the same claim that week must reuse the key - unlike
    lineup keys, which are Job-scoped because re-applying a lineup is a no-op."""
    rescored = {**WEEK4_TARGET, "decision_inputs": {**WEEK4_TARGET["decision_inputs"], "add_player_vorp_score": 9.9}}
    key = we.waiver_idempotency_key("7", WEEK4_TARGET)

    assert key == we.waiver_idempotency_key("7", rescored)
    assert key.startswith("waiver-7-")
    next_week = {**WEEK4_TARGET, "decision_inputs": {"season": 2026, "week": 5}}
    assert key != we.waiver_idempotency_key("7", next_week)
    assert key != we.waiver_idempotency_key("7", {**WEEK4_TARGET, "drop_player_id": "e-1111"})
    assert key != we.waiver_idempotency_key("8", WEEK4_TARGET)


def test_waiver_idempotency_key_is_none_without_season_week():
    """Otherwise the key would be identical every week and the 7-day TTL could swallow
    next week's legitimate claim."""
    assert we.waiver_idempotency_key("7", {**WEEK4_TARGET, "decision_inputs": {"season": 2026}}) is None
    assert we.waiver_idempotency_key("7", {k: v for k, v in WEEK4_TARGET.items() if k != "decision_inputs"}) is None


def test_publish_waiver_claim_without_a_key_posts_once_without_retries():
    target = {k: v for k, v in WEEK4_TARGET.items() if k != "decision_inputs"}
    with patch("src.optimization.waiver_evaluator.post_trigger") as mock_post, \
         patch("src.optimization.waiver_evaluator.post_trigger_idempotent") as mock_idem:
        assert we.publish_waiver_claim("7", target) is True
    mock_post.assert_called_once()
    mock_idem.assert_not_called()


def test_publish_waiver_claim_posts_idempotently_with_the_claim_key():
    response = MagicMock()
    response.json.return_value = {"status": "accepted"}

    with patch("src.optimization.waiver_evaluator.post_trigger_idempotent", return_value=response) as mock_post:
        assert we.publish_waiver_claim("7", WEEK4_TARGET) is True

    args, kwargs = mock_post.call_args
    assert args[0] == "waiver"
    assert args[1] == {"team_id": "7", **WEEK4_TARGET}
    assert args[2] == we.waiver_idempotency_key("7", WEEK4_TARGET)
    assert kwargs["core_api_base_url"] == we.CORE_API_BASE_URL


def test_publish_waiver_claim_says_so_when_the_claim_was_already_queued(caplog):
    response = MagicMock()
    response.json.return_value = {"status": "duplicate"}

    with patch("src.optimization.waiver_evaluator.post_trigger_idempotent", return_value=response), \
         caplog.at_level("INFO", logger="WaiverEvaluator"):
        assert we.publish_waiver_claim("7", WEEK4_TARGET) is False

    assert "already queued" in caplog.text
    assert "Published waiver claim" not in caplog.text


def test_run_weekly_waivers_live_wires_decision_logic_to_publish():
    resolved_target = {"player_id": "e-4046", "drop_player_id": "e-9999", "name": "Some Back", "team": "KC"}

    with patch("src.optimization.waiver_evaluator.get_db_connection") as mock_get_conn, \
         patch("src.optimization.waiver_evaluator._horizon_pools") as mock_pools, \
         patch("src.optimization.waiver_evaluator.waiver_strategy.propose_waiver_moves") as mock_propose, \
         patch("src.optimization.waiver_evaluator.resolve_live_waiver_moves", return_value=[resolved_target]) as mock_resolve, \
         patch("src.optimization.waiver_evaluator.publish_waiver_claim") as mock_publish:
        mock_conn = MagicMock()
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        bench = {"RB": [{"player_id": "bench1", "vorp_score": 1.0}]}
        fa = {"RB": {"player_id": "4046", "vorp_score": 5.0}}
        mock_pools.return_value = (bench, fa)
        mock_propose.return_value = [{"position": "RB", "drop_player_id": "bench1", "add_player_id": "4046"}]

        result = we.run_weekly_waivers_live("7", 2023, 5)

    mock_propose.assert_called_once_with(bench, fa, margin=we.waiver_strategy.MARGIN_THRESHOLD)
    # CORE-17 (#35): resolve_live_waiver_moves needs season/week to fold into
    # decision_inputs - the reasoning bot_decisions records for the eventual claim.
    expected_cursor = mock_conn.cursor.return_value.__enter__.return_value
    mock_resolve.assert_called_once_with(expected_cursor, mock_propose.return_value, 2023, 5)
    mock_publish.assert_called_once_with("7", resolved_target, we.CORE_API_BASE_URL)
    assert result == [resolved_target]


# ---------------------------------------------------------------------------
# Missing target-week inputs (#178 follow-up): an empty bench/free-agent pool means the
# target week's weekly_stats haven't landed, not "no waiver moves worth making".
# ---------------------------------------------------------------------------

import pytest


def test_load_live_waiver_inputs_raises_when_no_free_agents_have_target_week_rows():
    with patch("src.optimization.waiver_evaluator._horizon_pools",
               return_value=({"RB": [{"player_id": "bench1", "vorp_score": 1.0}]}, {})), \
         pytest.raises(we.InsufficientWaiverInputError) as exc:
        we.load_live_waiver_inputs(MagicMock(), "7", 2026, 5)
    assert "free-agent" in str(exc.value)
    assert "Season 2026 Week 5" in str(exc.value)


def test_load_live_waiver_inputs_raises_when_bench_has_no_target_week_rows():
    with patch("src.optimization.waiver_evaluator._horizon_pools",
               return_value=({}, {"RB": {"player_id": "4046", "vorp_score": 5.0}})), \
         pytest.raises(we.InsufficientWaiverInputError) as exc:
        we.load_live_waiver_inputs(MagicMock(), "7", 2026, 5)
    assert "bench" in str(exc.value)


def test_load_live_waiver_inputs_returns_both_pools():
    bench = {"RB": [{"player_id": "bench1", "vorp_score": 1.0}]}
    fa = {"RB": {"player_id": "4046", "vorp_score": 5.0}}
    with patch("src.optimization.waiver_evaluator._horizon_pools", return_value=(bench, fa)):
        assert we.load_live_waiver_inputs(MagicMock(), "7", 2026, 5) == (bench, fa)


def test_live_pools_value_players_over_the_horizon_with_byes_as_replacement_level():
    cursor = MagicMock()
    # A bench stud on bye in week 5 and a scrub; the old single-week pool had no row
    # for the stud (or a 0.0 one that made him the drop).
    cursor.fetchall.return_value = [
        ("stud", "e1", "Stud", "RB", "BUF", w, 12.0, True, False, "BE", None, None) for w in (6, 7, 8)
    ] + [
        ("scrub", "e2", "Scrub", "RB", "KC", w, 1.0, True, False, "BE", None, None) for w in (5, 6, 7, 8)
    ] + [
        ("fa", "e3", "FA", "RB", "KC", w, 5.0, False, None, None, None, None) for w in (5, 6, 7, 8)
    ]
    with patch("src.optimization.waiver_evaluator.bye_weeks_or_empty", return_value={"BUF": 5}):
        bench, fa = we._horizon_pools(cursor, "7", 2026, 5)

    moves = we.waiver_strategy.propose_waiver_moves(bench, fa)
    assert [m["drop_player_id"] for m in moves] == ["scrub"]
    assert cursor.execute.call_args.args[1] == ("7", 2026, 5, 8, "7")


def test_run_weekly_waivers_live_publishes_nothing_on_insufficient_input():
    with patch("src.optimization.waiver_evaluator.get_db_connection"), \
         patch("src.optimization.waiver_evaluator.load_live_waiver_inputs",
               side_effect=we.InsufficientWaiverInputError("no free-agent rows")), \
         patch("src.optimization.waiver_evaluator.publish_waiver_claim") as mock_publish, \
         pytest.raises(we.InsufficientWaiverInputError):
        we.run_weekly_waivers_live("7", 2026, 5)
    mock_publish.assert_not_called()


WAIVER_ARGV = ["--team-id", "7", "--season", "2026", "--week", "5"]


def test_main_exits_nonzero_with_reason_on_insufficient_input(caplog):
    with patch("src.optimization.waiver_evaluator.run_weekly_waivers_live",
               side_effect=we.InsufficientWaiverInputError("no free-agent rows")):
        assert we.main(WAIVER_ARGV) == 1
    assert "cannot evaluate Team 7 Season 2026 Week 5" in caplog.text
    assert "no free-agent rows" in caplog.text


def test_run_weekly_waivers_live_logs_a_valid_zero_move_result(caplog):
    """#159: a real-input run that proposes nothing went silent after "Evaluating...",
    indistinguishable in the logs from a hang. Say so explicitly."""
    pools = ({"RB": [{"player_id": "bench1", "vorp_score": 9.0}]}, {"RB": {"player_id": "4046", "vorp_score": 5.0}})
    with patch("src.optimization.waiver_evaluator.get_db_connection"), \
         patch("src.optimization.waiver_evaluator.load_live_waiver_inputs", return_value=pools), \
         patch("src.optimization.waiver_evaluator.waiver_strategy.propose_waiver_moves", return_value=[]), \
         patch("src.optimization.waiver_evaluator.resolve_live_waiver_moves", return_value=[]), \
         patch("src.optimization.waiver_evaluator.publish_waiver_claim") as mock_publish, \
         caplog.at_level("INFO", logger="WaiverEvaluator"):
        assert we.run_weekly_waivers_live("7", 2026, 5) == []
    mock_publish.assert_not_called()
    [record] = [r for r in caplog.records if "No waiver moves proposed" in r.getMessage()]
    assert record.levelname == "INFO"
    assert "Team 7 Season 2026 Week 5" in record.getMessage()


def test_run_weekly_waivers_live_warns_when_every_proposed_move_is_dropped(caplog):
    """Worthwhile moves that all fail espn_id resolution are a crosswalk gap blocking real
    claims - not the same as "nothing worth claiming", so it must not read as healthy."""
    pools = ({"RB": [{"player_id": "bench1", "vorp_score": 1.0}]}, {"RB": {"player_id": "4046", "vorp_score": 9.0}})
    proposed = [{"position": "RB", "drop_player_id": "bench1", "add_player_id": "4046"}] * 3
    with patch("src.optimization.waiver_evaluator.get_db_connection"), \
         patch("src.optimization.waiver_evaluator.load_live_waiver_inputs", return_value=pools), \
         patch("src.optimization.waiver_evaluator.waiver_strategy.propose_waiver_moves", return_value=proposed), \
         patch("src.optimization.waiver_evaluator.resolve_live_waiver_moves", return_value=[]), \
         patch("src.optimization.waiver_evaluator.publish_waiver_claim") as mock_publish, \
         caplog.at_level("INFO", logger="WaiverEvaluator"):
        assert we.run_weekly_waivers_live("7", 2026, 5) == []
    mock_publish.assert_not_called()
    assert not any("No waiver moves proposed" in r.getMessage() for r in caplog.records)
    [record] = [r for r in caplog.records if "espn_id" in r.getMessage()]
    assert record.levelname == "WARNING"
    assert "3" in record.getMessage()


def test_run_weekly_waivers_live_withholds_ir_and_add_moves_while_ir_is_open(caplog):
    """Auto-publish of IR moves stays off: the free agent that should come in through an IR
    stash is logged, not published, and never claimed by dropping the healthy player."""
    pools = (
        {"RB": [{"player_id": "hurt", "vorp_score": 6.0, "espn_injury_status": "OUT"},
                {"player_id": "healthy", "vorp_score": 2.0, "espn_injury_status": "ACTIVE"}]},
        {"RB": {"player_id": "4046", "vorp_score": 9.0}},
    )
    with patch("src.optimization.waiver_evaluator.get_db_connection"), \
         patch("src.optimization.waiver_evaluator.load_live_waiver_inputs", return_value=pools), \
         patch("src.optimization.waiver_evaluator.load_open_ir_slots", return_value=1), \
         patch("src.optimization.waiver_evaluator.resolve_live_waiver_moves", return_value=[]) as mock_resolve, \
         patch("src.optimization.waiver_evaluator.publish_waiver_claim") as mock_publish, \
         caplog.at_level("INFO", logger="WaiverEvaluator"):
        assert we.run_weekly_waivers_live("7", 2026, 5) == []

    mock_publish.assert_not_called()
    assert mock_resolve.call_args.args[1] == []
    [record] = [r for r in caplog.records if "suggest IR hurt + add 4046" in r.getMessage()]
    assert record.levelname == "WARNING"
    assert "not published" in record.getMessage()


def test_run_weekly_waivers_live_refreshes_rosters_before_reading_them():
    order = []
    with patch("src.optimization.waiver_evaluator.sync_league.refresh_rosters_if_stale",
               side_effect=lambda season: order.append(("refresh", season))), \
         patch("src.optimization.waiver_evaluator.get_db_connection",
               side_effect=lambda: order.append(("db",)) or MagicMock()), \
         patch("src.optimization.waiver_evaluator.load_live_waiver_inputs", return_value=({}, {})), \
         patch("src.optimization.waiver_evaluator.waiver_strategy.propose_waiver_moves", return_value=[]):
        we.run_weekly_waivers_live("7", 2026, 5)
    assert order[:2] == [("refresh", 2026), ("db",)]


def test_plan_live_waiver_moves_skips_the_ir_slot_lookup_when_nobody_is_eligible():
    moves = [{"position": "RB", "drop_player_id": "b", "add_player_id": "f", "add_vorp_score": 5.0, "drop_vorp_score": 1.0}]
    bench = {"RB": [{"player_id": "b", "vorp_score": 1.0, "espn_injury_status": "QUESTIONABLE"}]}
    with patch("src.optimization.waiver_evaluator.load_open_ir_slots") as mock_slots, \
         patch("src.optimization.waiver_evaluator.waiver_strategy.propose_waiver_moves", return_value=moves):
        assert we.plan_live_waiver_moves(MagicMock(), "7", 2026, bench, {"RB": {"player_id": "f", "vorp_score": 5.0}}, week=5) == moves
    mock_slots.assert_not_called()


def test_main_exits_zero_when_no_moves_are_worth_making():
    with patch("src.optimization.waiver_evaluator.run_weekly_waivers_live", return_value=[]) as mock_run:
        assert we.main(WAIVER_ARGV) == 0
    mock_run.assert_called_once_with("7", 2026, 5)


# ---------------------------------------------------------------------------
# #191: the weekly waiver CronJob retries (in-process, and pod-level under restartPolicy:
# Never) without ever queueing a claim twice. End to end through main() and the real
# trigger client, against a fake /v1/trigger/waiver that dedupes on idempotency_key the
# way the gateway does.
# ---------------------------------------------------------------------------

import json

import requests

CLAIM_1 = {
    "player_id": "e-4046", "drop_player_id": "e-9999", "name": "Some Back", "team": "KC",
    "decision_inputs": {"add_player_vorp_score": 5.0, "drop_player_vorp_score": 1.0, "season": 2026, "week": 5},
}
CLAIM_2 = {
    "player_id": "e-5050", "drop_player_id": "e-8888", "name": "Other Receiver", "team": "BUF",
    "decision_inputs": {"add_player_vorp_score": 4.0, "drop_player_vorp_score": 0.5, "season": 2026, "week": 5},
}


def _response(status_code, body):
    response = requests.Response()
    response.status_code = status_code
    response._content = json.dumps(body).encode()
    response.url = "http://gateway/v1/trigger/waiver"
    return response


class _FakeWaiverGateway:
    """Queues each idempotency key once; a repeat is 202 "duplicate". `failures` maps a
    player_id to the exceptions/status codes to serve before that claim succeeds."""

    def __init__(self, failures=None):
        self.failures = {k: list(v) for k, v in (failures or {}).items()}
        self.queued = []
        self.posts = []

    def post(self, url, json, headers, timeout):
        self.posts.append(json)
        pending = self.failures.get(json["player_id"])
        if pending:
            failure = pending.pop(0)
            if isinstance(failure, Exception):
                raise failure
            return _response(failure, {"detail": "failed"})
        key = json["idempotency_key"]
        if key in self.queued:
            return _response(202, {"status": "duplicate"})
        self.queued.append(key)
        return _response(202, {"status": "accepted"})


def _run_main(gateway, claims=(CLAIM_1, CLAIM_2)):
    pools = ({"RB": [{"player_id": "bench1", "vorp_score": 1.0}]}, {"RB": {"player_id": "4046", "vorp_score": 5.0}})
    with patch("src.optimization.waiver_evaluator.get_db_connection"), \
         patch("src.optimization.waiver_evaluator.load_live_waiver_inputs", return_value=pools), \
         patch("src.optimization.waiver_evaluator.waiver_strategy.propose_waiver_moves", return_value=[{}] * len(claims)), \
         patch("src.optimization.waiver_evaluator.resolve_live_waiver_moves", return_value=[dict(c) for c in claims]), \
         patch("src.trigger_client.time.sleep") as mock_sleep, \
         patch("src.trigger_client.requests.post", side_effect=gateway.post):
        return we.main(WAIVER_ARGV), mock_sleep


KEY_1 = we.waiver_idempotency_key("7", CLAIM_1)
KEY_2 = we.waiver_idempotency_key("7", CLAIM_2)


def test_main_survives_a_refused_connection_and_queues_each_claim_once():
    """An API rollout refusing the first connection (what failed the 2026-09-30 lineup run)
    is retried in-process under the same key: the Job succeeds, each claim queued once."""
    gateway = _FakeWaiverGateway(failures={"e-4046": [requests.ConnectionError("Connection refused")]})

    exit_code, mock_sleep = _run_main(gateway)

    assert exit_code == 0
    assert gateway.queued == [KEY_1, KEY_2]
    assert [p["idempotency_key"] for p in gateway.posts] == [KEY_1, KEY_1, KEY_2]
    mock_sleep.assert_called_once()


def test_a_pod_retry_after_claim_2_fails_queues_only_claim_2():
    """Claims publish one at a time: claim 1 is queued, then claim 2 exhausts its retries
    and the pod fails. The Job's next pod re-runs everything - claim 1 must come back
    "duplicate" and only claim 2 is queued."""
    gateway = _FakeWaiverGateway(failures={"e-5050": [503] * 6})

    with pytest.raises(requests.HTTPError):
        _run_main(gateway)
    assert gateway.queued == [KEY_1]

    exit_code, _ = _run_main(gateway)

    assert exit_code == 0
    assert gateway.queued == [KEY_1, KEY_2]
    assert [p["idempotency_key"] for p in gateway.posts[-2:]] == [KEY_1, KEY_2]


def test_a_4xx_is_not_retried():
    """A 4xx (the #187 namespace guard's 422, a bad API key) gets the same answer every
    time: fail the run at once rather than burn the retry budget."""
    gateway = _FakeWaiverGateway(failures={"e-4046": [422]})

    with pytest.raises(requests.HTTPError):
        _run_main(gateway)

    assert len(gateway.posts) == 1
    assert gateway.queued == []


def test_main_exits_nonzero_on_insufficient_input_without_posting(caplog):
    """#178 follow-up: missing target-week inputs still fail the Job (every pod retry
    fails the same way) and never reach the gateway."""
    gateway = _FakeWaiverGateway()
    with patch("src.optimization.waiver_evaluator.get_db_connection"), \
         patch("src.optimization.waiver_evaluator.load_live_waiver_inputs",
               side_effect=we.InsufficientWaiverInputError("no free-agent rows")), \
         patch("src.trigger_client.requests.post", side_effect=gateway.post):
        assert we.main(WAIVER_ARGV) == 1
    assert gateway.posts == []


# ---------------------------------------------------------------------------
# Auto-published IR + add moves, behind AUTO_PUBLISH_IR_MOVES (off by default). The IR move
# goes to /trigger/lineup first; the add-only claim is chained to its tracking id so the
# gateway holds it until that IR move is verified on ESPN.
# ---------------------------------------------------------------------------

IR_POOLS = (
    {"RB": [{"player_id": "hurt", "vorp_score": 6.0, "espn_injury_status": "OUT"},
            {"player_id": "healthy", "vorp_score": 2.0, "espn_injury_status": "ACTIVE"}]},
    {"RB": {"player_id": "4046", "vorp_score": 9.0}},
)
IR_AND_ADD_TARGET = {
    "player_id": "e-4046", "drop_player_id": None, "name": "Some Back", "team": "KC",
    "ir_player_id": "e-7777", "ir_player_name": "Hurt Back",
    "decision_inputs": {
        "add_player_vorp_score": 9.0, "drop_player_vorp_score": None, "season": 2026, "week": 5,
        "ir_player_vorp_score": 6.0, "ir_player_internal_id": "hurt",
    },
}


@pytest.fixture
def ir_flag_on(monkeypatch):
    monkeypatch.setenv("AUTO_PUBLISH_IR_MOVES", "true")


@pytest.mark.parametrize("value,enabled", [
    (None, False), ("", False), ("0", False), ("false", False), ("no", False), ("off", False),
    ("1", True), ("true", True), ("TRUE", True), ("yes", True),
])
def test_ir_auto_publish_is_off_unless_explicitly_enabled(monkeypatch, value, enabled):
    if value is None:
        monkeypatch.delenv("AUTO_PUBLISH_IR_MOVES", raising=False)
    else:
        monkeypatch.setenv("AUTO_PUBLISH_IR_MOVES", value)
    assert we.ir_auto_publish_enabled() is enabled


@pytest.mark.parametrize("value", [None, "0", "false"])
def test_flag_off_is_an_exact_no_op(monkeypatch, caplog, value):
    """Same plan, same WARNING, no blocking-check query and nothing posted anywhere."""
    if value is None:
        monkeypatch.delenv("AUTO_PUBLISH_IR_MOVES", raising=False)
    else:
        monkeypatch.setenv("AUTO_PUBLISH_IR_MOVES", value)
    with patch("src.optimization.waiver_evaluator.get_db_connection"), \
         patch("src.optimization.waiver_evaluator.load_live_waiver_inputs", return_value=IR_POOLS), \
         patch("src.optimization.waiver_evaluator.load_open_ir_slots", return_value=1), \
         patch("src.optimization.waiver_evaluator.load_ineligible_ir_occupants") as mock_blocking, \
         patch("src.optimization.waiver_evaluator.resolve_live_waiver_moves", return_value=[]) as mock_resolve, \
         patch("src.trigger_client.requests.post") as mock_post, \
         caplog.at_level("INFO", logger="WaiverEvaluator"):
        assert we.run_weekly_waivers_live("7", 2026, 5) == []

    mock_post.assert_not_called()
    mock_blocking.assert_not_called()
    assert mock_resolve.call_args.args[1] == []
    [record] = [r for r in caplog.records if "suggest IR hurt + add 4046" in r.getMessage()]
    assert record.levelname == "WARNING"


def test_flag_on_plans_ir_and_add_moves_after_the_swaps(ir_flag_on, caplog):
    bench, agents = IR_POOLS
    agents = {**agents, "WR": {"player_id": "wr-fa", "vorp_score": 8.0}}
    bench = {**bench, "WR": [{"player_id": "wr-bench", "vorp_score": 1.0, "espn_injury_status": "ACTIVE"}]}
    with patch("src.optimization.waiver_evaluator.load_open_ir_slots", return_value=1), \
         patch("src.optimization.waiver_evaluator.load_ineligible_ir_occupants", return_value=[]), \
         caplog.at_level("INFO", logger="WaiverEvaluator"):
        moves = we.plan_live_waiver_moves(MagicMock(), "7", 2026, bench, agents, week=5)

    assert [m.get("kind") for m in moves] == [None, "ir_and_add"]
    assert moves[0]["drop_player_id"] == "wr-bench"
    assert moves[1]["ir_player_id"] == "hurt" and moves[1]["add_player_id"] == "4046"
    assert not [r for r in caplog.records if r.levelname == "WARNING"]


def test_flag_on_withholds_ir_and_adds_while_an_ineligible_player_sits_in_our_ir(ir_flag_on, caplog):
    """An active player left in IR may block ESPN transactions, and activating them is a
    manager's call, so the stash is withheld and the run says why at ERROR."""
    with patch("src.optimization.waiver_evaluator.load_open_ir_slots", return_value=1), \
         patch("src.optimization.waiver_evaluator.load_ineligible_ir_occupants",
               return_value=[("old-hurt", "Healed Guy", "ACTIVE")]), \
         caplog.at_level("INFO", logger="WaiverEvaluator"):
        moves = we.plan_live_waiver_moves(MagicMock(), "7", 2026, *IR_POOLS, week=5)

    assert all(m.get("kind") != "ir_and_add" for m in moves)
    [record] = [r for r in caplog.records if r.levelname == "ERROR"]
    assert "Healed Guy" in record.getMessage() and "activate" in record.getMessage()


def test_resolve_live_waiver_moves_carries_the_ir_players_espn_identity():
    cursor = MagicMock()
    # add player lookup, then the IR player's (no drop on an ir_and_add).
    cursor.fetchone.side_effect = [("e-4046", "Some Back", "KC"), ("e-7777", "Hurt Back", "KC")]
    moves = [{
        "kind": "ir_and_add", "position": "RB", "ir_player_id": "hurt", "ir_position": "RB",
        "drop_player_id": None, "add_player_id": "4046", "add_vorp_score": 9.0, "ir_vorp_score": 6.0,
    }]

    assert we.resolve_live_waiver_moves(cursor, moves, season=2026, week=5) == [IR_AND_ADD_TARGET]


def test_resolve_live_waiver_moves_skips_an_ir_and_add_whose_ir_player_has_no_espn_id():
    cursor = MagicMock()
    cursor.fetchone.side_effect = [("e-4046", "Some Back", "KC"), (None, "Hurt Back", "KC")]
    moves = [{"kind": "ir_and_add", "ir_player_id": "hurt", "drop_player_id": None, "add_player_id": "4046"}]

    assert we.resolve_live_waiver_moves(cursor, moves, season=2026, week=5) == []


def test_chained_add_key_is_stable_and_distinct_from_a_plain_add_only_claim():
    plain = {k: v for k, v in IR_AND_ADD_TARGET.items() if not k.startswith("ir_")}
    key = we.waiver_idempotency_key("7", IR_AND_ADD_TARGET)

    assert key == we.waiver_idempotency_key("7", dict(IR_AND_ADD_TARGET))
    assert key != we.waiver_idempotency_key("7", plain)
    assert key != we.waiver_idempotency_key("7", {**IR_AND_ADD_TARGET, "ir_player_id": "e-1"})


class _FakeGateway:
    """/trigger/lineup and /trigger/waiver that dedupe on idempotency_key and answer with a
    stable tracking id per key, the way _accept_durable does. `failures` maps a path to the
    status codes to serve first; `lineup_body` overrides the lineup response."""

    def __init__(self, failures=None, lineup_body=None):
        self.failures = {k: list(v) for k, v in (failures or {}).items()}
        self.lineup_body = lineup_body
        self.tracking = {}
        self.posts = []

    def post(self, url, json, headers, timeout):
        path = url.rsplit("/", 1)[-1]
        self.posts.append((path, json))
        pending = self.failures.get(path)
        if pending:
            return _response(pending.pop(0), {"detail": "failed"})
        if path == "lineup" and self.lineup_body is not None:
            return _response(202, self.lineup_body)
        key = json["idempotency_key"]
        status = "duplicate" if key in self.tracking else "accepted"
        self.tracking.setdefault(key, f"{path}-track-{len(self.tracking) + 1}")
        return _response(202, {"status": status, "tracking_id": self.tracking[key]})


def _publish_chained(gateway, target=IR_AND_ADD_TARGET):
    with patch("src.trigger_client.time.sleep"), \
         patch("src.trigger_client.requests.post", side_effect=gateway.post):
        return we.publish_waiver_claim("7", dict(target))


def test_ir_and_add_posts_the_ir_move_then_an_add_chained_to_it(ir_flag_on):
    gateway = _FakeGateway()

    assert _publish_chained(gateway) is True

    (ir_path, ir_body), (add_path, add_body) = gateway.posts
    assert ir_path == "lineup"
    assert ir_body["team_id"] == "7"
    [entry] = ir_body["optimized_swaps"]
    assert {k: entry[k] for k in ("kind", "player_id", "player_name", "slot")} == {
        "kind": "ir", "player_id": "e-7777", "player_name": "Hurt Back", "slot": "IR",
    }
    assert entry["decision_inputs"]["season"] == 2026 and entry["decision_inputs"]["week"] == 5
    assert add_path == "waiver"
    assert add_body["after_tracking_id"] == "lineup-track-1"
    assert add_body["drop_player_id"] is None
    assert add_body["player_id"] == "e-4046"
    assert not any(k.startswith("ir_") for k in add_body)
    assert add_body["idempotency_key"] == we.waiver_idempotency_key("7", IR_AND_ADD_TARGET)
    assert ir_body["idempotency_key"] != add_body["idempotency_key"]


def test_ir_and_add_replay_dedupes_both_steps(ir_flag_on):
    """A pod retry or rumor replay gets the original IR tracking id back and re-chains to
    it; the held add comes back duplicate rather than being queued again."""
    gateway = _FakeGateway()
    assert _publish_chained(gateway) is True
    assert _publish_chained(gateway) is False

    adds = [body for path, body in gateway.posts if path == "waiver"]
    assert [a["after_tracking_id"] for a in adds] == ["lineup-track-1", "lineup-track-1"]
    assert len(gateway.tracking) == 2


def test_a_rejected_ir_move_never_sends_the_add(ir_flag_on):
    gateway = _FakeGateway(failures={"lineup": [422]})

    with pytest.raises(requests.HTTPError):
        _publish_chained(gateway)

    assert [path for path, _ in gateway.posts] == ["lineup"]


def test_an_ir_move_that_exhausts_retries_never_sends_the_add(ir_flag_on):
    gateway = _FakeGateway(failures={"lineup": [503] * 6})

    with pytest.raises(requests.HTTPError):
        _publish_chained(gateway)

    assert {path for path, _ in gateway.posts} == {"lineup"}


def test_an_ir_response_without_a_tracking_id_never_sends_the_add(ir_flag_on):
    gateway = _FakeGateway(lineup_body={"status": "accepted"})

    with pytest.raises(we.IrMoveNotChainableError):
        _publish_chained(gateway)

    assert [path for path, _ in gateway.posts] == ["lineup"]


def test_ir_and_add_refuses_with_the_flag_off(monkeypatch):
    """A rumor checkpoint prepared while the flag was on must not publish after it's off."""
    monkeypatch.delenv("AUTO_PUBLISH_IR_MOVES", raising=False)
    gateway = _FakeGateway()

    with pytest.raises(we.IrAutoPublishDisabledError):
        _publish_chained(gateway)

    assert gateway.posts == []


def test_ir_and_add_refuses_without_season_and_week(ir_flag_on):
    gateway = _FakeGateway()
    target = {**IR_AND_ADD_TARGET, "decision_inputs": {"add_player_vorp_score": 9.0}}

    with pytest.raises(we.IrMoveNotChainableError):
        _publish_chained(gateway, target)

    assert gateway.posts == []


def test_main_with_the_flag_on_publishes_the_ir_move_before_its_add(ir_flag_on):
    """End to end through the real planner, identity resolution and trigger client."""
    identities = {"4046": ("e-4046", "Some Back", "KC"), "hurt": ("e-7777", "Hurt Back", "KC")}

    def identity(cursor, player_id):
        espn_id, name, team = identities[player_id]
        return {"espn_id": espn_id, "name": name, "team": team}

    gateway = _FakeGateway()
    with patch("src.optimization.waiver_evaluator.get_db_connection"), \
         patch("src.optimization.waiver_evaluator.sync_league.refresh_rosters_if_stale"), \
         patch("src.optimization.waiver_evaluator.load_live_waiver_inputs", return_value=IR_POOLS), \
         patch("src.optimization.waiver_evaluator.load_open_ir_slots", return_value=1), \
         patch("src.optimization.waiver_evaluator.load_ineligible_ir_occupants", return_value=[]), \
         patch("src.optimization.waiver_evaluator.resolve_espn_identity", side_effect=identity), \
         patch("src.trigger_client.requests.post", side_effect=gateway.post):
        assert we.main(WAIVER_ARGV) == 0

    assert [path for path, _ in gateway.posts] == ["lineup", "waiver"]
    assert gateway.posts[1][1]["after_tracking_id"] == "lineup-track-1"


def test_a_failed_ir_and_add_does_not_hold_back_the_weeks_swaps(ir_flag_on):
    """Swaps publish first, so an IR move the gateway refuses fails the Job (and alerts)
    only after the ordinary claims are queued; a pod retry dedupes them."""
    gateway = _FakeGateway(failures={"lineup": [422]})
    with patch("src.optimization.waiver_evaluator.get_db_connection"), \
         patch("src.optimization.waiver_evaluator.sync_league.refresh_rosters_if_stale"), \
         patch("src.optimization.waiver_evaluator.load_live_waiver_inputs", return_value=IR_POOLS), \
         patch("src.optimization.waiver_evaluator.plan_live_waiver_moves", return_value=[{}, {}]), \
         patch("src.optimization.waiver_evaluator.resolve_live_waiver_moves",
               return_value=[dict(CLAIM_1), dict(IR_AND_ADD_TARGET)]), \
         patch("src.trigger_client.time.sleep"), \
         patch("src.trigger_client.requests.post", side_effect=gateway.post), \
         pytest.raises(requests.HTTPError):
        we.main(WAIVER_ARGV)

    assert [path for path, _ in gateway.posts] == ["waiver", "lineup"]


@pytest.fixture(autouse=True)
def full_roster_by_default(monkeypatch):
    monkeypatch.setattr(we.waiver_strategy, "load_rostered_positions", lambda *args: {"K", "D/ST"})
    monkeypatch.setattr(we.waiver_strategy, "load_open_roster_slots", lambda *args: 0)



def test_weekly_waivers_publishes_add_only_claims_first_without_ir_flag(monkeypatch):
    monkeypatch.delenv("AUTO_PUBLISH_IR_MOVES", raising=False)
    monkeypatch.setattr(we.waiver_strategy, "load_open_roster_slots", lambda *args: 2)
    monkeypatch.setattr(we.waiver_strategy, "load_add_candidates", lambda *args: [
        {"player_id": "fa1", "position": "WR", "vorp_score": 8.0},
        {"player_id": "fa2", "position": "WR", "vorp_score": 7.0},
        {"player_id": "fa3", "position": "WR", "vorp_score": 6.0}])
    pools = ({"WR": [{"player_id": "bench", "vorp_score": 1.0}]},
             {"WR": {"player_id": "fa1", "vorp_score": 8.0}})
    def identity(cursor, pid):
        return {"espn_id": "espn-" + pid, "name": pid, "team": "SEA"}
    with patch.object(we, "get_db_connection"), \
         patch.object(we, "load_live_waiver_inputs", return_value=pools), \
         patch.object(we, "resolve_espn_identity", side_effect=identity), \
         patch.object(we, "post_trigger_idempotent") as post:
        resolved = we.run_weekly_waivers_live("8", 2026, 5)
    assert [t["player_id"] for t in resolved] == ["espn-fa1", "espn-fa2", "espn-fa3"]
    assert [t["drop_player_id"] for t in resolved] == [None, None, "espn-bench"]
    assert post.call_count == 3
    assert all(c.args[0] == "waiver" for c in post.call_args_list)
    assert all("after_tracking_id" not in c.args[1] for c in post.call_args_list)
    assert resolved[0]["decision_inputs"]["drop_player_vorp_score"] is None


def test_live_input_allows_empty_bench_when_roster_has_capacity(monkeypatch):
    monkeypatch.setattr(we.waiver_strategy, "load_open_roster_slots", lambda *args: 1)
    fa = {"WR": {"player_id": "fa", "vorp_score": 5}}
    with patch.object(we, "_horizon_pools", return_value=({}, fa)):
        assert we.load_live_waiver_inputs(MagicMock(), "8", 2026, 5) == ({}, fa)


def test_weekly_waivers_publishes_negative_vorp_skill_add(monkeypatch):
    monkeypatch.setattr(we.waiver_strategy, "load_open_roster_slots", lambda *args: 1)
    monkeypatch.setattr(we.waiver_strategy, "load_add_candidates", lambda *args: [
        {"player_id": "k", "position": "K", "vorp_score": -0.1},
        {"player_id": "dst", "position": "D/ST", "vorp_score": -0.2},
        {"player_id": "hurt", "position": "WR", "vorp_score": -1, "injury_status": "IR"},
        {"player_id": "worthy", "position": "WR", "vorp_score": -4.78},
    ])
    with patch.object(we, "get_db_connection"), \
         patch.object(we, "load_live_waiver_inputs", return_value=({}, {"WR": {"player_id": "worthy", "vorp_score": -4.78}})), \
         patch.object(we, "resolve_espn_identity", return_value={"espn_id": "1", "name": "Xavier Worthy", "team": "KC"}), \
         patch.object(we, "post_trigger_idempotent") as post:
        resolved = we.run_weekly_waivers_live("8", 2026, 6)
    assert len(resolved) == 1
    assert resolved[0]["drop_player_id"] is None
    assert resolved[0]["decision_inputs"]["add_player_vorp_score"] == -4.78
    post.assert_called_once()
    assert post.call_args.args[1]["player_id"] == "1"
    assert post.call_args.args[1]["drop_player_id"] is None
