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
         patch("src.optimization.waiver_evaluator.waiver_strategy.load_bench_by_position") as mock_bench, \
         patch("src.optimization.waiver_evaluator.waiver_strategy.load_best_free_agent_by_position") as mock_fa, \
         patch("src.optimization.waiver_evaluator.waiver_strategy.propose_waiver_moves") as mock_propose, \
         patch("src.optimization.waiver_evaluator.resolve_live_waiver_moves", return_value=[resolved_target]) as mock_resolve, \
         patch("src.optimization.waiver_evaluator.publish_waiver_claim") as mock_publish:
        mock_conn = MagicMock()
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        mock_bench.return_value = {"RB": [{"player_id": "bench1", "vorp_score": 1.0}]}
        mock_fa.return_value = {"RB": {"player_id": "4046", "vorp_score": 5.0}}
        mock_propose.return_value = [{"position": "RB", "drop_player_id": "bench1", "add_player_id": "4046"}]

        result = we.run_weekly_waivers_live("7", 2023, 5)

    mock_propose.assert_called_once_with(mock_bench.return_value, mock_fa.return_value)
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
    with patch("src.optimization.waiver_evaluator.waiver_strategy.load_bench_by_position",
               return_value={"RB": [{"player_id": "bench1", "vorp_score": 1.0}]}), \
         patch("src.optimization.waiver_evaluator.waiver_strategy.load_best_free_agent_by_position", return_value={}), \
         pytest.raises(we.InsufficientWaiverInputError) as exc:
        we.load_live_waiver_inputs(MagicMock(), "7", 2026, 5)
    assert "free-agent" in str(exc.value)
    assert "Season 2026 Week 5" in str(exc.value)


def test_load_live_waiver_inputs_raises_when_bench_has_no_target_week_rows():
    with patch("src.optimization.waiver_evaluator.waiver_strategy.load_bench_by_position", return_value={}), \
         patch("src.optimization.waiver_evaluator.waiver_strategy.load_best_free_agent_by_position",
               return_value={"RB": {"player_id": "4046", "vorp_score": 5.0}}), \
         pytest.raises(we.InsufficientWaiverInputError) as exc:
        we.load_live_waiver_inputs(MagicMock(), "7", 2026, 5)
    assert "bench" in str(exc.value)


def test_load_live_waiver_inputs_returns_both_pools():
    bench = {"RB": [{"player_id": "bench1", "vorp_score": 1.0}]}
    fa = {"RB": {"player_id": "4046", "vorp_score": 5.0}}
    with patch("src.optimization.waiver_evaluator.waiver_strategy.load_bench_by_position", return_value=bench), \
         patch("src.optimization.waiver_evaluator.waiver_strategy.load_best_free_agent_by_position", return_value=fa):
        assert we.load_live_waiver_inputs(MagicMock(), "7", 2026, 5) == (bench, fa)


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


def test_run_weekly_waivers_live_withholds_ir_eligible_drops_while_ir_is_open(caplog):
    """Auto-publish of IR moves stays off, so a claim whose drop should go to IR is
    withheld (and logged) rather than published as a drop or swapped for another drop."""
    pools = (
        {"RB": [{"player_id": "hurt", "vorp_score": -1.0, "espn_injury_status": "OUT"},
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
    [record] = [r for r in caplog.records if "IR" in r.getMessage() and "hurt" in r.getMessage()]
    assert "not published" in record.getMessage()


def test_withhold_ir_moves_skips_the_ir_slot_lookup_when_no_drop_is_eligible():
    moves = [{"position": "RB", "drop_player_id": "b", "add_player_id": "f", "add_vorp_score": 5.0, "drop_vorp_score": 1.0}]
    bench = {"RB": [{"player_id": "b", "vorp_score": 1.0, "espn_injury_status": "QUESTIONABLE"}]}
    with patch("src.optimization.waiver_evaluator.load_open_ir_slots") as mock_slots:
        assert we.withhold_ir_moves(MagicMock(), "7", 2026, moves, bench) == moves
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
