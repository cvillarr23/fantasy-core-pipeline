from contextlib import contextmanager
from unittest.mock import MagicMock, patch


@contextmanager
def _fake_db_cursor(cursor=None):
    yield cursor or MagicMock()


def _mock_waiver_strategy_calls(monkeypatch, moves):
    from src.services import waiver_service

    monkeypatch.setattr(
        waiver_service.waiver_strategy, "load_bench_by_position", lambda cur, team_id, season, week: {"RB": [{"player_id": "rb_bench", "vorp_score": 2.0}]}
    )
    monkeypatch.setattr(
        waiver_service.waiver_strategy, "load_best_free_agent_by_position", lambda cur, season, week: {"RB": {"player_id": "rb_fa", "vorp_score": 8.0}}
    )
    monkeypatch.setattr(
        waiver_service.waiver_strategy, "propose_waiver_moves", lambda bench, free_agents, margin: moves
    )


def test_get_waiver_recommendations_success(client, monkeypatch):
    _mock_waiver_strategy_calls(
        monkeypatch,
        [
            {
                "position": "RB",
                "add_player_id": "rb_fa",
                "drop_player_id": "rb_bench",
                "add_vorp_score": 8.0,
                "drop_vorp_score": 2.0,
            }
        ],
    )
    cursor = MagicMock()
    cursor.fetchall.return_value = [
        ("rb_fa", "101", "FA RB", "SEA", None),
        ("rb_bench", "102", "Bench RB", "NYJ", "Out"),
    ]

    with patch("src.services.waiver_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        response = client.get("/v1/waiver/recommendations?season=2026&week=3")

    assert response.status_code == 200
    body = response.json()
    assert body["season"] == 2026
    assert body["week"] == 3
    assert len(body["recommendations"]) == 1
    rec = body["recommendations"][0]
    assert rec["position"] == "RB"
    assert rec["vorp_delta"] == 6.0
    assert rec["free_agent"]["name"] == "FA RB"
    assert rec["bench_player"]["injury_status"] == "Out"
    assert rec["rumor_sentiment"] is None
    assert body["source_mode"] == "target_week"
    assert body["source_weeks"] == [3]


def test_get_waiver_recommendations_db_failure_returns_500(client):
    @contextmanager
    def _broken_cursor():
        raise Exception("connection refused")
        yield  # pragma: no cover

    with patch("src.services.waiver_service.get_db_cursor", _broken_cursor):
        response = client.get("/v1/waiver/recommendations?season=2026&week=3")

    assert response.status_code == 500


def test_get_waiver_recommendations_requires_season_and_week(client):
    response = client.get("/v1/waiver/recommendations")

    assert response.status_code == 422


def test_get_waiver_recommendations_returns_503_when_inputs_unavailable(client):
    from src.services import waiver_service

    error = waiver_service.WaiverInputsUnavailableError("no usable weekly_stats.vorp_score for Season 2026 Week 1")
    with patch("src.api.waiver.compute_waiver_recommendations", side_effect=error):
        response = client.get("/v1/waiver/recommendations?season=2026&week=1")

    assert response.status_code == 503
    assert "no usable weekly_stats.vorp_score" in response.json()["detail"]



def test_get_waiver_recommendations_serializes_ir_and_add(client, monkeypatch):
    from src.services import waiver_service

    monkeypatch.setattr(
        waiver_service.waiver_strategy, "load_bench_by_position",
        lambda cur, team_id, season, week: {"RB": [{"player_id": "rb_hurt", "vorp_score": -1.0, "espn_injury_status": "INJURY_RESERVE"}]},
    )
    monkeypatch.setattr(
        waiver_service.waiver_strategy, "load_best_free_agent_by_position",
        lambda cur, season, week: {"RB": {"player_id": "rb_fa", "vorp_score": 8.0}},
    )
    cursor = MagicMock()
    cursor.fetchall.return_value = [
        ("rb_fa", "101", "FA RB", "SEA", None),
        ("rb_hurt", "102", "Hurt RB", "NYJ", "IR"),
    ]

    with patch("src.services.waiver_service.get_db_cursor", lambda: _fake_db_cursor(cursor)), \
         patch("src.services.waiver_service.load_open_ir_slots", return_value=1):
        response = client.get("/v1/waiver/recommendations?season=2026&week=3")

    assert response.status_code == 200
    body = response.json()
    [rec] = body["recommendations"]
    assert rec["kind"] == "ir_and_add"
    assert rec["bench_player"] is None
    assert rec["ir_player"]["name"] == "Hurt RB"
    assert body["open_ir_slots"] == 1
    assert body["ir_moves"] == [{"action": "move_to_ir", "position": "RB", "player": {
        "espn_id": "102", "name": "Hurt RB", "pro_team": "NYJ", "injury_status": "IR", "net_vorp": -1.0,
    }}]
