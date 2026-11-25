import json
from unittest.mock import MagicMock, patch

from tests.test_live_service import LIVE_EVENT, _db, game_seeded, seeded  # noqa: F401  (fixtures)


def test_live_players_returns_blocks_and_nulls_and_bumps_last_viewed(client, seeded):
    response = client.get("/v1/live/players?ids=bijan,ghost")

    assert response.status_code == 200
    players = response.json()["players"]
    assert players["bijan"]["status"] == "in_progress" and players["bijan"]["live_points"] == 12.4
    assert players["ghost"] is None
    assert seeded.data["live:last_viewed"].isdigit() and seeded.ttl["live:last_viewed"] == 600


def test_live_players_rejects_empty_and_oversized_id_lists(client, seeded):
    assert client.get("/v1/live/players?ids=,").status_code == 422
    assert client.get("/v1/live/players?ids=" + ",".join(f"p{i}" for i in range(101))).status_code == 422
    assert "live:last_viewed" not in seeded.data


def test_matchup_feed_route_returns_items_and_bumps_last_viewed(client, seeded):
    response = client.get("/v1/matchup/feed?season=2026&week=11&limit=2")

    body = response.json()
    assert response.status_code == 200
    assert [i["play_id"] for i in body["items"]] == ["12", "90"]
    assert body["next_before"] == body["items"][-1]["cursor"]
    assert seeded.data["live:last_viewed"].isdigit()


def test_matchup_feed_limit_is_bounded(client, seeded):
    assert client.get("/v1/matchup/feed?season=2026&week=11&limit=0").status_code == 422
    assert client.get("/v1/matchup/feed?season=2026&week=11&limit=201").status_code == 422


def test_matchup_route_adds_live_after_the_service(client, seeded, monkeypatch):
    from src.services import live_service

    monkeypatch.setattr(live_service, "_player_ids_by_espn_id", lambda ids: {"4429795": "bijan"})
    line = {
        "player_id": "4429795", "espn_id": "4429795", "name": "Bijan Robinson", "position": "RB", "slot": "RB",
        "points": 12.4, "projected_points": 20.0, "game_status": "in_progress", "headshot_url": "h",
        "deficit_flag": False,
    }
    side = {"team": {"name": "A", "logo_url": "l", "score": 1.0, "projected": 2.0}, "starters": [line], "bench": []}
    fake = {"season": 2026, "week": 11, "our_team": side, "opponent": {**side, "starters": []}}

    with patch("src.api.matchup.get_current_team_matchup", return_value=fake):
        response = client.get("/v1/matchup?season=2026&week=11")

    body = response.json()["our_team"]["starters"][0]
    assert body["live"]["live_points"] == 12.4 and body["live"]["status"] == body["game_status"]
    assert seeded.data["live:last_viewed"].isdigit()


def test_matchup_route_without_a_snapshot_has_null_live(client):
    line = {
        "player_id": "1", "espn_id": "1", "name": "N", "position": "RB", "slot": "RB", "points": 0.0,
        "projected_points": 1.0, "headshot_url": "h", "deficit_flag": False,
    }
    side = {"team": {"name": "A", "logo_url": "l", "score": 1.0, "projected": 2.0}, "starters": [line], "bench": []}
    fake = {"season": 2026, "week": 11, "our_team": side, "opponent": side}

    with patch("src.api.matchup.get_current_team_matchup", return_value=fake):
        response = client.get("/v1/matchup?season=2026&week=11")

    assert response.status_code == 200
    assert response.json()["our_team"]["starters"][0]["live"] is None


def test_matchup_game_route_returns_the_split_and_bumps_last_viewed(client, game_seeded):
    response = client.get(f"/v1/matchup/games/{LIVE_EVENT}?season=2026&week=11")

    assert response.status_code == 200
    body = response.json()
    assert (body["mine_points"], body["theirs_points"]) == (12.4, 9.0)
    assert body["game"]["home"] == {"abbr": "ATL", "score": 17}
    assert body["players"][0]["breakdown"][0]["rule_label"] == "1 pt per 10 rushing yds"
    assert body["plays"][0]["type"] == "play"
    assert game_seeded.data["live:last_viewed"].isdigit()


def test_matchup_game_route_404s_for_an_unknown_game(client, game_seeded):
    assert client.get("/v1/matchup/games/nope?season=2026&week=11").status_code == 404


def test_matchup_game_route_503s_without_redis(client):
    assert client.get(f"/v1/matchup/games/{LIVE_EVENT}?season=2026&week=11").status_code == 503


def _settings_cursor(row):
    cursor = MagicMock()
    cursor.fetchone.return_value = row
    context = MagicMock()
    context.__enter__.return_value = cursor
    return context, cursor


def test_scoring_rules_route_explains_the_league_rules(client):
    settings = {"scoringItems": [{"statId": 24, "points": 0.1}, {"statId": 99, "points": 0, "pointsOverrides": {"16": 1.0}}]}
    context, cursor = _settings_cursor((2026, json.dumps(settings)))

    with patch("src.services.scoring_service.get_db_cursor", return_value=context):
        response = client.get("/v1/scoring/rules")

    assert response.status_code == 200
    assert response.json() == {"season": 2026, "rules": [
        {"stat_key": "rushYd", "label": "Rushing yards", "category": "Rushing", "points_per": 0.1, "unit": "yds",
         "rule_label": "1 pt per 10 rushing yds", "position": None},
        {"stat_key": "sacks", "label": "Sacks", "category": "Defense", "points_per": 1.0, "unit": "sacks",
         "rule_label": "1 pt per sack", "position": "D/ST"},
    ]}
    assert cursor.execute.call_args.args[1] == (None, None)


def test_scoring_rules_route_404s_without_synced_settings(client):
    context, cursor = _settings_cursor(None)

    with patch("src.services.scoring_service.get_db_cursor", return_value=context):
        response = client.get("/v1/scoring/rules?season=2025")

    assert response.status_code == 404
    assert cursor.execute.call_args.args[1] == (2025, 2025)
