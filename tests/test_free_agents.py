from contextlib import contextmanager
import os
from unittest.mock import MagicMock, patch

import psycopg2
import pytest

from src.services import free_agent_service as service


@contextmanager
def _cursor_context(cursor):
    yield cursor


def _browse(rows, weeks=(4,), **kwargs):
    cursor = MagicMock()
    cursor.fetchall.side_effect = [[(w,) for w in weeks], rows]
    with patch.object(
        service, "get_db_cursor", lambda: _cursor_context(cursor)
    ), patch.object(service, "refresh_rosters_if_stale") as refresh, patch.object(
        service, "load_teams_playing", return_value={"SEA", "NYJ"}
    ) as schedule:
        result = service.browse_free_agents(2026, kwargs.pop("week", 4), **kwargs)
    refresh.assert_called_once_with(2026)
    return result, cursor, schedule


ROWS = [
    ("bench1", "101", "Bench", "RB", "NYJ", "OUT", 5.0, 2.0, True),
    ("bench2", "102", "Better bench", "RB", "NYJ", None, 9.0, 6.0, True),
    ("fa1", "201", "Alpha Back", "RB", "SEA", None, 7.0, 4.0, False),
    ("fa2", "202", "Beta Back", "RB", "SEA", None, 10.0, 1.0, False),
    ("fa3", "203", "Wideout", "WR", "SEA", None, 8.0, 3.0, False),
    ("fa4", "204", "No metrics", "RB", "SEA", "Questionable", None, None, False),
]


def test_browse_full_pool_and_same_position_swaps():
    result, _, schedule = _browse(ROWS)
    assert result["source_mode"] == "target_week"
    assert result["source_weeks"] == [4]
    assert result["total"] == 4
    agents = {p["player_id"]: p for p in result["free_agents"]}
    assert agents["fa1"]["bench_player"]["player_id"] == "bench1"
    assert agents["fa1"]["vorp_delta"] == 2.0
    assert agents["fa2"]["vorp_delta"] == -1.0
    assert agents["fa3"]["bench_player"] is None
    assert agents["fa3"]["vorp_delta"] is None
    assert agents["fa4"]["vorp_score"] is None
    assert agents["fa4"]["vorp_delta"] is None
    schedule.assert_not_called()


def test_filters_sort_and_pagination_apply_in_order():
    result, _, _ = _browse(
        ROWS, position="rb", q="BACK", sort="projected", limit=1, offset=1
    )
    assert result["total"] == 2
    assert result["limit"] == 1
    assert result["offset"] == 1
    assert [p["player_id"] for p in result["free_agents"]] == ["fa1"]
    empty, _, _ = _browse(ROWS, q="does not exist")
    assert empty["total"] == 0
    assert empty["free_agents"] == []
    past_end, _, _ = _browse(ROWS, offset=100)
    assert past_end["total"] == 4
    assert past_end["free_agents"] == []


def test_ties_are_stable_and_missing_metrics_sort_last():
    rows = ROWS + [("fa0", "200", "Alpha Back", "RB", "SEA", None, 7.0, 4.0, False)]
    for sort in ("vorp", "projected"):
        result, _, _ = _browse(rows[::-1], sort=sort)
        ids = [p["player_id"] for p in result["free_agents"]]
        assert ids.index("fa0") < ids.index("fa1")
        assert ids[-1] == "fa4"


def test_fallback_uses_prior_window_and_keeps_bye_players_without_metrics(monkeypatch):
    monkeypatch.setenv("ESPN_TEAM_ID", "8")
    rows = ROWS + [("bye", "300", "Bye player", "RB", "BUF", None, 50.0, 40.0, False)]
    result, cursor, schedule = _browse(rows, weeks=(2, 3))
    assert result["source_mode"] == "rolling_prior_weeks"
    assert result["source_weeks"] == [2, 3]
    assert cursor.execute.call_args_list[0].args[1] == (2026, 1, 4, "8")
    assert cursor.execute.call_args_list[1].args[1] == (2026, [2, 3], False, "8")
    bye = next(p for p in result["free_agents"] if p["player_id"] == "bye")
    assert bye["vorp_score"] is None
    assert bye["projected_points"] is None
    schedule.assert_called_once_with(2026, 4)


@pytest.mark.parametrize(
    "weeks, week, message",
    [
        ((), 4, "no usable"),
        ((2,), 4, "stale"),
        ((), 1, "no usable"),
    ],
)
def test_unavailable_metrics_raise(weeks, week, message):
    with pytest.raises(service.WaiverInputsUnavailableError, match=message):
        _browse([], weeks=weeks, week=week)


def test_schedule_failure_is_unavailable():
    cursor = MagicMock()
    cursor.fetchall.return_value = [(3,)]
    with patch.object(
        service, "get_db_cursor", lambda: _cursor_context(cursor)
    ), patch.object(service, "refresh_rosters_if_stale"), patch.object(
        service, "load_teams_playing", side_effect=RuntimeError("schedule down")
    ):
        with pytest.raises(service.WaiverInputsUnavailableError, match="schedule"):
            service.browse_free_agents(2026, 4)


@pytest.mark.parametrize(
    "query",
    [
        "",
        "?season=2026",
        "?season=2026&week=0",
        "?season=2026&week=19",
        "?season=2026&week=4&sort=name",
        "?season=2026&week=4&limit=0",
        "?season=2026&week=4&limit=201",
        "?season=2026&week=4&offset=-1",
    ],
)
def test_route_validates_query(client, query):
    with patch("src.api.waiver.browse_free_agents") as browse:
        assert client.get("/v1/waiver/free-agents" + query).status_code == 422
    browse.assert_not_called()


def test_route_serializes_and_forwards_filters(client):
    payload, _, _ = _browse(ROWS)
    with patch("src.api.waiver.browse_free_agents", return_value=payload) as browse:
        response = client.get(
            "/v1/waiver/free-agents?season=2026&week=4&position=RB&q=back&sort=projected&limit=10&offset=2"
        )
    assert response.status_code == 200
    assert response.json() == payload
    browse.assert_called_once_with(2026, 4, "RB", "back", "projected", 10, 2)


@pytest.mark.parametrize(
    "error, status",
    [
        (service.WaiverInputsUnavailableError("stale metrics"), 503),
        (RuntimeError("database down"), 500),
    ],
)
def test_route_error_status(client, error, status):
    with patch("src.api.waiver.browse_free_agents", side_effect=error):
        response = client.get("/v1/waiver/free-agents?season=2026&week=4")
    assert response.status_code == status


@pytest.fixture
def postgres(monkeypatch):
    dsn = os.getenv("ACTIVITY_TEST_DSN")
    if not dsn:
        pytest.skip("Set ACTIVITY_TEST_DSN to a disposable local Postgres")
    assert psycopg2.extensions.parse_dsn(dsn).get("host") in {"127.0.0.1", "localhost"}
    conn = psycopg2.connect(dsn)
    with conn.cursor() as cursor:
        cursor.execute(
            """
            CREATE TEMP TABLE players (
                player_id TEXT PRIMARY KEY, espn_id TEXT, name TEXT, position TEXT,
                team TEXT, injury_status TEXT
            );
            CREATE TEMP TABLE league_rosters (
                player_id TEXT, team_id TEXT, is_starter BOOLEAN,
                slot_position TEXT, espn_injury_status TEXT
            );
            CREATE TEMP TABLE weekly_stats (
                player_id TEXT, season INT, week INT, vorp_score DOUBLE PRECISION,
                projected_points DOUBLE PRECISION, PRIMARY KEY (player_id, season, week)
            );
        """
        )

    @contextmanager
    def cursor_context():
        with conn.cursor() as cursor:
            yield cursor

    monkeypatch.setenv("ESPN_TEAM_ID", "8")
    monkeypatch.setattr(service, "get_db_cursor", cursor_context)
    monkeypatch.setattr(service, "refresh_rosters_if_stale", lambda season: False)
    monkeypatch.setattr(
        service, "load_teams_playing", lambda season, week: {"SEA", "NYJ"}
    )
    try:
        yield conn
    finally:
        conn.rollback()
        conn.close()


def test_postgres_full_pool_ownership_metrics_and_prior_means(postgres):
    with postgres.cursor() as cursor:
        cursor.executemany(
            "INSERT INTO players VALUES (%s,%s,%s,%s,%s,%s)",
            [
                ("fa1", "1", "First Back", "RB", "SEA", None),
                ("fa2", "2", "Second Back", "RB", "SEA", "Questionable"),
                ("unknown", "3", "Unprojected Back", "RB", "SEA", None),
                ("bye", "4", "Bye Back", "RB", "BUF", None),
                ("unmapped", None, "Unmapped Back", "RB", "SEA", None),
                ("blank", "", "Blank Back", "RB", "SEA", None),
                ("bench", "5", "Our Bench", "RB", "NYJ", None),
                ("starter", "6", "Our Starter", "RB", "NYJ", None),
                ("ir", "7", "Our IR", "RB", "NYJ", None),
                ("other", "8", "Other Bench", "RB", "NYJ", None),
            ],
        )
        cursor.executemany(
            "INSERT INTO league_rosters VALUES (%s,%s,%s,%s,%s)",
            [
                ("bench", "8", None, "BE", "OUT"),
                ("starter", "8", True, "RB", None),
                ("ir", "8", False, "IR", "INJURY_RESERVE"),
                ("other", "9", False, "BE", None),
            ],
        )
        cursor.executemany(
            "INSERT INTO weekly_stats VALUES (%s,%s,%s,%s,%s)",
            [
                ("fa1", 2026, 2, 4.0, 8.0),
                ("fa1", 2026, 3, 8.0, 12.0),
                ("fa2", 2026, 3, -1.0, 2.0),
                ("bye", 2026, 3, 40.0, 50.0),
                ("bench", 2026, 3, 2.0, 5.0),
                ("starter", 2026, 3, -50.0, 0.0),
                ("ir", 2026, 3, -40.0, 0.0),
                ("other", 2026, 3, -30.0, 0.0),
                ("fa1", 2025, 3, 100.0, 100.0),
            ],
        )
    result = service.browse_free_agents(2026, 4)
    assert result["source_weeks"] == [2, 3]
    assert result["total"] == 4
    agents = {p["player_id"]: p for p in result["free_agents"]}
    assert set(agents) == {"fa1", "fa2", "unknown", "bye"}
    assert agents["fa1"]["vorp_score"] == 6.0
    assert agents["fa1"]["projected_points"] == 10.0
    assert agents["fa1"]["vorp_delta"] == 4.0
    assert agents["fa1"]["bench_player"]["player_id"] == "bench"
    assert agents["fa1"]["bench_player"]["injury_status"] == "OUT"
    assert agents["fa2"]["vorp_delta"] == -3.0
    assert agents["bye"]["vorp_score"] is None
    assert agents["unknown"]["vorp_score"] is None
    with postgres.cursor() as cursor:
        cursor.execute("INSERT INTO weekly_stats VALUES ('fa1',2026,4,9.0,15.0)")
        cursor.execute("INSERT INTO weekly_stats VALUES ('fa2',2026,4,NULL,12.0)")
    result = service.browse_free_agents(2026, 4, limit=1)
    assert result["source_mode"] == "target_week"
    assert result["source_weeks"] == [4]
    assert result["total"] == 4
    assert result["free_agents"][0]["vorp_score"] == 9.0
    assert result["free_agents"][0]["bench_player"] is None
    result = service.browse_free_agents(2026, 4, q="Second")
    assert result["free_agents"][0]["projected_points"] == 12.0
    assert result["free_agents"][0]["vorp_score"] is None


def test_postgres_empty_bench_does_not_prevent_browsing(postgres):
    with postgres.cursor() as cursor:
        cursor.execute(
            "INSERT INTO players VALUES ('fa','1','Free Agent','WR','SEA',NULL)"
        )
        cursor.execute("INSERT INTO weekly_stats VALUES ('fa',2026,3,4.0,8.0)")
    result = service.browse_free_agents(2026, 4)
    assert result["total"] == 1
    assert result["free_agents"][0]["bench_player"] is None
    with postgres.cursor() as cursor:
        cursor.execute("INSERT INTO league_rosters VALUES ('fa','9',FALSE,'BE',NULL)")
        cursor.execute(
            "INSERT INTO players VALUES ('bench','2','Bench','RB','NYJ',NULL)"
        )
        cursor.execute(
            "INSERT INTO league_rosters VALUES ('bench','8',FALSE,'BE',NULL)"
        )
        cursor.execute("INSERT INTO weekly_stats VALUES ('bench',2026,4,1.0,4.0)")
    result = service.browse_free_agents(2026, 4)
    assert result["total"] == 0
    assert result["free_agents"] == []


def test_limit_none_returns_the_whole_sorted_pool():
    result, _, _ = _browse(ROWS, limit=None)
    assert [p["player_id"] for p in result["free_agents"]] == ["fa1", "fa3", "fa2", "fa4"]
