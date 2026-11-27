import copy
import json
from contextlib import contextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.ingestion import sync_injuries as sync

# Trimmed from ESPN's live feed on 2026-11-27.
FEED = json.loads((Path(__file__).parent / "fixtures/espn_injuries/injuries_2026-11-27.json").read_text())


def feed():
    return copy.deepcopy(FEED)


def entries(data):
    return [e for team in data["injuries"] for e in team["injuries"]]


def test_parses_one_row_per_injured_athlete_by_espn_id():
    rows, counts = sync.parse_injury_feed(feed())
    by_id = {row["espn_id"]: row for row in rows}
    assert set(by_id) == {"4430841", "4870808", "3043078", "4683153", "4678008"}
    assert counts == {"entries": 6, "injured_entries": 5, "unparsed_ids": 0}
    love = by_id["4870808"]
    assert love["status"] == "Injured Reserve"
    assert love["fantasy_status"] == "IR"
    assert love["nfl_team"] == "ARI"
    assert love["injury_type"] == "Ankle"
    assert love["return_date"] == date(2026, 12, 6)
    assert love["reported_at"] == datetime(2026, 11, 25, 19, 50, tzinfo=timezone.utc)
    # The Active entry is news about a healthy player, not an injury.
    assert "4428512" not in by_id


def test_keeps_the_newest_report_per_athlete():
    data = feed()
    older = copy.deepcopy(next(e for e in entries(data) if e["athlete"]["displayName"] == "Skyler Bell"))
    older["date"] = "2026-11-20T10:00Z"
    older["status"] = "Questionable"
    data["injuries"][0]["injuries"].append(older)
    rows, _ = sync.parse_injury_feed(data)
    assert next(r for r in rows if r["espn_id"] == "4683153")["status"] == "Out"


def test_entry_without_an_athlete_link_is_counted_and_skipped():
    data = feed()
    next(e for e in entries(data) if e["status"] == "Out")["athlete"]["links"] = []
    rows, counts = sync.parse_injury_feed(data)
    assert counts["unparsed_ids"] == 1
    assert "4683153" not in {r["espn_id"] for r in rows}


def test_unparsable_return_date_is_unknown_not_an_error():
    data = feed()
    next(e for e in entries(data) if e["status"] == "Out")["details"]["returnDate"] = "soon"
    rows, _ = sync.parse_injury_feed(data)
    assert next(r for r in rows if r["espn_id"] == "4683153")["return_date"] is None


@contextmanager
def fake_connection(conn):
    yield conn


def run_sync(data, today=date(2026, 11, 27)):
    conn = MagicMock()
    cursor = conn.cursor.return_value.__enter__.return_value
    with patch.object(sync, "get_db_connection", lambda: fake_connection(conn)), \
            patch.object(sync, "execute_values") as execute_values:
        result = sync.sync_injuries(fetch=lambda: data, today=today)
    return result, cursor, execute_values


def test_sync_replaces_the_whole_snapshot():
    result, cursor, execute_values = run_sync(feed())
    assert result["written"] == 5 and result["with_return_date"] == 5
    cursor.execute.assert_called_once_with("DELETE FROM player_injuries;")
    inserted = execute_values.call_args.args[2]
    assert sorted(row[0] for row in inserted) == ["3043078", "4430841", "4678008", "4683153", "4870808"]


def healthy_only(data):
    data["injuries"] = [{"injuries": [e for e in entries(data) if e["status"] == "Active"]}]
    return data


def test_feed_with_only_healthy_players_clears_the_table_in_the_offseason():
    result, cursor, execute_values = run_sync(healthy_only(feed()), today=date(2026, 6, 1))
    assert result["written"] == 0
    cursor.execute.assert_called_once_with("DELETE FROM player_injuries;")
    execute_values.assert_not_called()


@pytest.mark.parametrize("mutate", [
    healthy_only,
    # A dropped `status` field reads as Active for every entry.
    lambda data: [e.pop("status", None) for e in entries(data)],
])
def test_feed_with_no_injured_player_in_season_keeps_the_existing_snapshot(mutate):
    data = feed()
    mutate(data)
    with patch.object(sync, "get_db_connection") as connection:
        with pytest.raises(RuntimeError, match="no injured player in season"):
            sync.sync_injuries(fetch=lambda: data, today=date(2026, 11, 27))
    connection.assert_not_called()


@pytest.mark.parametrize("mutate", [
    lambda data: data.update(injuries=[]),
    lambda data: [e["athlete"].update(links=[]) for e in entries(data)],
])
def test_empty_or_unparsable_feed_keeps_the_existing_snapshot(mutate):
    data = feed()
    mutate(data)
    with patch.object(sync, "get_db_connection") as connection:
        with pytest.raises(RuntimeError, match="keeping the existing snapshot"):
            sync.sync_injuries(fetch=lambda: data)
    connection.assert_not_called()


def test_fetch_retries_then_raises():
    calls = []

    def get(url, timeout):
        calls.append(url)
        raise ConnectionError("down")

    with pytest.raises(ConnectionError):
        sync.fetch_injury_feed(get=get, attempts=2)
    assert calls == [sync.INJURIES_URL] * 2
