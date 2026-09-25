from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from src.ingestion import sync_players


RAW_SLEEPER_PAYLOAD = {
    "4046": {
        "player_id": "4046", "first_name": "Christian", "last_name": "McCaffrey",
        "position": "RB", "team": "SF", "status": "Active", "injury_status": "Questionable",
        "gsis_id": "00-0033280",
    },
    "9999": {
        "player_id": "9999", "first_name": "Some", "last_name": "Kicker",
        "position": "K", "team": "SF", "status": "Active", "injury_status": None,
        "gsis_id": "00-0000000",
    },
    "HOU": {
        "player_id": "HOU", "first_name": "Houston", "last_name": "Texans",
        "position": "DEF", "team": "HOU", "status": None, "injury_status": None,
        "gsis_id": None,
    },
    "1111": {
        "player_id": "1111", "first_name": "Free", "last_name": "Agent",
        "position": "WR", "team": None, "status": "Active", "injury_status": None,
        "gsis_id": None,
    },
    "2222": {
        "player_id": "2222", "first_name": "Not", "last_name": "Rosterable",
        "position": "LS", "team": "SF", "status": "Active", "injury_status": None,
        "gsis_id": None,
    },
}


def test_filter_skill_position_players_drops_non_rosterable_and_teamless():
    df = sync_players.filter_skill_position_players(RAW_SLEEPER_PAYLOAD)

    assert df.height == 3
    by_id = {row["player_id"]: row for row in df.iter_rows(named=True)}
    assert set(by_id) == {"4046", "9999", "HOU"}

    rb_row = by_id["4046"]
    assert rb_row["name"] == "Christian McCaffrey"
    assert rb_row["injury_status"] == "Questionable"
    assert rb_row["gsis_id"] == "00-0033280"

    k_row = by_id["9999"]
    assert k_row["position"] == "K"
    assert k_row["injury_status"] == "Healthy"

    def_row = by_id["HOU"]
    assert def_row["position"] == "D/ST"
    assert def_row["name"] == "Houston Texans"
    assert def_row["status"] == "Unknown"


def test_assert_position_coverage_raises_when_a_position_is_entirely_missing():
    df = sync_players.filter_skill_position_players(RAW_SLEEPER_PAYLOAD)

    with pytest.raises(RuntimeError, match="QB"):
        sync_players.assert_position_coverage(df)


def test_assert_position_coverage_passes_when_every_position_present():
    full_payload = dict(RAW_SLEEPER_PAYLOAD)
    full_payload["4047"] = {
        "player_id": "4047", "first_name": "Some", "last_name": "Quarterback",
        "position": "QB", "team": "SF", "status": "Active", "injury_status": None,
        "gsis_id": None,
    }
    full_payload["4048"] = {
        "player_id": "4048", "first_name": "Some", "last_name": "TightEnd",
        "position": "TE", "team": "SF", "status": "Active", "injury_status": None,
        "gsis_id": None,
    }
    full_payload["4049"] = {
        "player_id": "4049", "first_name": "Some", "last_name": "Receiver",
        "position": "WR", "team": "SF", "status": "Active", "injury_status": None,
        "gsis_id": None,
    }
    df = sync_players.filter_skill_position_players(full_payload)

    sync_players.assert_position_coverage(df)  # does not raise


def _mccaffrey_only(df: pl.DataFrame) -> pl.DataFrame:
    """Scopes the shared fixture's filtered output down to just player 4046, so these
    diff_players tests aren't tripped up by the K/D-ST rows RAW_SLEEPER_PAYLOAD also
    carries (added to exercise filter_skill_position_players's position coverage)."""
    return df.filter(pl.col("player_id") == "4046")


def test_diff_players_flags_injury_status_change_as_transition():
    df = _mccaffrey_only(sync_players.filter_skill_position_players(RAW_SLEEPER_PAYLOAD))
    existing = {"4046": {"status": "Active", "injury_status": "Healthy"}}

    changed = sync_players.diff_players(df, existing)

    assert len(changed) == 1
    assert changed[0]["player_id"] == "4046"
    assert changed[0]["is_transition"] is True


def test_diff_players_status_only_change_is_not_a_transition():
    df = _mccaffrey_only(sync_players.filter_skill_position_players(RAW_SLEEPER_PAYLOAD))
    # injury_status matches incoming ("Questionable"); only `status` differs.
    existing = {"4046": {"status": "Inactive", "injury_status": "Questionable"}}

    changed = sync_players.diff_players(df, existing)

    assert len(changed) == 1
    assert changed[0]["player_id"] == "4046"
    assert changed[0]["is_transition"] is False


def test_diff_players_ignores_unchanged_rows():
    df = _mccaffrey_only(sync_players.filter_skill_position_players(RAW_SLEEPER_PAYLOAD))
    existing = {"4046": {"status": "Active", "injury_status": "Questionable"}}

    changed = sync_players.diff_players(df, existing)

    assert changed == []


def test_diff_players_new_player_is_not_a_transition():
    df = _mccaffrey_only(sync_players.filter_skill_position_players(RAW_SLEEPER_PAYLOAD))

    changed = sync_players.diff_players(df, existing_by_id={})

    assert len(changed) == 1
    assert changed[0]["is_transition"] is False


def _row(player_id="4046", name="Christian McCaffrey", team="SF", injury_status="Questionable", is_transition=True):
    return {
        "player_id": player_id, "name": name, "position": "RB", "team": team,
        "status": "Active", "injury_status": injury_status, "gsis_id": "00-0033280",
        "is_transition": is_transition,
    }


def test_publish_waiver_alerts_fires_for_rostered_transition():
    mock_channel = MagicMock()
    mock_connection = MagicMock()
    mock_connection.channel.return_value = mock_channel

    with patch("src.ingestion.sync_players.pika.BlockingConnection", return_value=mock_connection):
        sync_players.publish_waiver_alerts([_row()], rostered_ids={"4046"})

    mock_channel.queue_declare.assert_called_once_with(queue="waiver_actions", durable=True)
    publish_kwargs = mock_channel.basic_publish.call_args.kwargs
    assert publish_kwargs["routing_key"] == "waiver_actions"
    assert publish_kwargs["properties"].delivery_mode == 2
    assert "4046" in publish_kwargs["body"]
    mock_connection.close.assert_called_once()


def test_publish_waiver_alerts_skips_non_rostered_player():
    with patch("src.ingestion.sync_players.pika.BlockingConnection") as mock_pika:
        sync_players.publish_waiver_alerts([_row(player_id="4046")], rostered_ids={"9999"})

    mock_pika.assert_not_called()


def test_publish_waiver_alerts_skips_new_player_insert():
    with patch("src.ingestion.sync_players.pika.BlockingConnection") as mock_pika:
        sync_players.publish_waiver_alerts([_row(is_transition=False)], rostered_ids={"4046"})

    mock_pika.assert_not_called()


def test_publish_waiver_alerts_skips_healthy_status():
    with patch("src.ingestion.sync_players.pika.BlockingConnection") as mock_pika:
        sync_players.publish_waiver_alerts([_row(injury_status="Healthy")], rostered_ids={"4046"})

    mock_pika.assert_not_called()


def test_publish_waiver_alerts_propagates_broker_failure_and_still_closes():
    mock_connection = MagicMock()
    mock_connection.channel.side_effect = Exception("broker unreachable")

    with patch("src.ingestion.sync_players.pika.BlockingConnection", return_value=mock_connection):
        with pytest.raises(Exception, match="broker unreachable"):
            sync_players.publish_waiver_alerts([_row()], rostered_ids={"4046"})

    mock_connection.close.assert_called_once()


def test_sync_players_skips_fetch_when_lock_already_held():
    mock_redis_instance = MagicMock()
    mock_redis_instance.set.return_value = None  # NX failed: lock already held

    with patch("src.ingestion.sync_players.get_redis_client", return_value=mock_redis_instance), \
         patch("src.ingestion.sync_players.fetch_sleeper_players") as mock_fetch:
        sync_players.sync_players(force=False)

    mock_fetch.assert_not_called()


def test_sync_players_forces_fetch_when_requested():
    mock_redis_instance = MagicMock()
    mock_redis_instance.set.return_value = True

    with patch("src.ingestion.sync_players.get_redis_client", return_value=mock_redis_instance), \
         patch("src.ingestion.sync_players.fetch_sleeper_players", return_value={}) as mock_fetch, \
         patch("src.ingestion.sync_players.assert_position_coverage"), \
         patch("src.ingestion.sync_players.get_db_connection") as mock_get_conn, \
         patch("src.ingestion.sync_players.publish_waiver_alerts") as mock_publish:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        sync_players.sync_players(force=True)

    mock_redis_instance.delete.assert_called_once_with(sync_players.SYNC_LOCK_KEY)
    mock_fetch.assert_called_once()
    mock_publish.assert_called_once()
    # First call acquires the in-progress lock, second marks the run as fully synced.
    assert mock_redis_instance.set.call_count == 2
    mark_synced_kwargs = mock_redis_instance.set.call_args.kwargs
    assert mark_synced_kwargs["ex"] == sync_players.SYNC_LOCK_TTL_SECONDS


def test_sync_players_does_not_mark_synced_when_publish_fails():
    mock_redis_instance = MagicMock()
    mock_redis_instance.set.return_value = True

    with patch("src.ingestion.sync_players.get_redis_client", return_value=mock_redis_instance), \
         patch("src.ingestion.sync_players.fetch_sleeper_players", return_value={}), \
         patch("src.ingestion.sync_players.assert_position_coverage"), \
         patch("src.ingestion.sync_players.get_db_connection") as mock_get_conn, \
         patch("src.ingestion.sync_players.publish_waiver_alerts", side_effect=Exception("broker down")):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        with pytest.raises(Exception, match="broker down"):
            sync_players.sync_players(force=True)

    # Only the initial lock-acquire call happened; mark_synced (the 24h extension) did not.
    assert mock_redis_instance.set.call_count == 1
