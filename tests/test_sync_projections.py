from unittest.mock import MagicMock, patch

import pytest
import requests

from src.ingestion import sync_projections as sp


def _proj_row(player_id="4046", season="2024", week=1, pts_ppr=16.31, pts_half_ppr=14.6, pts_std=12.89):
    return {
        "player_id": player_id, "season": season, "week": week, "team": "SF", "opponent": "LAR",
        "stats": {"pts_ppr": pts_ppr, "pts_half_ppr": pts_half_ppr, "pts_std": pts_std},
    }


def test_build_projection_frame_selects_pts_ppr_not_half_or_std():
    frame = sp.build_projection_frame([_proj_row()])
    row = frame.row(0, named=True)

    assert row["projected_points"] == 16.31
    assert row["projected_points"] != 14.6
    assert row["projected_points"] != 12.89


def test_build_projection_frame_missing_stat_defaults_to_zero():
    row = _proj_row()
    row["stats"] = {"pts_half_ppr": 14.6}  # no pts_ppr key at all
    frame = sp.build_projection_frame([row])

    assert frame.row(0, named=True)["projected_points"] == 0.0


def test_build_projection_frame_null_stat_defaults_to_zero():
    row = _proj_row(pts_ppr=None)
    frame = sp.build_projection_frame([row])

    assert frame.row(0, named=True)["projected_points"] == 0.0


def test_persist_projections_filters_unknown_player_ids():
    frame = sp.build_projection_frame([_proj_row(player_id="4046"), _proj_row(player_id="unknown")])
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    written = sp.persist_projections(mock_conn, frame, known_player_ids={"4046"})

    assert written == 1
    rows = mock_cursor.executemany.call_args.args[1]
    assert rows[0][0] == "4046"


def test_persist_projections_set_clause_touches_only_projected_points():
    frame = sp.build_projection_frame([_proj_row()])
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    sp.persist_projections(mock_conn, frame, known_player_ids={"4046"})

    query = mock_cursor.executemany.call_args.args[0]
    assert "SET projected_points = EXCLUDED.projected_points" in query
    assert "vorp_score" not in query
    assert "actual_points" not in query


def test_fetch_projections_retries_then_raises_after_exhausting_attempts():
    """Aborts the whole fetch as soon as one position exhausts its retries, rather than
    plowing ahead and fetching the remaining positions."""
    with patch("src.ingestion.sync_projections.requests.get") as mock_get:
        mock_get.side_effect = requests.ConnectionError("boom")
        with pytest.raises(requests.ConnectionError):
            sp.fetch_projections(season=2024, week=1)

    assert mock_get.call_count == sp.FETCH_RETRY_ATTEMPTS


def test_fetch_projections_aggregates_rows_across_positions():
    mock_response = MagicMock()
    mock_response.json.return_value = [_proj_row()]
    mock_response.raise_for_status.return_value = None

    with patch("src.ingestion.sync_projections.requests.get", return_value=mock_response) as mock_get:
        rows = sp.fetch_projections(season=2024, week=1)

    assert len(rows) == len(sp.SKILL_POSITIONS)
    assert mock_get.call_count == len(sp.SKILL_POSITIONS)


def _bye_row(player_id="5970"):
    # Sleeper's shape for a player whose team is on bye: a team, no opponent or game,
    # and no scoring stats at all.
    return {"player_id": player_id, "season": "2026", "week": 7, "team": "BUF",
            "opponent": None, "game_id": None, "stats": {"adp_dd_ppr": 1000.0}}


def test_build_projection_frame_skips_bye_week_players():
    """A bye is no row, never a 0.0 one: lineup, waiver and trade code read a missing
    row as "on bye", while a 0.0 row looks like a startable player with negative VORP."""
    frame = sp.build_projection_frame([_proj_row(player_id="4046"), _bye_row()])

    assert frame["player_id"].to_list() == ["4046"]


def test_build_projection_frame_keeps_teamless_players_without_opponent():
    """No team (a free agent in Sleeper's data) isn't a bye; keep the existing 0.0 row."""
    row = {**_bye_row(player_id="999"), "team": None}

    frame = sp.build_projection_frame([row])

    assert frame["player_id"].to_list() == ["999"]


def test_bye_week_player_ids_names_only_players_on_a_team_with_no_game():
    raw = [_proj_row(player_id="4046"), _bye_row(player_id="5970"),
           {**_bye_row(player_id="999"), "team": None}]

    assert sp.bye_week_player_ids(raw) == {"5970"}


def test_delete_bye_week_projections_only_touches_unscored_rows_for_that_week():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.rowcount = 1

    deleted = sp.delete_bye_week_projections(mock_conn, 2026, 7, {"5970"})

    assert deleted == 1
    query, params = mock_cursor.execute.call_args.args
    assert "DELETE FROM weekly_stats" in query
    assert "actual_points_source IS NULL" in query
    assert params == (["5970"], 2026, 7)


def test_delete_bye_week_projections_skips_the_query_with_no_byes():
    mock_conn = MagicMock()

    assert sp.delete_bye_week_projections(mock_conn, 2026, 7, set()) == 0
    mock_conn.cursor.assert_not_called()
