from unittest.mock import MagicMock, patch

from src.optimization import draft_matrix as dm

LEAGUE_CONFIG = {
    "team_count": 10,
    "lineup_slot_counts": {"QB": 1, "RB": 2, "WR": 2, "TE": 1, "RB/WR/TE": 1, "BE": 7},
}


def _player(pid, position, vorp_score, name=None):
    return {"player_id": pid, "name": name or pid, "position": position, "vorp_score": vorp_score}


# ---------------------------------------------------------------------------
# resolve_own_team_flex_allocation / compute_position_caps
# ---------------------------------------------------------------------------

def test_resolve_own_team_flex_allocation_uses_default_weights_pre_draft():
    alloc = dm.resolve_own_team_flex_allocation(LEAGUE_CONFIG)
    assert sum(alloc.values()) == 1
    assert set(alloc.keys()) <= {"RB", "WR", "TE"}


def test_compute_position_caps_no_picks_yet():
    position_caps, starting_needs = dm.compute_position_caps(LEAGUE_CONFIG, drafted_by_us={})

    # Starting needs: QB=1, RB=2, WR=2, TE=1, plus 1 FLEX slot split across RB/WR/TE.
    assert starting_needs["QB"] == 1
    assert starting_needs["RB"] >= 2
    assert starting_needs["WR"] >= 2
    assert starting_needs["TE"] >= 1

    # Caps must be >= starting needs (bench adds on top), and QB caps stay tight since
    # QB has no FLEX/bench share in FLEX_DEFAULT_WEIGHTS.
    for pos in dm.SKILL_POSITIONS:
        assert position_caps[pos] >= starting_needs[pos]
    assert position_caps["QB"] == starting_needs["QB"]


def test_compute_position_caps_shrinks_as_we_draft():
    # RB's starting target includes its FLEX_DEFAULT_WEIGHTS share of the 1 FLEX slot
    # (0.5 weight -> the FLEX slot itself under largest-remainder rounding), so 2 drafted
    # RBs still leaves 1 unit of starting need (the FLEX slot), unlike QB which has none.
    position_caps, starting_needs = dm.compute_position_caps(LEAGUE_CONFIG, drafted_by_us={"QB": 1, "RB": 3})
    assert starting_needs["QB"] == 0
    assert starting_needs["RB"] == 0
    assert position_caps["QB"] == 0  # QB cap has no bench share, so fully consumed


# ---------------------------------------------------------------------------
# build_and_solve_draft_pick
# ---------------------------------------------------------------------------

def test_build_and_solve_draft_pick_respects_position_caps():
    pool = [
        _player("rb1", "RB", 10.0),
        _player("rb2", "RB", 9.0),
        _player("rb3", "RB", 8.0),
        _player("wr1", "WR", 7.0),
    ]
    selected = dm.build_and_solve_draft_pick(pool, position_caps={"RB": 2, "WR": 1, "QB": 0, "TE": 0}, remaining_picks=5)
    positions = [p["position"] for p in selected]
    assert positions.count("RB") == 2
    assert positions.count("WR") == 1
    # Must take the two highest-VORP RBs, not rb3.
    ids = {p["player_id"] for p in selected}
    assert "rb1" in ids and "rb2" in ids and "rb3" not in ids


def test_build_and_solve_draft_pick_respects_remaining_picks_budget():
    pool = [_player("rb1", "RB", 10.0), _player("wr1", "WR", 9.0), _player("wr2", "WR", 8.0)]
    selected = dm.build_and_solve_draft_pick(pool, position_caps={"RB": 5, "WR": 5, "QB": 5, "TE": 5}, remaining_picks=1)
    assert len(selected) == 1
    assert selected[0]["player_id"] == "rb1"


def test_build_and_solve_draft_pick_empty_pool_returns_empty():
    assert dm.build_and_solve_draft_pick([], {}, remaining_picks=5) == []


def test_build_and_solve_draft_pick_zero_remaining_picks_returns_empty():
    pool = [_player("rb1", "RB", 10.0)]
    assert dm.build_and_solve_draft_pick(pool, {"RB": 5}, remaining_picks=0) == []


# ---------------------------------------------------------------------------
# select_recommended_target
# ---------------------------------------------------------------------------

def test_select_recommended_target_prioritizes_open_starting_need():
    selected = [_player("rb1", "RB", 20.0), _player("te1", "TE", 5.0)]
    # RB starting need already filled (0); TE still needed - TE should win even though
    # its raw vorp_score is lower.
    target = dm.select_recommended_target(selected, starting_needs={"RB": 0, "TE": 1})
    assert target["player_id"] == "te1"


def test_select_recommended_target_falls_back_to_best_available():
    selected = [_player("rb1", "RB", 20.0), _player("te1", "TE", 5.0)]
    target = dm.select_recommended_target(selected, starting_needs={"RB": 0, "TE": 0})
    assert target["player_id"] == "rb1"


def test_select_recommended_target_empty_selection_returns_none():
    assert dm.select_recommended_target([], starting_needs={}) is None


# ---------------------------------------------------------------------------
# load_undrafted_pool / load_team_position_counts / persist_draft_pick (query shape)
# ---------------------------------------------------------------------------

def test_load_undrafted_pool_excludes_drafted_players():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []

    dm.load_undrafted_pool(mock_cursor, league_id="L1", season=2026, week=1)

    query, params = mock_cursor.execute.call_args[0]
    assert "NOT EXISTS" in query
    assert "draft_picks" in query
    assert params == (2026, 1, "L1", 2026)


def test_load_team_position_counts_query_shape():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("RB", 2), ("WR", 1)]

    counts = dm.load_team_position_counts(mock_cursor, league_id="L1", season=2026, team_id="7")

    assert counts == {"RB": 2, "WR": 1}
    query, params = mock_cursor.execute.call_args[0]
    assert "draft_picks" in query
    assert params == ("L1", 2026, "7")


def test_persist_draft_pick_uses_on_conflict_do_nothing():
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    dm.persist_draft_pick(mock_conn, league_id="L1", season=2026, pick_number=5, round_=1, team_id="7", player_id="p1")

    query, params = mock_cursor.execute.call_args[0]
    assert "ON CONFLICT" in query
    assert "DO NOTHING" in query
    assert params == ("L1", 2026, 5, 1, "7", "p1")


# ---------------------------------------------------------------------------
# wait_for_pause_or_timeout
# ---------------------------------------------------------------------------

def test_wait_for_pause_or_timeout_proceeds_when_never_paused():
    mock_client = MagicMock()
    mock_client.get.return_value = None
    sleeps = []

    result = dm.wait_for_pause_or_timeout(mock_client, "L1", pick_number=3, delay_seconds=3, sleep_fn=sleeps.append)

    assert result is True
    assert len(sleeps) == 3


def test_wait_for_pause_or_timeout_aborts_when_paused():
    mock_client = MagicMock()
    mock_client.get.return_value = "1"

    result = dm.wait_for_pause_or_timeout(mock_client, "L1", pick_number=3, delay_seconds=5, sleep_fn=lambda s: None)

    assert result is False


# ---------------------------------------------------------------------------
# publish_draft_target
# ---------------------------------------------------------------------------

def test_publish_draft_target_posts_expected_payload():
    target = _player("p1", "RB", 12.0, name="Some Back")

    with patch("src.optimization.draft_matrix.requests.post") as mock_post:
        mock_post.return_value = MagicMock(status_code=202)
        dm.publish_draft_target("7", target)

    url, kwargs = mock_post.call_args[0], mock_post.call_args[1]
    assert url[0] == f"{dm.CORE_API_BASE_URL}/v1/trigger/draft-pick"
    assert kwargs["json"] == {"team_id": "7", "player_id": "p1", "name": "Some Back"}


# ---------------------------------------------------------------------------
# handle_pick_event (orchestrator)
# ---------------------------------------------------------------------------

def test_handle_pick_event_skips_recommendation_when_we_just_picked():
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_client = MagicMock()

    event = {"pick_number": "1", "round": "1", "team_id": "7", "player_id": "p1"}

    with patch("src.optimization.draft_matrix.get_db_connection") as mock_get_conn, \
         patch("src.optimization.draft_matrix.requests.post") as mock_post:
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        dm.handle_pick_event(mock_client, "L1", 2026, 1, our_team_id="7", delay_seconds=1, event=event)

    mock_post.assert_not_called()
    mock_client.hset.assert_not_called()


def test_handle_pick_event_publishes_after_opponent_pick():
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_client = MagicMock()
    mock_client.get.return_value = None

    event = {"pick_number": "1", "round": "1", "team_id": "3", "player_id": "p1"}
    target = _player("p2", "RB", 10.0, name="Best Back")

    with patch("src.optimization.draft_matrix.get_db_connection") as mock_get_conn, \
         patch("src.optimization.draft_matrix.compute_draft_recommendation", return_value=target), \
         patch("src.optimization.draft_matrix.time.sleep"), \
         patch("src.optimization.draft_matrix.requests.post") as mock_post:
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        mock_post.return_value = MagicMock(status_code=202)
        dm.handle_pick_event(mock_client, "L1", 2026, 1, our_team_id="7", delay_seconds=1, event=event)

    mock_client.hset.assert_called_once()
    mock_post.assert_called_once()
    assert mock_post.call_args[1]["json"]["player_id"] == "p2"
