from unittest.mock import MagicMock, patch

from src.backtesting import draft_simulator


def test_compute_snake_order_reverses_each_round():
    order = draft_simulator.compute_snake_order(team_count=3, total_rounds=2)
    assert order == [
        (1, 1), (2, 2), (3, 3),
        (4, 3), (5, 2), (6, 1),
    ]


def test_compute_snake_order_pick_count():
    order = draft_simulator.compute_snake_order(team_count=10, total_rounds=16)
    assert len(order) == 160
    assert order[0] == (1, 1)
    assert order[-1] == (160, 1)


def test_pick_best_available_by_adp_excludes_drafted():
    cursor = MagicMock()
    cursor.fetchone.return_value = ("Player Two", "WR", "BBB")
    cursor.fetchall.return_value = [("1",)]
    adp = {"1": 1, "2": 2, "3": 3}
    result = draft_simulator.pick_best_available_by_adp(cursor, adp, "backtest", 2023)
    assert result == {"player_id": "2", "name": "Player Two", "position": "WR", "team": "BBB"}


def test_pick_best_available_by_adp_empty_pool_returns_none():
    cursor = MagicMock()
    cursor.fetchall.return_value = [("1",), ("2",)]
    adp = {"1": 1, "2": 2}
    result = draft_simulator.pick_best_available_by_adp(cursor, adp, "backtest", 2023)
    assert result is None


def test_run_draft_our_turns_use_draft_matrix_opponents_use_adp():
    mock_conn = MagicMock()
    adp = {"opp1": 1}

    def fake_recommend(cursor, league_id, season, week, team_id, limit):
        return [{"player_id": "us1", "name": "Our Pick", "position": "RB", "team": "AAA"}]

    with patch.object(draft_simulator.draft_matrix, "compute_draft_recommendation_candidates", side_effect=fake_recommend) as mock_reco, \
         patch.object(draft_simulator, "pick_best_available_by_adp", return_value={"player_id": "opp1", "name": "Opp Pick", "position": "WR", "team": "BBB"}) as mock_adp_pick, \
         patch.object(draft_simulator.draft_matrix, "persist_draft_pick") as mock_persist:
        picks = draft_simulator.run_draft(mock_conn, adp, season=2023, rounds=1, week=1)

    assert len(picks) == 10
    assert mock_reco.call_count == 1
    assert mock_adp_pick.call_count == 9
    assert mock_persist.call_count == 10
    # our pick is slot 1, first pick of round 1
    assert picks[0]["team_id"] == draft_simulator.OUR_TEAM_ID
    assert picks[0]["player_id"] == "us1"


def test_run_draft_persists_every_pick_before_next_turn():
    mock_conn = MagicMock()
    call_order = []

    def fake_recommend(cursor, league_id, season, week, team_id, limit):
        call_order.append("recommend")
        return [{"player_id": "us1", "name": "Our Pick", "position": "RB", "team": "AAA"}]

    def fake_persist(conn, league_id, season, pick_number, round_, team_id, player_id):
        call_order.append(f"persist-{pick_number}")

    with patch.object(draft_simulator.draft_matrix, "compute_draft_recommendation_candidates", side_effect=fake_recommend), \
         patch.object(draft_simulator, "pick_best_available_by_adp", return_value={"player_id": "opp1", "name": "Opp", "position": "WR", "team": "BBB"}), \
         patch.object(draft_simulator.draft_matrix, "persist_draft_pick", side_effect=fake_persist):
        draft_simulator.run_draft(mock_conn, {}, season=2023, rounds=1, week=1)

    assert call_order == ["recommend", "persist-1"] + [f"persist-{n}" for n in range(2, 11)]


def test_seed_rosters_from_draft_inserts_all_players_as_bench():
    mock_conn = MagicMock()
    mock_cursor = mock_conn.cursor.return_value.__enter__.return_value
    picks = [
        {"pick_number": 1, "round": 1, "team_id": "backtest-team-1", "player_id": "us1", "name": "A", "position": "RB", "team": "AAA"},
        {"pick_number": 2, "round": 1, "team_id": "backtest-team-2", "player_id": "opp1", "name": "B", "position": "WR", "team": "BBB"},
    ]
    count = draft_simulator.seed_rosters_from_draft(mock_conn, picks)
    assert count == 2
    args, _ = mock_cursor.executemany.call_args
    rows = args[1]
    assert rows == [("backtest-team-1", "us1", "BE", False), ("backtest-team-2", "opp1", "BE", False)]


def test_seed_rosters_from_draft_empty_noop():
    mock_conn = MagicMock()
    count = draft_simulator.seed_rosters_from_draft(mock_conn, [])
    assert count == 0
    mock_conn.cursor.assert_not_called()


def test_simulate_pure_adp_draft_gives_best_players_to_slot_one():
    adp = {f"p{i}": i for i in range(1, 21)}
    rosters = draft_simulator.simulate_pure_adp_draft(adp, team_count=10, rounds=2)
    assert rosters["backtest-team-1"] == ["p1", "p20"]
    assert rosters["backtest-team-10"] == ["p10", "p11"]


def test_simulate_pure_adp_draft_no_player_drafted_twice():
    adp = {f"p{i}": i for i in range(1, 21)}
    rosters = draft_simulator.simulate_pure_adp_draft(adp, team_count=10, rounds=2)
    all_drafted = [pid for roster in rosters.values() for pid in roster]
    assert len(all_drafted) == len(set(all_drafted)) == 20


def test_simulate_pure_adp_draft_stops_when_pool_exhausted():
    adp = {"p1": 1, "p2": 2}
    rosters = draft_simulator.simulate_pure_adp_draft(adp, team_count=10, rounds=2)
    all_drafted = [pid for roster in rosters.values() for pid in roster]
    assert all_drafted == ["p1", "p2"]
