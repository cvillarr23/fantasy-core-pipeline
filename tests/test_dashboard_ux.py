from types import SimpleNamespace
from unittest.mock import patch, MagicMock
import pytest
from src.services import league_dashboard as dashboard
from src.services import activity_service as activity
from src.services.activity_runner import reconcile_action


def player(pid, position="RB", pts=10, status="ACTIVE"):
    return SimpleNamespace(
        playerId=pid,
        name=str(pid),
        position=position,
        proTeam="KC",
        injuryStatus=status,
        lineupSlot="BE",
        eligibleSlots=[position, "RB/WR/TE"],
        schedule={5: {}, 6: {}},
        stats={
            5: {"projected_points": pts},
            3: {"points": 8, "breakdown": {"rush": 1}},
            4: {"points": 12, "breakdown": {"rush": 1}},
        },
    )


def test_points_optimizer_handles_flex_without_double_counting():
    total, lineup, source = dashboard.solve_points(
        [player(1, pts=20), player(2, "WR", pts=15), player(3, pts=5)],
        {"RB": 1, "RB/WR/TE": 1},
        5,
        5,
    )
    assert total == 35
    assert len({p["espn_id"] for p in lineup}) == 2
    assert source == "Published projections"


def test_missing_projection_never_fakes_a_complete_total():
    p = player(1)
    p.stats = {}
    total, _, source = dashboard.solve_points([p, player(2)], {"RB": 1}, 5, 5)
    assert total is None
    assert "Incomplete" in source


def test_estimate_excludes_future_and_current_game_results():
    p = player(1)
    p.stats[5] = {"points": 100, "breakdown": {"rush": 1}}
    assert dashboard.point_projection(p, 6, 5) == (10, "Estimated from recent games")


def test_bye_and_ir_players_are_not_selected():
    p = player(1, pts=100)
    p.schedule = {4: {}}
    total, lineup, _ = dashboard.solve_points(
        [p, player(2, status="IR"), player(3, pts=8)], {"RB": 1}, 5, 5
    )
    assert total == 8 and lineup[0]["espn_id"] == "3"


def test_live_scores_require_exact_season_week_and_actual_source():
    p = {
        "stats": [
            {
                "seasonId": 2026,
                "scoringPeriodId": 5,
                "statSourceId": 1,
                "appliedTotal": 20,
            },
            {
                "seasonId": 2025,
                "scoringPeriodId": 5,
                "statSourceId": 0,
                "appliedTotal": 90,
            },
        ]
    }
    assert dashboard.stat_value(p, 2026, 5, 0) is None
    assert dashboard.stat_value(p, 2026, 5, 1) == 20


def test_reconciliation_requires_both_halves_of_waiver(monkeypatch):
    action = {
        "league_id": "123",
        "execution": "submitted",
        "outcome": "awaiting_processing",
        "kind": "waiver_actions",
        "payload": {"team_id": "8", "player_id": "1", "drop_player_id": "2"},
    }
    league = SimpleNamespace(
        teams=[
            SimpleNamespace(
                team_id=8,
                roster=[
                    SimpleNamespace(playerId=1, lineupSlot="BE"),
                    SimpleNamespace(playerId=2, lineupSlot="BE"),
                ],
            )
        ]
    )
    with (
        patch("src.services.activity_runner.action_detail", return_value=action),
        patch("src.ingestion.sync_league.build_league", return_value=league),
        patch("src.services.activity_runner.record_event") as event,
        patch("src.services.activity_runner.get_db_cursor"),
        patch("src.services.read_cache.invalidate_league"),
    ):
        reconcile_action("test")
        assert event.call_args.args[1] == "status_checked"
        league.teams[0].roster.pop()
        reconcile_action("test")
        assert event.call_args.kwargs["outcome"] == "verified_complete"


def test_redaction_handles_nested_secrets_and_bearer(monkeypatch):
    monkeypatch.setenv("TRIGGER_API_KEY", "private-123456")
    assert activity.redact(
        {
            "Authorization": "anything",
            "content": ["key private-123456", "Bearer abcdefghi"],
        }
    ) == {
        "Authorization": "[REDACTED]",
        "content": ["key [REDACTED]", "Bearer [REDACTED]"],
    }


def test_duplicate_action_returns_original_without_second_outbox():
    c = MagicMock()
    c.fetchone.side_effect = [None, ("original",)]
    with patch.object(activity, "get_db_cursor") as db:
        db.return_value.__enter__.return_value = c
        assert (
            activity.create_action("waiver_actions", {"idempotency_key": "same"}, "new")
            == "original"
        )
    assert not any(
        "INSERT INTO activity_outbox" in x.args[0] for x in c.execute.call_args_list
    )


def test_legacy_keyed_waiver_reuses_original_tracking_without_new_outbox():
    from unittest.mock import MagicMock, patch
    from contextlib import contextmanager
    from src.services.activity_service import create_action

    cursor = MagicMock()
    cursor.fetchone.return_value = ("original-waiver",)

    @contextmanager
    def db():
        yield cursor

    with patch("src.services.activity_service.get_db_cursor", db):
        result = create_action(
            "waiver_actions",
            {
                "idempotency_key": "new-key-for-same-week",
                "team_id": "8",
                "player_id": "1",
                "drop_player_id": "2",
                "decision_inputs": {"season": 2026, "week": 5},
            },
        )
    assert result == "original-waiver"
    assert not any(
        "activity_outbox" in call.args[0] for call in cursor.execute.call_args_list
    )
    assert next(call.args[1] for call in cursor.execute.call_args_list if "SELECT tracking_id" in call.args[0]) == ("8", 2026, 5, "1", "2")


def espn_card(pid, position="RB", pts=10, team_id=0):
    # Mirrors espn_api Player cards from league.player_info(): json_parsing() yields []
    # for absent keys, and schedule keys are the raw string scoring periods.
    p = player(pid, position, pts, status=[])
    p.onTeamId = [] if team_id == 0 else team_id
    p.schedule = {"5": [{}], "6": [{}]}
    return p


def preview_league(cards, roster_ids):
    return SimpleNamespace(
        current_week=5,
        teams=[
            SimpleNamespace(
                team_id=8,
                roster=[SimpleNamespace(playerId=pid, lineupSlot="BE") for pid in roster_ids],
            )
        ],
        player_info=lambda playerId: [c for c in cards if c.playerId in playerId],
    )


def test_move_preview_handles_espn_card_shapes(monkeypatch):
    monkeypatch.setenv("ESPN_TEAM_ID", "8")
    cards = [espn_card(1, pts=12), espn_card(2, pts=6), espn_card(3, pts=20)]
    raw = {"settings": {"rosterSettings": {"lineupSlotCounts": {"2": 1, "20": 2}}}}
    payload = SimpleNamespace(
        kind="waiver",
        season=2026,
        start_week=5,
        end_week=5,
        incoming_espn_ids=["3"],
        outgoing_espn_ids=["2"],
    )
    with (
        patch.object(dashboard, "build_league", return_value=preview_league(cards, [1, 2])),
        patch.object(dashboard, "fetch_raw_league_data", return_value=raw),
    ):
        result = dashboard.move_preview(payload)
    week = result["weeks"][0]
    assert (week["before"], week["after"], week["delta"]) == (12, 20, 8)
    assert result["incoming"][0]["injury_status"] is None
    assert not any("[]" in w or "rostered by another team" in w for w in result["warnings"])
    assert not any("byes" in w for w in result["warnings"])


def test_point_projection_treats_missing_injury_status_as_available():
    p = player(1, status=[])
    assert dashboard.point_projection(p, 5, 5) == (10, "Published projection")
