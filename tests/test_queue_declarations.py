from unittest.mock import MagicMock, patch

import pytest

from src.api import routes


@pytest.mark.parametrize(
    "handler,payload,queue",
    [
        (
            routes.trigger_lineup_swap,
            routes.LineupTriggerPayload(
                team_id="1",
                optimized_swaps=[
                    {"drop_player_id": "4046", "add_player_id": "4983", "slot": "FLEX"}
                ],
            ),
            "lineup_updates",
        ),
        (
            routes.trigger_waiver_claim,
            routes.WaiverTriggerPayload(
                team_id="1", player_id="4983", name="Test Player", team="CHI"
            ),
            "waiver_actions",
        ),
        (
            routes.trigger_draft_pick,
            routes.DraftPickTriggerPayload(
                team_id="1",
                player_id="4983",
                name="Test Player",
                position="RB",
                pro_team="CHI",
            ),
            "draft_targets",
        ),
    ],
)
def test_trigger_attaches_to_broker_queue_without_redeclaring_it(
    handler, payload, queue
):
    channel = MagicMock()
    connection = MagicMock()
    connection.channel.return_value = channel
    with (
        patch("src.api.routes.pika.BlockingConnection", return_value=connection),
        patch("src.api.routes._reject_internal_player_id"),
        patch("src.api.routes._record_bot_decision"),
        patch("src.api.routes._claim_idempotency_key", return_value="claimed"),
        patch("src.api.routes._pick_slot_already_filled", return_value=False),
    ):
        result = handler(payload)

    assert result["status"] == "accepted"
    channel.queue_declare.assert_called_once_with(queue=queue, passive=True)
    published = channel.basic_publish.call_args.kwargs
    assert published["routing_key"] == queue
    assert published["properties"].delivery_mode == 2
