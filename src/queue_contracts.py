"""RabbitMQ routing keys shared by core publishers."""

LINEUP_UPDATES = "lineup_updates"
WAIVER_ACTIONS = "waiver_actions"
DRAFT_TARGETS = "draft_targets"
TRADE_SUBMISSIONS = "trade_submissions"
TRADE_RESPONSES = "trade_responses"
TRADE_NEGOTIATIONS = "trade_negotiations"

PUBLISHED_QUEUES = frozenset(
    {
        LINEUP_UPDATES,
        WAIVER_ACTIONS,
        DRAFT_TARGETS,
        TRADE_SUBMISSIONS,
        TRADE_RESPONSES,
        TRADE_NEGOTIATIONS,
    }
)
