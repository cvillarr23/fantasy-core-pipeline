"""RabbitMQ connection settings and routing keys shared by core publishers."""

import os

# Default vhost must be the percent-encoded "%2f", not a bare trailing "/" - a bare "//"
# parses as vhost "" (empty string) in pika, not "/", and RabbitMQ then rejects the
# connection with "vhost  not found" instead of connecting to the default vhost.
AMQP_URL = os.getenv(
    "AMQP_URL", "amqp://guest:guest@fantasy-broker.databases.svc.cluster.local:5672/%2f"
)

LINEUP_UPDATES = "lineup_updates"
WAIVER_ACTIONS = "waiver_actions"
DRAFT_TARGETS = "draft_targets"
TRADE_SUBMISSIONS = "trade_submissions"
TRADE_RESPONSES = "trade_responses"
TRADE_NEGOTIATIONS = "trade_negotiations"
# Launcher run commands ("run this job"), consumed only by `python -m src.launcher`.
JOB_RUNS = "job_runs"

PUBLISHED_QUEUES = frozenset(
    {
        LINEUP_UPDATES,
        WAIVER_ACTIONS,
        DRAFT_TARGETS,
        TRADE_SUBMISSIONS,
        TRADE_RESPONSES,
        TRADE_NEGOTIATIONS,
        JOB_RUNS,
    }
)
