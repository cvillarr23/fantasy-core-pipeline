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
# Launcher events ("this happened"): a topic exchange whose routing key is the event's key.
JOB_EVENTS = "job_events"
# The launcher's own subscription to JOB_EVENTS; another subscriber binds its own queue.
JOB_EVENTS_LAUNCHER_QUEUE = "job_events.launcher"
# An exact dotted routing key: `<domain>.<fact>`. A wildcard is a binding, not an event.
EVENT_KEY_PATTERN = r"^[a-z0-9_]+(\.[a-z0-9_]+)+$"

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
PUBLISHED_EXCHANGES = frozenset({JOB_EVENTS})
