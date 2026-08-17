import os
import json
import logging
import uuid
import pika
from typing import List
from fastapi import APIRouter, HTTPException, status
from opentelemetry import propagate
from pydantic import BaseModel, Field
from src.database import get_db_cursor

logger = logging.getLogger("GatewayRoutes")
router = APIRouter()

# Load AMQP broker settings securely injected into environment variables
# Default vhost must be the percent-encoded "%2f", not a bare trailing "/" - a bare "//"
# parses as vhost "" (empty string) in pika, not "/", and RabbitMQ then rejects the
# connection with "vhost  not found" instead of connecting to the default vhost.
AMQP_URL = os.getenv("AMQP_URL", "amqp://guest:guest@fantasy-broker.databases.svc.cluster.local:5672/%2f")

# ------------------------------------------------------------------------------
# PYDANTIC DATA TRANSFER OBJECTS (SCHEMAS)
# ------------------------------------------------------------------------------
class SwapInstruction(BaseModel):
    drop_player_id: str = Field(..., description="Unique ESPN/Sleeper tracking ID to bench or cut")
    add_player_id: str = Field(..., description="Unique ESPN/Sleeper tracking ID to elevate to active lineup")
    slot: str = Field(..., description="Target lineup position frame (e.g., RB1, WR2, FLEX)")

class LineupTriggerPayload(BaseModel):
    team_id: str = Field(..., description="Target local league team identification number")
    optimized_swaps: List[SwapInstruction] = Field(..., description="Array of mathematical player reallocations")

class DraftPickTriggerPayload(BaseModel):
    team_id: str = Field(..., description="Target local league team identification number")
    player_id: str = Field(..., description="players.player_id (Sleeper-namespaced ID, not espn_id) of the recommended draft target")
    name: str = Field(..., description="Display name of the recommended draft target, as expected by the click bot")
    position: str = Field(..., description="Player's position, for a name+position+team match against ESPN's own player pool")
    pro_team: str = Field(..., description="Player's NFL team abbreviation, for a name+position+team match against ESPN's own player pool")

# ------------------------------------------------------------------------------
# ROUTE ENDPOINTS
# ------------------------------------------------------------------------------
@router.get("/health", status_code=status.HTTP_200_OK)
def health_check():
    """
    Lightweight health probe verifying cluster connection liveness to internal datastores.
    Used natively by Kubernetes liveness and readiness tracking monitors.
    """
    # 1. Validate PostgreSQL connection health
    try:
        with get_db_cursor() as cursor:
            cursor.execute("SELECT 1;")
    except Exception as e:
        logger.error(f"Liveness probe failed database connection check: {e}")
        raise HTTPException(status_code=503, detail="Stateful database tier connection unreachable.")

    # 2. Validate RabbitMQ broker link parameters
    try:
        params = pika.URLParameters(AMQP_URL)
        connection = pika.BlockingConnection(params)
        connection.close()
    except Exception as e:
        logger.error(f"Liveness probe failed message broker connection check: {e}")
        raise HTTPException(status_code=503, detail="Event message bus broker unreachable.")

    return {"status": "healthy", "database": "connected", "broker": "connected"}

@router.post("/trigger/lineup", status_code=status.HTTP_202_ACCEPTED)
def trigger_lineup_swap(payload: LineupTriggerPayload):
    """
    Asymmetric API gateway: Translates incoming high-level HTTP payloads into structured
    persistent queue messages inside RabbitMQ to activate automated browser worker scripts.
    """
    tracking_id = str(uuid.uuid4())
    logger.info(f"Gateway processing roster realignment command sequence for Team ID {payload.team_id} (tracking_id={tracking_id})...")

    try:
        # 1. Establish an on-demand transaction connection channel to the cluster broker
        params = pika.URLParameters(AMQP_URL)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()

        # 2. Declare and enforce durable queue settings matching the infrastructure layer
        channel.queue_declare(queue="lineup_updates", durable=True)

        # W3C trace context, separate from tracking_id below: tracking_id stays the
        # human/log-facing correlation UUID already returned to callers; traceparent is
        # the machine-facing OTel context so fantasy-browser-worker's eventual consumer
        # span (WORKER-09) can link back into this same trace instead of starting a
        # disconnected one. Per #26's spec this rides in the AMQP JSON body itself
        # (alongside tracking_id), not only in AMQP message properties, so a consumer
        # that only parses the body - not pika headers - can still recover it.
        carrier: dict = {}
        propagate.inject(carrier)

        # 3. Serialize our structured Pydantic body, stamped with a correlation ID and
        # trace context, into a compact system string mapping
        message_body = json.dumps({
            "tracking_id": tracking_id,
            "traceparent": carrier.get("traceparent"),
            **payload.model_dump(),
        })

        # 4. Publish message with high persistence markers to survive unexpected broker reboots
        channel.basic_publish(
            exchange="",
            routing_key="lineup_updates",
            body=message_body,
            properties=pika.BasicProperties(
                delivery_mode=2, # Make message persistent on disk
                content_type="application/json",
                headers=carrier,
            )
        )
        connection.close()
        logger.info(f"Asymmetric event transaction committed securely to RabbitMQ for Team {payload.team_id}.")
        return {
            "status": "accepted",
            "queue": "lineup_updates",
            "team_id": payload.team_id,
            "tracking_id": tracking_id,
        }

    except Exception as e:
        logger.error(f"Gateway failed to push transaction onto system exchange queue: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to submit message to background execution broker: {str(e)}")

@router.post("/trigger/draft-pick", status_code=status.HTTP_202_ACCEPTED)
def trigger_draft_pick(payload: DraftPickTriggerPayload):
    """
    Draft-day counterpart to /trigger/lineup (CORE-08): publishes the live optimizer's
    recommended draft target to the click-bot queue. Same asymmetric gateway shape -
    HTTP in, durable AMQP message out - the optimizer module never touches pika directly.
    """
    tracking_id = str(uuid.uuid4())
    logger.info(f"Gateway processing draft pick command for Team ID {payload.team_id} -> {payload.name} (tracking_id={tracking_id})...")

    try:
        params = pika.URLParameters(AMQP_URL)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()

        channel.queue_declare(queue="draft_targets", durable=True)

        carrier: dict = {}
        propagate.inject(carrier)

        message_body = json.dumps({
            "tracking_id": tracking_id,
            "traceparent": carrier.get("traceparent"),
            **payload.model_dump(),
        })

        channel.basic_publish(
            exchange="",
            routing_key="draft_targets",
            body=message_body,
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type="application/json",
                headers=carrier,
            )
        )
        connection.close()
        logger.info(f"Draft pick target committed to RabbitMQ for Team {payload.team_id}: {payload.name}.")
        return {
            "status": "accepted",
            "queue": "draft_targets",
            "team_id": payload.team_id,
            "tracking_id": tracking_id,
        }

    except Exception as e:
        logger.error(f"Gateway failed to push draft pick target onto system exchange queue: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to submit message to background execution broker: {str(e)}")

