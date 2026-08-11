import os
import json
import logging
import uuid
import pika
from typing import List
from fastapi import APIRouter, HTTPException, status
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

        # 3. Serialize our structured Pydantic body, stamped with a correlation ID, into a compact system string mapping
        message_body = json.dumps({"tracking_id": tracking_id, **payload.model_dump()})

        # 4. Publish message with high persistence markers to survive unexpected broker reboots
        channel.basic_publish(
            exchange="",
            routing_key="lineup_updates",
            body=message_body,
            properties=pika.BasicProperties(
                delivery_mode=2, # Make message persistent on disk
                content_type="application/json"
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

