import asyncio
from src.services.activity_runner import run as run_activity
from src.api.dashboard import dashboard_router
from src.api.activity import activity_router
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from src.migrations import run_migrations
from src.api.routes import router
from src.api.draft import draft_router
from src.api.players import player_router
from src.api.backtest import backtest_router
from src.api.roster import roster_router
from src.api.waiver import waiver_router
from src.api.matchup import matchup_router, scoreboard_router
from src.api.league import league_router
from src.api.stats import stats_router
from src.api.trade import trade_router
from src.api.chat import chat_router
from src.api.rumor_action import rumor_action_router
from src.api.mcp_server import mcp, mcp_asgi_app
from src.telemetry import configure_tracing, configure_trace_logging

configure_tracing("fantasy-core-api")
configure_trace_logging()

logger = logging.getLogger("GatewayEngine")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Triggers self-healing database migrations on pod initialization before the entry
    controller starts accepting cluster traffic, then enters the MCP session manager's
    own lifespan. The latter is required, not optional: mcp.streamable_http_app() is a
    Starlette sub-app whose lifespan is never invoked just by app.mount()-ing it - Mount
    doesn't propagate a child app's lifespan into the parent's. Without entering
    session_manager.run() here, every authenticated MCP request fails with "Task group
    is not initialized" the moment it reaches the transport, even though the auth
    middleware layer above it (which doesn't need the session manager) works fine.
    """
    logger.info("Initializing gateway control plane partitions...")
    try:
        run_migrations()
        logger.info("Gateway cluster interfaces verified active.")
    except Exception as e:
        logger.critical(
            f"Gateway failed to bootstrap due to database migration panic: {e}"
        )
        raise
    stop = asyncio.Event()
    task = asyncio.create_task(run_activity(stop))
    try:
        async with mcp.session_manager.run():
            yield
    finally:
        stop.set()
        await task


app = FastAPI(
    title="Fantasy Automation API Gateway",
    description="Central control plane and asymmetric message translator",
    version="1.0.0",
    lifespan=lifespan,
)


@app.middleware("http")
async def catch_exceptions_middleware(request: Request, call_next):
    """
    Global exception barrier ensuring unhandled runtime panics are intercepted,
    formatted as valid JSON bodies, and returned as HTTP 500 without crashing threads.
    """
    try:
        return await call_next(request)
    except Exception as e:
        logger.error(
            f"HTTP Gateway intercepted unhandled thread exception: {e}", exc_info=True
        )
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": "Internal control plane server panic.",
                "details": str(e),
            },
        )


# Bind the unified API route configurations
app.include_router(dashboard_router, prefix="/v1")
app.include_router(activity_router, prefix="/v1/activity")
app.include_router(router, prefix="/v1")
app.include_router(draft_router, prefix="/v1/draft")
app.include_router(player_router, prefix="/v1/players")
app.include_router(backtest_router, prefix="/v1/backtest")
app.include_router(roster_router, prefix="/v1/roster")
app.include_router(waiver_router, prefix="/v1/waiver")
app.include_router(matchup_router, prefix="/v1/matchup")
app.include_router(scoreboard_router, prefix="/v1/scoreboard")
app.include_router(league_router, prefix="/v1/league")
app.include_router(stats_router, prefix="/v1/stats")
app.include_router(trade_router, prefix="/v1/trade")
app.include_router(chat_router, prefix="/v1/chat")
app.include_router(rumor_action_router, prefix="/v1")

# Mount the MCP (Model Context Protocol) server (CORE-07) - its own routes already carry
# absolute paths (/mcp, /.well-known/oauth-protected-resource/mcp), so it's mounted at
# the app root rather than under a prefix. Registered after include_router() so /v1/*
# still resolves first; Starlette tries routes in registration order.
app.mount("/", mcp_asgi_app)

# Instrumented last, after every route/mount is registered (CORE-12) - instrument_app()
# wraps the already-assembled ASGI middleware stack, and instrumenting before the /mcp
# mount above risks that sub-app's requests showing up as an unlabeled catch-all span
# instead of getting accurate http.route templating.
FastAPIInstrumentor.instrument_app(app)
