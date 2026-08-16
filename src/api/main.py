import logging
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from src.migrations import run_migrations
from src.api.routes import router
from src.api.mcp_server import mcp_asgi_app

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("GatewayEngine")

app = FastAPI(
    title="Fantasy Automation API Gateway",
    description="Central control plane and asymmetric message translator",
    version="1.0.0"
)

@app.on_event("startup")
async def startup_event():
    """
    Triggers self-healing database migrations on pod initialization 
    before the entry controller starts accepting cluster traffic.
    """
    logger.info("Initializing gateway control plane partitions...")
    try:
        run_migrations()
        logger.info("Gateway cluster interfaces verified active.")
    except Exception as e:
        logger.critical(f"Gateway failed to bootstrap due to database migration panic: {e}")
        raise

@app.middleware("http")
async def catch_exceptions_middleware(request: Request, call_next):
    """
    Global exception barrier ensuring unhandled runtime panics are intercepted,
    formatted as valid JSON bodies, and returned as HTTP 500 without crashing threads.
    """
    try:
        return await call_next(request)
    except Exception as e:
        logger.error(f"HTTP Gateway intercepted unhandled thread exception: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": "Internal control plane server panic.", "details": str(e)}
        )

# Bind the unified API route configurations
app.include_router(router, prefix="/v1")

# Mount the MCP (Model Context Protocol) server (CORE-07) - its own routes already carry
# absolute paths (/mcp, /.well-known/oauth-protected-resource/mcp), so it's mounted at
# the app root rather than under a prefix. Registered after include_router() so /v1/*
# still resolves first; Starlette tries routes in registration order.
app.mount("/", mcp_asgi_app)

