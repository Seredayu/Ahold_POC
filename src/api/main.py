from datetime import datetime

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .routers import exceptions, orders

app = FastAPI(title="Ahold Delhaize Freshness POC API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(exceptions.router, prefix="/exceptions", tags=["exceptions"])
app.include_router(orders.router, prefix="/orders", tags=["orders"])


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat() + "Z"}
