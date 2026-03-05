from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pymongo.errors import OperationFailure

from app.api.admin import router as admin_router
from app.api.auth import router as auth_router
from app.api.agent import router as agent_router
from app.api.customer import router as customer_router
from app.api.supervisor import router as supervisor_router
from app.core.settings import settings
from app.db.mongo import get_db
from pathlib import Path

app = FastAPI(title="nigelec-backend")

UPLOADS_ROOT = Path("uploads")
UPLOADS_ROOT.mkdir(parents=True, exist_ok=True)
app.mount("/uploads", StaticFiles(directory=str(UPLOADS_ROOT)), name="uploads")

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_origin_regex=settings.CORS_ORIGIN_REGEX,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup() -> None:
    db = get_db()

    async def _create_or_replace_index(collection_name: str, **kwargs) -> None:
        try:
            await db[collection_name].create_index(**kwargs)
        except OperationFailure as e:
            if e.codeName not in {"IndexOptionsConflict", "IndexKeySpecsConflict"}:
                raise
            name = kwargs.get("name")
            if not name:
                raise
            await db[collection_name].drop_index(name)
            await db[collection_name].create_index(**kwargs)

    await _create_or_replace_index("users", keys="phone", unique=True, name="phone_1")
    await _create_or_replace_index(
        "users",
        keys="meterNumber",
        unique=True,
        name="customer_meterNumber_unique",
        partialFilterExpression={"role": "customer", "meterNumber": {"$exists": True}},
    )
    await _create_or_replace_index(
        "users",
        keys=[("subscriberNumber", 1), ("police", 1)],
        unique=True,
        name="customer_subscriber_police_unique",
        partialFilterExpression={
            "role": "customer",
            "subscriberNumber": {"$exists": True},
            "police": {"$exists": True},
        },
    )

    await _create_or_replace_index(
        "meters",
        keys=[("cycleId", 1), ("meterNumber", 1)],
        unique=True,
        name="meter_cycle_meterNumber_unique",
    )
    await _create_or_replace_index(
        "meters",
        keys=[("cycleId", 1), ("center", 1), ("zone", 1), ("sector", 1), ("routeOrder", 1)],
        name="meter_cycle_center_zone_sector_routeOrder",
    )
    await _create_or_replace_index(
        "meters",
        keys=[("cycleId", 1), ("center", 1), ("zone", 1), ("sector", 1)],
        name="meter_cycle_center_zone_sector",
    )

    await _create_or_replace_index(
        "zones",
        keys=[("center", 1), ("zone", 1), ("sector", 1)],
        unique=True,
        name="zone_center_zone_sector_unique",
    )

    await _create_or_replace_index(
        "billing_cycles",
        keys="cycleId",
        unique=True,
        name="billing_cycle_id_unique",
    )
    await _create_or_replace_index(
        "billing_cycles",
        keys=[("status", 1), ("cycleId", -1)],
        name="billing_cycle_status_cycleId",
    )

    await _create_or_replace_index(
        "tours",
        keys=[("cycleId", 1), ("agentId", 1), ("date", 1)],
        name="tour_cycle_agent_date",
    )
    await _create_or_replace_index(
        "tours",
        keys=[("cycleId", 1), ("center", 1), ("zone", 1), ("sector", 1), ("date", 1)],
        name="tour_cycle_center_zone_sector_date",
    )
    await _create_or_replace_index(
        "tours",
        keys=[("cycleId", 1), ("date", 1), ("items.meterNumber", 1)],
        unique=True,
        name="tour_unique_cycle_meter_per_date",
    )

    await _create_or_replace_index(
        "readings",
        keys=[("cycleId", 1), ("meterNumber", 1)],
        unique=True,
        name="reading_unique_cycle_meter",
    )
    await _create_or_replace_index(
        "readings",
        keys=[("cycleId", 1), ("agentId", 1), ("date", 1)],
        name="reading_cycle_agent_date",
    )

    await _create_or_replace_index(
        "invoices",
        keys="invoiceId",
        unique=True,
        name="invoice_invoiceId_unique",
    )
    await _create_or_replace_index(
        "invoices",
        keys=[("cycleId", 1), ("readingId", 1)],
        unique=True,
        name="invoice_cycle_reading_unique",
    )
    await _create_or_replace_index(
        "invoices",
        keys=[("customerId", 1), ("status", 1), ("dueDate", 1)],
        name="invoice_customer_status_dueDate",
    )
    await _create_or_replace_index(
        "invoices",
        keys=[("center", 1), ("zone", 1), ("sector", 1), ("status", 1), ("dueDate", 1)],
        name="invoice_zone_status_dueDate",
    )


@app.get("/health")
def health():
    return {"status": "ok"}


app.include_router(auth_router)
app.include_router(admin_router)
app.include_router(supervisor_router)
app.include_router(agent_router)
app.include_router(customer_router)