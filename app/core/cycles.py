from datetime import date, datetime, timezone

from fastapi import HTTPException, status
from motor.motor_asyncio import AsyncIOMotorDatabase


def cycle_id_from_date(value: str) -> str:
    try:
        parsed = date.fromisoformat(str(value))
    except Exception:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Date invalide (format attendu YYYY-MM-DD).")
    return f"{parsed.year:04d}-{parsed.month:02d}"


def current_cycle_id() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.year:04d}-{now.month:02d}"


async def ensure_cycle_open(db: AsyncIOMotorDatabase, cycle_id: str) -> None:
    normalized = str(cycle_id).strip()
    if len(normalized) != 7 or normalized[4] != "-":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="cycleId invalide (format attendu YYYY-MM).")

    now = datetime.now(timezone.utc)
    await db.billing_cycles.update_one(
        {"cycleId": normalized},
        {
            "$setOnInsert": {
                "cycleId": normalized,
                "status": "OPEN",
                "openedAt": now,
                "createdAt": now,
            },
            "$set": {"updatedAt": now},
        },
        upsert=True,
    )

    cycle = await db.billing_cycles.find_one({"cycleId": normalized}, {"status": 1})
    if not cycle or str(cycle.get("status") or "OPEN").upper() != "OPEN":
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Cycle {normalized} est clôturé.")


async def get_active_cycle_id(db: AsyncIOMotorDatabase) -> str:
    cycle = await db.billing_cycles.find_one({"status": "OPEN"}, sort=[("cycleId", -1)], projection={"cycleId": 1})
    if cycle and isinstance(cycle.get("cycleId"), str) and cycle.get("cycleId"):
        return str(cycle.get("cycleId"))

    fallback = current_cycle_id()
    await ensure_cycle_open(db, fallback)
    return fallback


async def resolve_cycle_id(db: AsyncIOMotorDatabase, *, date_value: str | None = None, cycle_id: str | None = None) -> str:
    if isinstance(cycle_id, str) and cycle_id.strip():
        resolved = cycle_id.strip()
    elif isinstance(date_value, str) and date_value.strip():
        resolved = cycle_id_from_date(date_value)
    else:
        resolved = await get_active_cycle_id(db)

    await ensure_cycle_open(db, resolved)
    return resolved
