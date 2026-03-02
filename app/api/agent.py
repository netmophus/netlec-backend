from datetime import date, datetime, timedelta, timezone
import calendar

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query, status
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.api.models import AgentReadingSummaryItem, CreateReadingRequest, ReadingPublic, TourPublic, UpdateReadingRequest
from app.core.deps import get_current_user_payload, get_database, require_roles

router = APIRouter(prefix="/agent", tags=["agent"])


def _tariff_rate_per_kwh(tariff_code: str | None) -> int | None:
    if not tariff_code:
        return None
    code = str(tariff_code).strip().upper()
    if code == "T1":
        return 50
    if code == "T2":
        return 75
    if code == "T3":
        return 100
    return None


async def _load_tariff_tiers_db(db: AsyncIOMotorDatabase) -> list[dict]:
    docs = await db.tariffs.find({}).sort([("fromKwh", 1), ("code", 1)]).to_list(length=50)
    tiers: list[dict] = []
    for d in docs:
        rk = d.get("ratePerKwh")
        fk = d.get("fromKwh")
        tk = d.get("toKwh")
        if not isinstance(rk, int) or not isinstance(fk, int):
            continue
        to_kwh: int | None = None
        if isinstance(tk, int):
            to_kwh = int(tk)
        tiers.append({"fromKwh": int(fk), "toKwh": to_kwh, "ratePerKwh": int(rk)})
    tiers.sort(key=lambda x: int(x.get("fromKwh") or 0))
    return tiers


def _compute_progressive_amount(consumption: int, tiers: list[dict]) -> int | None:
    if not isinstance(consumption, int) or consumption < 0:
        return None
    if consumption == 0:
        return 0
    if not tiers:
        return None

    amount = 0
    for t in tiers:
        try:
            start = int(t.get("fromKwh"))
        except Exception:
            continue
        end_raw = t.get("toKwh")
        end: int | None
        if end_raw is None:
            end = None
        else:
            try:
                end = int(end_raw)
            except Exception:
                end = None

        rate = t.get("ratePerKwh")
        if not isinstance(rate, int) or rate < 0:
            continue

        low = max(1, start)
        high = consumption if end is None else min(consumption, end)
        if high < low:
            continue

        kwh_in_tier = int(high - low + 1)
        amount += int(kwh_in_tier * rate)
        if end is None:
            break
    return int(amount)


async def _tariff_rate_per_kwh_db(db: AsyncIOMotorDatabase, tariff_code: str | None) -> int | None:
    if not tariff_code:
        return None
    code = str(tariff_code).strip().upper()
    doc = await db.tariffs.find_one({"code": code}, {"ratePerKwh": 1})
    if doc and isinstance(doc.get("ratePerKwh"), int):
        return int(doc.get("ratePerKwh"))
    return _tariff_rate_per_kwh(code)


def _end_of_month_due_date(reading_date_iso: str, grace_days: int) -> date | None:
    try:
        rd_d = date.fromisoformat(str(reading_date_iso))
    except Exception:
        return None
    last_day = calendar.monthrange(rd_d.year, rd_d.month)[1]
    return date(rd_d.year, rd_d.month, last_day) + timedelta(days=int(grace_days))


@router.get(
    "/tours",
    response_model=list[TourPublic],
    dependencies=[Depends(require_roles("agent"))],
)
async def list_agent_tours(
    token_payload: dict = Depends(get_current_user_payload),
    db: AsyncIOMotorDatabase = Depends(get_database),
    date: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
):
    agent_id = token_payload.get("sub")
    if not agent_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    try:
        agent_oid = ObjectId(str(agent_id))
    except Exception:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    agent = await db.users.find_one({"_id": agent_oid})
    if not agent:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Utilisateur introuvable.")

    query: dict = {"agentId": str(agent["_id"])}
    if date:
        query["date"] = date

    cursor = db.tours.find(query).sort([("date", -1), ("createdAt", -1)]).limit(limit)
    docs = await cursor.to_list(length=limit)

    meter_numbers: set[str] = set()
    for d in docs:
        for it in d.get("items") or []:
            mn = it.get("meterNumber")
            if mn:
                meter_numbers.add(str(mn))

    old_index_by_meter: dict[str, int | None] = {}
    if meter_numbers:
        cursor2 = db.users.find(
            {"role": "customer", "meterNumber": {"$in": list(meter_numbers)}},
            {"meterNumber": 1, "oldIndex": 1},
        )
        async for u in cursor2:
            mn = u.get("meterNumber")
            if mn:
                old_index_by_meter[str(mn)] = u.get("oldIndex")

    out: list[TourPublic] = []
    for d in docs:
        d["_id"] = str(d["_id"])
        items = d.get("items") or []
        for it in items:
            mn = it.get("meterNumber")
            if mn:
                it["oldIndex"] = old_index_by_meter.get(str(mn))
        out.append(TourPublic(**d))

    return out


@router.post(
    "/readings",
    response_model=ReadingPublic,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_roles("agent"))],
)
async def create_reading(
    payload: CreateReadingRequest,
    token_payload: dict = Depends(get_current_user_payload),
    db: AsyncIOMotorDatabase = Depends(get_database),
):
    agent_id = token_payload.get("sub")
    if not agent_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    try:
        agent_oid = ObjectId(str(agent_id))
    except Exception:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    agent = await db.users.find_one({"_id": agent_oid})
    if not agent:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Utilisateur introuvable.")

    tour = await db.tours.find_one({"_id": ObjectId(payload.tourId)})
    if not tour:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tournée introuvable.")
    if str(tour.get("agentId")) != str(agent.get("_id")):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Tournée non autorisée.")
    if tour.get("date") != payload.date:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Date incohérente avec la tournée.")

    in_tour = any((it.get("meterNumber") == payload.meterNumber) for it in (tour.get("items") or []))
    if not in_tour:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Compteur non présent dans la tournée.")

    customer = await db.users.find_one({"role": "customer", "meterNumber": payload.meterNumber})
    if not customer:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Client introuvable pour ce compteur.")

    old_index = customer.get("oldIndex")
    consumption: int | None = None
    if isinstance(old_index, int):
        if payload.newIndex < old_index:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Nouvel index inférieur à l'ancien index.")
        consumption = payload.newIndex - old_index

    now = datetime.now(timezone.utc)
    doc = {
        "date": payload.date,
        "tourId": payload.tourId,
        "agentId": str(agent.get("_id")),
        "meterNumber": payload.meterNumber,
        "oldIndex": old_index,
        "newIndex": payload.newIndex,
        "consumption": consumption,
        "tariffCode": customer.get("tariffCode"),
        "gps": payload.gps,
        "gpsMissing": payload.gpsMissing,
        "gpsMissingReason": payload.gpsMissingReason,
        "createdAt": now,
        "updatedAt": now,
    }

    existing = await db.readings.find_one({"date": payload.date, "meterNumber": payload.meterNumber})
    if existing:
        existing["_id"] = str(existing["_id"])
        return ReadingPublic(**existing)

    res = await db.readings.insert_one(doc)
    created = await db.readings.find_one({"_id": res.inserted_id})
    if not created:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erreur création relevé.")

    await db.users.update_one({"_id": customer["_id"]}, {"$set": {"oldIndex": payload.newIndex, "updatedAt": now}})

    grace_days = 10
    due_d = _end_of_month_due_date(payload.date, grace_days)
    due_date_str = due_d.isoformat() if due_d else None
    invoice_id = f"INV-{str(res.inserted_id)}"

    amount: int | None = None
    cons = created.get("consumption")
    tc = created.get("tariffCode") or customer.get("tariffCode")
    if isinstance(cons, int):
        tiers = await _load_tariff_tiers_db(db)
        amount = _compute_progressive_amount(int(cons), tiers)
        if amount is None:
            rate = await _tariff_rate_per_kwh_db(db, tc)
            if rate is not None:
                amount = int(int(cons) * int(rate))

    status_str = "DUE"
    if due_d is not None and now.date() > due_d:
        status_str = "OVERDUE"

    await db.invoices.update_one(
        {"invoiceId": invoice_id},
        {
            "$set": {
                "invoiceId": invoice_id,
                "readingId": str(res.inserted_id),
                "customerId": str(customer.get("_id")),
                "meterNumber": str(payload.meterNumber),
                "period": str(payload.date)[:7] if len(str(payload.date)) >= 7 else str(payload.date),
                "date": str(payload.date),
                "dueDate": due_date_str,
                "tariffCode": str(tc) if tc is not None else None,
                "consumption": cons if isinstance(cons, int) else None,
                "amount": amount,
                "status": status_str,
                "center": customer.get("center"),
                "zone": customer.get("zone"),
                "sector": customer.get("sector"),
                "updatedAt": now,
            },
            "$setOnInsert": {"createdAt": now},
        },
        upsert=True,
    )

    created["_id"] = str(created["_id"])
    return ReadingPublic(**created)


@router.get(
    "/readings",
    response_model=list[ReadingPublic],
    dependencies=[Depends(require_roles("agent"))],
)
async def list_agent_readings(
    token_payload: dict = Depends(get_current_user_payload),
    db: AsyncIOMotorDatabase = Depends(get_database),
    date: str | None = Query(default=None),
    tourId: str | None = Query(default=None),
    meterNumber: str | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=1000),
):
    agent_id = token_payload.get("sub")
    if not agent_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    try:
        agent_oid = ObjectId(str(agent_id))
    except Exception:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    agent = await db.users.find_one({"_id": agent_oid})
    if not agent:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Utilisateur introuvable.")

    q: dict = {"agentId": str(agent.get("_id"))}
    if date:
        q["date"] = str(date)
    if tourId:
        q["tourId"] = str(tourId)
    if meterNumber:
        q["meterNumber"] = str(meterNumber)

    cursor = db.readings.find(q).sort([("date", -1), ("createdAt", -1)]).limit(limit)
    docs = await cursor.to_list(length=limit)
    out: list[ReadingPublic] = []
    for d in docs:
        d["_id"] = str(d["_id"])
        out.append(ReadingPublic(**d))
    return out


@router.patch(
    "/readings/{reading_id}",
    response_model=ReadingPublic,
    dependencies=[Depends(require_roles("agent"))],
)
async def update_agent_reading(
    reading_id: str,
    payload: UpdateReadingRequest,
    token_payload: dict = Depends(get_current_user_payload),
    db: AsyncIOMotorDatabase = Depends(get_database),
):
    agent_id = token_payload.get("sub")
    if not agent_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    try:
        agent_oid = ObjectId(str(agent_id))
    except Exception:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    agent = await db.users.find_one({"_id": agent_oid})
    if not agent:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Utilisateur introuvable.")

    try:
        roid = ObjectId(str(reading_id))
    except Exception:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Identifiant invalide.")

    reading = await db.readings.find_one({"_id": roid})
    if not reading:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Relevé introuvable.")

    if str(reading.get("agentId")) != str(agent.get("_id")):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Relevé non autorisé.")

    meter_number = str(reading.get("meterNumber"))
    reading_date = str(reading.get("date"))

    customer = await db.users.find_one({"role": "customer", "meterNumber": meter_number})
    if not customer:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Client introuvable pour ce compteur.")

    old_index = reading.get("oldIndex")
    if isinstance(old_index, int) and payload.newIndex < old_index:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Nouvel index inférieur à l'ancien index.")

    consumption: int | None = None
    if isinstance(old_index, int):
        consumption = int(payload.newIndex - old_index)

    now = datetime.now(timezone.utc)
    update_doc: dict = {
        "newIndex": payload.newIndex,
        "consumption": consumption,
        "updatedAt": now,
    }
    if payload.gps is not None:
        update_doc["gps"] = payload.gps
    if payload.gpsMissing is not None:
        update_doc["gpsMissing"] = payload.gpsMissing
    if payload.gpsMissingReason is not None:
        update_doc["gpsMissingReason"] = payload.gpsMissingReason

    await db.readings.update_one({"_id": roid}, {"$set": update_doc})

    updated = await db.readings.find_one({"_id": roid})
    if not updated:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erreur mise à jour relevé.")

    # Update customer's oldIndex only if this reading is the latest for this meter.
    # This avoids overwriting a newer index if multiple readings exist for the same meter.
    latest = await db.readings.find_one(
        {"meterNumber": meter_number},
        sort=[("date", -1), ("createdAt", -1)],
        projection={"_id": 1},
    )
    if latest and str(latest.get("_id")) == str(roid):
        await db.users.update_one({"_id": customer["_id"]}, {"$set": {"oldIndex": payload.newIndex, "updatedAt": now}})

    invoice_id = f"INV-{str(roid)}"
    amount: int | None = None
    tc = updated.get("tariffCode") or customer.get("tariffCode")
    cons = updated.get("consumption")
    if isinstance(cons, int):
        tiers = await _load_tariff_tiers_db(db)
        amount = _compute_progressive_amount(int(cons), tiers)
        if amount is None:
            rate = await _tariff_rate_per_kwh_db(db, tc)
            if rate is not None:
                amount = int(int(cons) * int(rate))

    grace_days = 10
    due_d = _end_of_month_due_date(reading_date, grace_days)
    due_date_str = due_d.isoformat() if due_d else None

    status_str = "DUE"
    if due_d is not None and now.date() > due_d:
        status_str = "OVERDUE"

    await db.invoices.update_one(
        {"invoiceId": invoice_id},
        {
            "$set": {
                "invoiceId": invoice_id,
                "readingId": str(roid),
                "customerId": str(customer.get("_id")),
                "meterNumber": meter_number,
                "period": str(reading_date)[:7] if len(str(reading_date)) >= 7 else str(reading_date),
                "date": str(reading_date),
                "dueDate": due_date_str,
                "tariffCode": str(tc) if tc is not None else None,
                "consumption": cons if isinstance(cons, int) else None,
                "amount": amount,
                "status": status_str,
                "center": customer.get("center"),
                "zone": customer.get("zone"),
                "sector": customer.get("sector"),
                "updatedAt": now,
            },
            "$setOnInsert": {"createdAt": now},
        },
        upsert=True,
    )

    updated["_id"] = str(updated["_id"])
    return ReadingPublic(**updated)


@router.get(
    "/readings/summary",
    response_model=list[AgentReadingSummaryItem],
    dependencies=[Depends(require_roles("agent"))],
)
async def list_agent_readings_summary(
    token_payload: dict = Depends(get_current_user_payload),
    db: AsyncIOMotorDatabase = Depends(get_database),
    date: str | None = Query(default=None),
    tourId: str | None = Query(default=None),
    limit: int = Query(default=5000, ge=1, le=20000),
):
    agent_id = token_payload.get("sub")
    if not agent_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    try:
        agent_oid = ObjectId(str(agent_id))
    except Exception:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    agent = await db.users.find_one({"_id": agent_oid})
    if not agent:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Utilisateur introuvable.")

    q: dict = {"agentId": str(agent.get("_id"))}
    if date:
        q["date"] = str(date)
    if tourId:
        q["tourId"] = str(tourId)

    cursor = db.readings.find(q, {"tourId": 1, "meterNumber": 1, "date": 1}).limit(limit)
    out: list[AgentReadingSummaryItem] = []
    async for d in cursor:
        out.append(
            AgentReadingSummaryItem(
                tourId=str(d.get("tourId")),
                meterNumber=str(d.get("meterNumber")),
                date=str(d.get("date")),
            )
        )
    return out
