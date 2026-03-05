from datetime import datetime, timezone
import io
import unicodedata

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query, status
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.errors import DuplicateKeyError
from starlette.responses import StreamingResponse

from app.api.agent import (
    _build_invoice_detail,
    _compute_progressive_amount,
    _end_of_month_due_date,
    _infer_tariff_code_from_consumption,
    _load_tariff_tiers_db,
    _tariff_rate_per_kwh_db,
)
from app.api.models import (
    CreateAgentBySupervisorRequest,
    GenerateToursRequest,
    GenerateToursResponse,
    MeterPublic,
    ReadingPublic,
    ReadingWithLocationPublic,
    ReviewReadingCorrectionPayload,
    TourItem,
    TourPublic,
    UserPublic,
    ZoneRef,
)
from app.core.cycles import resolve_cycle_id
from app.core.deps import get_current_user_payload, get_database, require_roles
from app.core.security import hash_password

router = APIRouter(prefix="/supervisor", tags=["supervisor"])


def _ascii(value: object) -> str:
    if value is None:
        return ""
    s = str(value)
    s = unicodedata.normalize("NFKD", s)
    s = s.encode("ascii", "ignore").decode("ascii")
    return s


async def _get_supervisor_context(token_payload: dict, db: AsyncIOMotorDatabase) -> tuple[dict, list[dict]]:
    supervisor_id = token_payload.get("sub")
    if not supervisor_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    try:
        supervisor_oid = ObjectId(str(supervisor_id))
    except Exception:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    supervisor = await db.users.find_one({"_id": supervisor_oid})
    if not supervisor:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Utilisateur introuvable.")

    assigned_zones = supervisor.get("assignedZones") or []
    zones_or = _zones_or_query(assigned_zones, include_sector_none=True)
    return supervisor, zones_or


def _pdf_canvas_or_500():
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.units import mm
        from reportlab.pdfgen import canvas
    except ModuleNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Dépendance PDF manquante (reportlab).",
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erreur import reportlab: {type(e).__name__}: {e}",
        )
    return A4, mm, canvas


def _zones_or_query(assigned_zones: list[dict], include_sector_none: bool) -> list[dict]:
    zones_or: list[dict] = []
    for z in assigned_zones:
        center = z.get("center")
        zone = z.get("zone")
        sector = z.get("sector")
        if not center or not zone:
            continue

        if sector:
            if include_sector_none:
                zones_or.append({"center": center, "zone": zone, "$or": [{"sector": sector}, {"sector": None}]})
            else:
                zones_or.append({"center": center, "zone": zone, "sector": sector})
        else:
            zones_or.append({"center": center, "zone": zone})
    return zones_or


def _normalize_zone_refs(items: list[dict]) -> list[dict]:
    out: list[dict] = []
    seen: set[tuple[str, str, str]] = set()
    for z in items:
        center = str(z.get("center") or "").strip()
        zone = str(z.get("zone") or "").strip()
        sector = str(z.get("sector") or "").strip()
        if not center or not zone or not sector:
            continue
        key = (center, zone, sector)
        if key in seen:
            continue
        seen.add(key)
        out.append({"center": center, "zone": zone, "sector": sector})
    out.sort(key=lambda x: (x["center"], x["zone"], x["sector"]))
    return out


@router.patch(
    "/readings/{reading_id}/correction-review",
    response_model=ReadingPublic,
    dependencies=[Depends(require_roles("supervisor"))],
)
async def review_reading_correction(
    reading_id: str,
    payload: ReviewReadingCorrectionPayload,
    token_payload: dict = Depends(get_current_user_payload),
    db: AsyncIOMotorDatabase = Depends(get_database),
):
    supervisor, zones_or = await _get_supervisor_context(token_payload, db)
    if not zones_or:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Aucune zone affectée.")

    try:
        roid = ObjectId(str(reading_id))
    except Exception:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Identifiant invalide.")

    reading = await db.readings.find_one({"_id": roid})
    if not reading:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Relevé introuvable.")

    if str(reading.get("source") or "AGENT").upper() != "AGENT":
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Correction réservée aux relevés agent.")

    if str(reading.get("correctionStatus") or "NONE").upper() != "PENDING_SUPERVISOR":
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Aucune correction en attente pour ce relevé.")

    tour_id_raw = reading.get("tourId")
    try:
        tour_oid = ObjectId(str(tour_id_raw))
    except Exception:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Tournée introuvable pour ce relevé.")

    tour = await db.tours.find_one({"_id": tour_oid, "$or": zones_or}, {"_id": 1, "center": 1, "zone": 1, "sector": 1})
    if not tour:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Relevé hors de vos zones supervisées.")

    cycle_id_raw = reading.get("cycleId")
    if isinstance(cycle_id_raw, str) and cycle_id_raw.strip():
        cycle_id = cycle_id_raw.strip()
    else:
        cycle_id = await resolve_cycle_id(db, date_value=str(reading.get("date")))

    now = datetime.now(timezone.utc)
    review_note = payload.note.strip() if isinstance(payload.note, str) and payload.note.strip() else None

    if not payload.approve:
        await db.readings.update_one(
            {"_id": roid},
            {
                "$set": {
                    "correctionStatus": "REJECTED",
                    "correctionReviewedBy": str(supervisor.get("_id")),
                    "correctionReviewedAt": now,
                    "correctionReviewNote": review_note,
                    "updatedAt": now,
                },
                "$push": {
                    "correctionAudit": {
                        "action": "REJECTED",
                        "reviewedBy": str(supervisor.get("_id")),
                        "reviewedAt": now,
                        "note": review_note,
                        "proposedIndex": reading.get("correctionProposedIndex"),
                    }
                },
            },
        )
        rejected = await db.readings.find_one({"_id": roid})
        if not rejected:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erreur traitement correction.")
        rejected["_id"] = str(rejected["_id"])
        return ReadingPublic(**rejected)

    proposed_index = reading.get("correctionProposedIndex")
    if not isinstance(proposed_index, int):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Index proposé invalide.")

    old_index = reading.get("oldIndex")
    if isinstance(old_index, int) and int(proposed_index) < old_index:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Index proposé inférieur à l'ancien index.")

    meter_number = str(reading.get("meterNumber") or "")
    if not meter_number:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Compteur invalide sur le relevé.")

    customer = await db.users.find_one({"role": "customer", "meterNumber": meter_number})
    if not customer:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Client introuvable pour ce compteur.")

    consumption: int | None = None
    if isinstance(old_index, int):
        consumption = int(proposed_index - old_index)

    tiers = await _load_tariff_tiers_db(db)
    tariff_code: str | None = None
    reading_tariff = reading.get("tariffCode")
    customer_tariff = customer.get("tariffCode")
    raw_tariff = reading_tariff if isinstance(reading_tariff, str) and reading_tariff.strip() else customer_tariff
    if isinstance(raw_tariff, str) and raw_tariff.strip():
        tariff_code = raw_tariff.strip().upper()
    if not tariff_code and isinstance(consumption, int):
        tariff_code = _infer_tariff_code_from_consumption(int(consumption), tiers)

    update_doc: dict = {
        "newIndex": int(proposed_index),
        "consumption": consumption,
        "updatedAt": now,
        "correctionStatus": "APPROVED",
        "correctionReviewedBy": str(supervisor.get("_id")),
        "correctionReviewedAt": now,
        "correctionReviewNote": review_note,
    }
    if tariff_code is not None:
        update_doc["tariffCode"] = tariff_code

    await db.readings.update_one(
        {"_id": roid},
        {
            "$set": update_doc,
            "$push": {
                "correctionAudit": {
                    "action": "APPROVED",
                    "reviewedBy": str(supervisor.get("_id")),
                    "reviewedAt": now,
                    "oldIndex": reading.get("newIndex"),
                    "approvedIndex": int(proposed_index),
                    "reason": reading.get("correctionReason"),
                    "note": review_note,
                }
            },
        },
    )

    latest = await db.readings.find_one(
        {"cycleId": cycle_id, "meterNumber": meter_number},
        sort=[("date", -1), ("createdAt", -1)],
        projection={"_id": 1},
    )
    if latest and str(latest.get("_id")) == str(roid):
        await db.users.update_one({"_id": customer["_id"]}, {"$set": {"oldIndex": int(proposed_index), "updatedAt": now}})

    tc = tariff_code
    amount: int | None = None
    if isinstance(consumption, int):
        amount = _compute_progressive_amount(int(consumption), tiers)
        if amount is None:
            rate = await _tariff_rate_per_kwh_db(db, tc)
            if rate is not None:
                amount = int(int(consumption) * int(rate))

    detail = _build_invoice_detail(consumption if isinstance(consumption, int) else None, tiers, amount)
    due_d = _end_of_month_due_date(str(reading.get("date")), 10)
    due_date_str = due_d.isoformat() if due_d else None
    status_str = "DUE"
    if due_d is not None and now.date() > due_d:
        status_str = "OVERDUE"

    invoice_id = f"INV-{str(roid)}"
    await db.invoices.update_one(
        {"invoiceId": invoice_id},
        {
            "$set": {
                "invoiceId": invoice_id,
                "cycleId": cycle_id,
                "readingId": str(roid),
                "customerId": str(customer.get("_id")),
                "meterNumber": meter_number,
                "period": str(reading.get("date"))[:7] if len(str(reading.get("date"))) >= 7 else str(reading.get("date")),
                "date": str(reading.get("date")),
                "dueDate": due_date_str,
                "tariffCode": tc,
                "consumption": consumption if isinstance(consumption, int) else None,
                "amount": detail["amount"],
                "energyAmount": detail["energyAmount"],
                "tvFee": detail["tvFee"],
                "fsspFee": detail["fsspFee"],
                "subtotal": detail["subtotal"],
                "taxAmount": detail["taxAmount"],
                "totalAmount": detail["totalAmount"],
                "breakdown": detail["breakdown"],
                "status": status_str,
                "center": tour.get("center") or customer.get("center"),
                "zone": tour.get("zone") or customer.get("zone"),
                "sector": tour.get("sector") or customer.get("sector"),
                "updatedAt": now,
            },
            "$setOnInsert": {"createdAt": now},
        },
        upsert=True,
    )

    approved = await db.readings.find_one({"_id": roid})
    if not approved:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erreur validation correction.")
    approved["_id"] = str(approved["_id"])
    return ReadingPublic(**approved)


@router.get(
    "/zones",
    response_model=list[ZoneRef],
    dependencies=[Depends(require_roles("supervisor"))],
)
async def list_supervisor_zones(
    token_payload: dict = Depends(get_current_user_payload),
    db: AsyncIOMotorDatabase = Depends(get_database),
):
    await _get_supervisor_context(token_payload, db)

    zone_docs = await db.zones.find(
        {},
        {"center": 1, "zone": 1, "sector": 1},
    ).to_list(length=2000)

    normalized = _normalize_zone_refs(zone_docs)
    return normalized


@router.get(
    "/customers",
    response_model=list[UserPublic],
    dependencies=[Depends(require_roles("supervisor"))],
)
async def list_customers(
    token_payload: dict = Depends(get_current_user_payload),
    db: AsyncIOMotorDatabase = Depends(get_database),
    q: str | None = None,
    active: bool | None = None,
    limit: int = 500,
):
    supervisor_id = token_payload.get("sub")
    if not supervisor_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    try:
        supervisor_oid = ObjectId(str(supervisor_id))
    except Exception:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    supervisor = await db.users.find_one({"_id": supervisor_oid})
    if not supervisor:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Utilisateur introuvable.")

    assigned_zones = supervisor.get("assignedZones") or []
    zones_or = _zones_or_query(assigned_zones, include_sector_none=True)
    if not zones_or:
        return []

    query: dict = {"role": "customer", "$or": zones_or}
    if active is not None:
        query["isActive"] = active
    if q and q.strip():
        query["$and"] = [
            {"$or": zones_or},
            {
                "$or": [
                    {"phone": {"$regex": q.strip(), "$options": "i"}},
                    {"name": {"$regex": q.strip(), "$options": "i"}},
                    {"meterNumber": {"$regex": q.strip(), "$options": "i"}},
                ]
            },
        ]
        query.pop("$or", None)

    cursor = db.users.find(query, {"passwordHash": 0}).sort("createdAt", -1).limit(max(1, min(limit, 1000)))
    items = await cursor.to_list(length=max(1, min(limit, 1000)))
    for it in items:
        it["_id"] = str(it["_id"])
    return items


@router.get(
    "/readings",
    response_model=list[ReadingWithLocationPublic],
    dependencies=[Depends(require_roles("supervisor"))],
)
async def list_readings(
    token_payload: dict = Depends(get_current_user_payload),
    db: AsyncIOMotorDatabase = Depends(get_database),
    date: str | None = Query(default=None),
    agentId: str | None = Query(default=None),
    meterNumber: str | None = Query(default=None),
    correctionStatus: str | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=1000),
):
    supervisor_id = token_payload.get("sub")
    if not supervisor_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    try:
        supervisor_oid = ObjectId(str(supervisor_id))
    except Exception:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    supervisor = await db.users.find_one({"_id": supervisor_oid})
    if not supervisor:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Utilisateur introuvable.")

    assigned_zones = supervisor.get("assignedZones") or []
    zones_or = _zones_or_query(assigned_zones, include_sector_none=True)
    if not zones_or:
        return []

    cycle_id = await resolve_cycle_id(db, date_value=date)

    tours_query: dict = {"$or": zones_or, "cycleId": cycle_id}
    if date:
        tours_query["date"] = date
    tour_docs = await db.tours.find(tours_query, {"_id": 1, "center": 1, "zone": 1, "sector": 1}).to_list(length=5000)
    if not tour_docs:
        return []

    tour_id_to_loc: dict[str, dict] = {}
    tour_ids: list[str] = []
    for t in tour_docs:
        tid = str(t.get("_id"))
        tour_ids.append(tid)
        tour_id_to_loc[tid] = {"center": t.get("center"), "zone": t.get("zone"), "sector": t.get("sector")}

    readings_query: dict = {"tourId": {"$in": tour_ids}, "cycleId": cycle_id}
    if date:
        readings_query["date"] = date
    if agentId:
        readings_query["agentId"] = agentId
    if meterNumber:
        readings_query["meterNumber"] = meterNumber
    if correctionStatus and correctionStatus.strip():
        readings_query["correctionStatus"] = correctionStatus.strip().upper()

    cursor = db.readings.find(readings_query).sort([("date", -1), ("createdAt", -1)]).limit(limit)
    docs = await cursor.to_list(length=limit)

    meter_numbers: set[str] = set()
    for d in docs:
        mn = d.get("meterNumber")
        if mn:
            meter_numbers.add(str(mn))

    customers_by_meter: dict[str, dict] = {}
    if meter_numbers:
        cursor_c = db.users.find(
            {"role": "customer", "meterNumber": {"$in": list(meter_numbers)}},
            {"meterNumber": 1, "tariffCode": 1},
        )
        async for c in cursor_c:
            mn = c.get("meterNumber")
            if mn:
                customers_by_meter[str(mn)] = c

    tier_docs = (
        await db.tariffs.find({}, {"code": 1, "fromKwh": 1, "toKwh": 1, "ratePerKwh": 1})
        .sort([("fromKwh", 1), ("code", 1)])
        .to_list(length=50)
    )
    tiers: list[dict] = []
    for t in tier_docs:
        if not isinstance(t.get("fromKwh"), int):
            continue
        code = t.get("code")
        if not code:
            continue
        rk = t.get("ratePerKwh")
        if not isinstance(rk, int) or rk < 0:
            continue
        to_raw = t.get("toKwh")
        to_val: int | None = None
        if isinstance(to_raw, int):
            to_val = int(to_raw)
        tiers.append({"code": str(code), "fromKwh": int(t.get("fromKwh")), "toKwh": to_val, "ratePerKwh": int(rk)})

    def compute_progressive_amount(consumption: int) -> int | None:
        if not isinstance(consumption, int) or consumption < 0:
            return None
        if consumption == 0:
            return 0
        if not tiers:
            return None

        amount = 0
        for t in tiers:
            start = t.get("fromKwh")
            end = t.get("toKwh")
            rate = t.get("ratePerKwh")
            if not isinstance(start, int) or not isinstance(rate, int):
                continue
            low = max(1, start)
            high = consumption if end is None else min(consumption, int(end))
            if high < low:
                continue
            amount += int((high - low + 1) * rate)
            if end is None:
                break
        return int(amount)

    out: list[ReadingWithLocationPublic] = []
    for d in docs:
        d["_id"] = str(d["_id"])
        loc = tour_id_to_loc.get(str(d.get("tourId"))) or {}
        d["center"] = loc.get("center")
        d["zone"] = loc.get("zone")
        d["sector"] = loc.get("sector")

        if not d.get("tariffCode"):
            mn = d.get("meterNumber")
            if mn:
                cust = customers_by_meter.get(str(mn))
                if cust and cust.get("tariffCode"):
                    d["tariffCode"] = cust.get("tariffCode")

        if not d.get("tariffCode") and isinstance(d.get("consumption"), int) and tiers:
            cons = int(d.get("consumption"))
            for t in tiers:
                start = t.get("fromKwh")
                end = t.get("toKwh")
                if not isinstance(start, int):
                    continue
                if cons < start:
                    continue
                if end is not None and cons > int(end):
                    continue
                d["tariffCode"] = t.get("code")
                break

        if isinstance(d.get("consumption"), int):
            d["amount"] = compute_progressive_amount(int(d.get("consumption")))

        out.append(ReadingWithLocationPublic(**d))

    return out


@router.get(
    "/customers/report.pdf",
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(require_roles("supervisor"))],
)
async def supervisor_customers_report_pdf(
    token_payload: dict = Depends(get_current_user_payload),
    db: AsyncIOMotorDatabase = Depends(get_database),
    limit: int = Query(default=2000, ge=1, le=10000),
):
    A4, mm, canvas = _pdf_canvas_or_500()
    _, zones_or = await _get_supervisor_context(token_payload, db)
    if not zones_or:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Aucune zone affectée au superviseur.")

    query: dict = {"role": "customer", "$or": zones_or}
    cursor = db.users.find(query, {"passwordHash": 0}).sort([("createdAt", -1)]).limit(limit)
    items = await cursor.to_list(length=limit)

    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    width, height = A4
    margin_x = 12 * mm
    margin_y = 12 * mm
    y = height - margin_y

    c.setFont("Helvetica-Bold", 14)
    c.drawString(margin_x, y, _ascii("Rapport clients (superviseur)"))
    y -= 6 * mm
    c.setFont("Helvetica", 9)
    c.drawString(margin_x, y, _ascii(f"Total: {len(items)}"))
    y -= 8 * mm

    headers = ["Telephone", "Compteur", "Nom", "Tarif", "Centre", "Zone", "Secteur"]
    col_widths = [28, 28, 55, 16, 22, 22, 22]
    col_widths = [w * mm for w in col_widths]
    x_positions = [margin_x]
    for w in col_widths[:-1]:
        x_positions.append(x_positions[-1] + w)

    def draw_header() -> None:
        nonlocal y
        c.setFont("Helvetica-Bold", 8)
        for j, h in enumerate(headers):
            c.drawString(x_positions[j], y, _ascii(h))
        y -= 4 * mm
        c.setLineWidth(0.3)
        c.line(margin_x, y, width - margin_x, y)
        y -= 3 * mm
        c.setFont("Helvetica", 8)

    def safe(v: object) -> str:
        return _ascii(v)[:60]

    row_h = 4 * mm
    draw_header()
    for it in items:
        if y < margin_y + 20 * mm:
            c.showPage()
            y = height - margin_y
            draw_header()
        values = [
            safe(it.get("phone")),
            safe(it.get("meterNumber")),
            safe(it.get("name")),
            safe(it.get("tariffCode")),
            safe(it.get("center")),
            safe(it.get("zone")),
            safe(it.get("sector")),
        ]
        for j, v in enumerate(values):
            c.drawString(x_positions[j], y, v)
        y -= row_h

    c.showPage()
    c.save()
    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="application/pdf",
        headers={"Content-Disposition": 'attachment; filename="supervisor_clients.pdf"'},
    )


@router.get(
    "/meters/report.pdf",
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(require_roles("supervisor"))],
)
async def supervisor_meters_report_pdf(
    token_payload: dict = Depends(get_current_user_payload),
    db: AsyncIOMotorDatabase = Depends(get_database),
    limit: int = Query(default=3000, ge=1, le=20000),
):
    A4, mm, canvas = _pdf_canvas_or_500()
    _, zones_or = await _get_supervisor_context(token_payload, db)
    if not zones_or:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Aucune zone affectée au superviseur.")

    cycle_id = await resolve_cycle_id(db)

    query: dict = {"$or": zones_or, "cycleId": cycle_id}
    cursor = db.meters.find(query).sort([("center", 1), ("zone", 1), ("sector", 1), ("routeOrder", 1)]).limit(limit)
    items = await cursor.to_list(length=limit)

    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    width, height = A4
    margin_x = 12 * mm
    margin_y = 12 * mm
    y = height - margin_y

    c.setFont("Helvetica-Bold", 14)
    c.drawString(margin_x, y, _ascii("Rapport compteurs (superviseur)"))
    y -= 6 * mm
    c.setFont("Helvetica", 9)
    c.drawString(margin_x, y, _ascii(f"Total: {len(items)}"))
    y -= 8 * mm

    headers = ["Compteur", "Ordre", "Abonne", "Police", "Centre", "Zone", "Secteur"]
    col_widths = [34, 14, 24, 18, 22, 22, 22]
    col_widths = [w * mm for w in col_widths]
    x_positions = [margin_x]
    for w in col_widths[:-1]:
        x_positions.append(x_positions[-1] + w)

    def draw_header() -> None:
        nonlocal y
        c.setFont("Helvetica-Bold", 8)
        for j, h in enumerate(headers):
            c.drawString(x_positions[j], y, _ascii(h))
        y -= 4 * mm
        c.setLineWidth(0.3)
        c.line(margin_x, y, width - margin_x, y)
        y -= 3 * mm
        c.setFont("Helvetica", 8)

    def safe(v: object) -> str:
        return _ascii(v)[:60]

    row_h = 4 * mm
    draw_header()
    for it in items:
        if y < margin_y + 20 * mm:
            c.showPage()
            y = height - margin_y
            draw_header()
        values = [
            safe(it.get("meterNumber")),
            safe(it.get("routeOrder")),
            safe(it.get("subscriberNumber")),
            safe(it.get("police")),
            safe(it.get("center")),
            safe(it.get("zone")),
            safe(it.get("sector")),
        ]
        for j, v in enumerate(values):
            c.drawString(x_positions[j], y, v)
        y -= row_h

    c.showPage()
    c.save()
    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="application/pdf",
        headers={"Content-Disposition": 'attachment; filename="supervisor_meters.pdf"'},
    )


@router.get(
    "/readings/report.pdf",
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(require_roles("supervisor"))],
)
async def supervisor_readings_report_pdf(
    token_payload: dict = Depends(get_current_user_payload),
    db: AsyncIOMotorDatabase = Depends(get_database),
    date: str | None = Query(default=None),
    limit: int = Query(default=2000, ge=1, le=20000),
):
    A4, mm, canvas = _pdf_canvas_or_500()
    _, zones_or = await _get_supervisor_context(token_payload, db)
    if not zones_or:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Aucune zone affectée au superviseur.")

    tours_query: dict = {"$or": zones_or}
    if date:
        tours_query["date"] = date
    tour_docs = await db.tours.find(tours_query, {"_id": 1, "center": 1, "zone": 1, "sector": 1}).to_list(length=5000)
    if not tour_docs:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Aucune tournée pour ces filtres.")

    tour_id_to_loc: dict[str, dict] = {}
    tour_ids: list[str] = []
    for t in tour_docs:
        tid = str(t.get("_id"))
        tour_ids.append(tid)
        tour_id_to_loc[tid] = {"center": t.get("center"), "zone": t.get("zone"), "sector": t.get("sector")}

    query: dict = {"tourId": {"$in": tour_ids}}
    if date:
        query["date"] = date
    cursor = db.readings.find(query).sort([("date", -1), ("createdAt", -1)]).limit(limit)
    items = await cursor.to_list(length=limit)

    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    width, height = A4
    margin_x = 12 * mm
    margin_y = 12 * mm
    y = height - margin_y

    c.setFont("Helvetica-Bold", 14)
    c.drawString(margin_x, y, _ascii("Rapport relevés (superviseur)"))
    y -= 6 * mm
    c.setFont("Helvetica", 9)
    c.drawString(margin_x, y, _ascii(f"Date: {date or 'Toutes'}"))
    y -= 4 * mm
    c.drawString(margin_x, y, _ascii(f"Total: {len(items)}"))
    y -= 8 * mm

    headers = ["Date", "Centre", "Zone", "Compteur", "Ancien", "Nouveau", "Conso", "Tarif", "Montant"]
    col_widths = [20, 20, 20, 40, 14, 14, 14, 14, 18]
    col_widths = [w * mm for w in col_widths]
    x_positions = [margin_x]
    for w in col_widths[:-1]:
        x_positions.append(x_positions[-1] + w)

    def draw_header() -> None:
        nonlocal y
        c.setFont("Helvetica-Bold", 7)
        for j, h in enumerate(headers):
            c.drawString(x_positions[j], y, _ascii(h))
        y -= 4 * mm
        c.setLineWidth(0.3)
        c.line(margin_x, y, width - margin_x, y)
        y -= 3 * mm
        c.setFont("Helvetica", 7)

    def safe(v: object) -> str:
        return _ascii(v)[:60]

    row_h = 4 * mm
    draw_header()
    for it in items:
        if y < margin_y + 20 * mm:
            c.showPage()
            y = height - margin_y
            draw_header()
        loc = tour_id_to_loc.get(str(it.get("tourId"))) or {}
        values = [
            safe(it.get("date")),
            safe(loc.get("center")),
            safe(loc.get("zone")),
            safe(it.get("meterNumber")),
            safe(it.get("oldIndex")),
            safe(it.get("newIndex")),
            safe(it.get("consumption")),
            safe(it.get("tariffCode")),
            safe(it.get("amount")),
        ]
        for j, v in enumerate(values):
            c.drawString(x_positions[j], y, v)
        y -= row_h

    c.showPage()
    c.save()
    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="application/pdf",
        headers={"Content-Disposition": 'attachment; filename="supervisor_readings.pdf"'},
    )


@router.get(
    "/tours/report.pdf",
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(require_roles("supervisor"))],
)
async def supervisor_tours_report_pdf(
    token_payload: dict = Depends(get_current_user_payload),
    db: AsyncIOMotorDatabase = Depends(get_database),
    date: str | None = Query(default=None),
    limit: int = Query(default=2000, ge=1, le=20000),
):
    A4, mm, canvas = _pdf_canvas_or_500()
    _, zones_or = await _get_supervisor_context(token_payload, db)
    if not zones_or:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Aucune zone affectée au superviseur.")

    cycle_id = await resolve_cycle_id(db, date_value=date)

    query: dict = {"$or": zones_or, "cycleId": cycle_id}
    if date:
        query["date"] = date
    cursor = db.tours.find(query).sort([("date", -1), ("createdAt", -1)]).limit(limit)
    tours = await cursor.to_list(length=limit)
    if not tours:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Aucune tournée.")

    agent_ids = list({str(t.get("agentId")) for t in tours if t.get("agentId")})
    agents_by_id: dict[str, dict] = {}
    if agent_ids:
        a_cursor = db.users.find({"role": "agent", "_id": {"$in": [ObjectId(x) for x in agent_ids if ObjectId.is_valid(x)]}}, {"name": 1, "phone": 1})
        async for a in a_cursor:
            agents_by_id[str(a.get("_id"))] = a

    # Sort by agent then by zone
    tours.sort(key=lambda t: (str(t.get("agentId") or ""), str(t.get("center") or ""), str(t.get("zone") or ""), str(t.get("sector") or "")))

    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    width, height = A4
    margin_x = 12 * mm
    margin_y = 12 * mm
    y = height - margin_y

    c.setFont("Helvetica-Bold", 14)
    c.drawString(margin_x, y, _ascii("Tournées (par agent)"))
    y -= 6 * mm
    c.setFont("Helvetica", 9)
    c.drawString(margin_x, y, _ascii(f"Date: {date or 'Toutes'}"))
    y -= 4 * mm
    c.drawString(margin_x, y, _ascii(f"Total tournées: {len(tours)}"))
    y -= 8 * mm

    def new_page() -> None:
        nonlocal y
        c.showPage()
        y = height - margin_y

    current_agent = None
    c.setFont("Helvetica", 9)
    for t in tours:
        aid = str(t.get("agentId") or "")
        if aid != current_agent:
            current_agent = aid
            if y < margin_y + 40 * mm:
                new_page()
            agent = agents_by_id.get(aid) or {}
            agent_label = (agent.get("name") or agent.get("phone") or aid or "(sans agent)")
            c.setFont("Helvetica-Bold", 11)
            c.drawString(margin_x, y, _ascii(f"Agent: {agent_label}"))
            y -= 6 * mm
            c.setFont("Helvetica", 9)

        if y < margin_y + 20 * mm:
            new_page()
            c.setFont("Helvetica", 9)

        zone_label = f"{t.get('center') or '-'} / {t.get('zone') or '-'} / {t.get('sector') or '-'}"
        c.drawString(margin_x, y, _ascii(f"Tournée {str(t.get('_id'))[:8]} · {t.get('date')} · {zone_label}"))
        y -= 4 * mm

        items = t.get("items") or []
        c.setFont("Helvetica", 8)
        for it in items[:80]:
            if y < margin_y + 15 * mm:
                new_page()
                c.setFont("Helvetica", 8)
            mn = it.get("meterNumber")
            ro = it.get("routeOrder")
            oi = it.get("oldIndex")
            c.drawString(margin_x + 6 * mm, y, _ascii(f"- {mn}  ordre={ro if ro is not None else '-'}  ancien={oi if oi is not None else '-'}"))
            y -= 3.5 * mm
        c.setFont("Helvetica", 9)
        y -= 2 * mm

    c.showPage()
    c.save()
    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="application/pdf",
        headers={"Content-Disposition": 'attachment; filename="supervisor_tours.pdf"'},
    )


@router.get(
    "/meters",
    response_model=list[MeterPublic],
    dependencies=[Depends(require_roles("supervisor"))],
)
async def list_meters(
    token_payload: dict = Depends(get_current_user_payload),
    db: AsyncIOMotorDatabase = Depends(get_database),
    q: str | None = None,
    limit: int = 1000,
):
    supervisor_id = token_payload.get("sub")
    if not supervisor_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    try:
        supervisor_oid = ObjectId(str(supervisor_id))
    except Exception:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    supervisor = await db.users.find_one({"_id": supervisor_oid})
    if not supervisor:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Utilisateur introuvable.")

    assigned_zones = supervisor.get("assignedZones") or []
    zones_or = _zones_or_query(assigned_zones, include_sector_none=True)
    if not zones_or:
        return []

    cycle_id = await resolve_cycle_id(db)

    query: dict = {"$or": zones_or, "cycleId": cycle_id}
    if q and q.strip():
        query = {
            "$and": [
                {"$or": zones_or, "cycleId": cycle_id},
                {
                    "$or": [
                        {"meterNumber": {"$regex": q.strip(), "$options": "i"}},
                        {"subscriberNumber": {"$regex": q.strip(), "$options": "i"}},
                        {"police": {"$regex": q.strip(), "$options": "i"}},
                    ]
                },
            ]
        }

    cursor = db.meters.find(query).sort([("center", 1), ("zone", 1), ("sector", 1), ("routeOrder", 1)]).limit(
        max(1, min(limit, 2000))
    )
    items = await cursor.to_list(length=max(1, min(limit, 2000)))
    for it in items:
        it["_id"] = str(it["_id"])
    return items


@router.get(
    "/agents",
    response_model=list[UserPublic],
    dependencies=[Depends(require_roles("supervisor"))],
)
async def list_agents(
    token_payload: dict = Depends(get_current_user_payload),
    db: AsyncIOMotorDatabase = Depends(get_database),
):
    supervisor_id = token_payload.get("sub")
    if not supervisor_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    try:
        supervisor_oid = ObjectId(str(supervisor_id))
    except Exception:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    supervisor = await db.users.find_one({"_id": supervisor_oid})
    if not supervisor:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Utilisateur introuvable.")

    assigned_zones = supervisor.get("assignedZones") or []
    zones_or = [
        {"center": z.get("center"), "zone": z.get("zone"), "sector": z.get("sector")}
        for z in assigned_zones
        if z.get("center") and z.get("zone") and z.get("sector")
    ]

    if not zones_or:
        return []

    query: dict = {"role": "agent", "$or": zones_or}

    cursor = db.users.find(query).sort("createdAt", -1)

    out: list[dict] = []
    async for doc in cursor:
        doc.pop("passwordHash", None)
        doc["_id"] = str(doc["_id"])
        out.append(doc)
    return out


@router.post(
    "/agents",
    response_model=UserPublic,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_roles("supervisor"))],
)
async def create_agent(
    payload: CreateAgentBySupervisorRequest,
    token_payload: dict = Depends(get_current_user_payload),
    db: AsyncIOMotorDatabase = Depends(get_database),
):
    supervisor_id = token_payload.get("sub")
    if not supervisor_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    try:
        supervisor_oid = ObjectId(str(supervisor_id))
    except Exception:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    supervisor = await db.users.find_one({"_id": supervisor_oid})
    if not supervisor:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Utilisateur introuvable.")

    assigned_zones = supervisor.get("assignedZones") or []
    if not assigned_zones:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Aucune zone affectée au superviseur.",
        )

    requested_zone = {"center": payload.center, "zone": payload.zone, "sector": payload.sector}
    allowed = any(
        z.get("center") == requested_zone["center"]
        and z.get("zone") == requested_zone["zone"]
        and z.get("sector") == requested_zone["sector"]
        for z in assigned_zones
    )
    if not allowed:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Zone non autorisée pour ce superviseur.")

    existing = await db.users.find_one({"phone": payload.phone})
    if existing:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Téléphone déjà utilisé.")

    now = datetime.now(timezone.utc)
    doc = {
        "phone": payload.phone,
        "name": payload.name,
        "role": "agent",
        "passwordHash": hash_password(payload.password),
        "isActive": payload.isActive,
        "mustChangePassword": False,
        "center": payload.center,
        "zone": payload.zone,
        "sector": payload.sector,
        "createdAt": now,
        "updatedAt": now,
        "createdBy": str(supervisor["_id"]),
    }

    res = await db.users.insert_one(doc)
    created = await db.users.find_one({"_id": res.inserted_id})
    if not created:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erreur création utilisateur.")

    created.pop("passwordHash", None)
    created["_id"] = str(created["_id"])
    return created


@router.post(
    "/tours/generate",
    response_model=GenerateToursResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(require_roles("supervisor"))],
)
async def generate_tours(
    payload: GenerateToursRequest,
    token_payload: dict = Depends(get_current_user_payload),
    db: AsyncIOMotorDatabase = Depends(get_database),
):
    supervisor_id = token_payload.get("sub")
    if not supervisor_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    try:
        supervisor_oid = ObjectId(str(supervisor_id))
    except Exception:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    supervisor = await db.users.find_one({"_id": supervisor_oid})
    if not supervisor:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Utilisateur introuvable.")

    assigned_zones = supervisor.get("assignedZones") or []
    zones_or = _zones_or_query(assigned_zones, include_sector_none=False)
    if not zones_or:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Aucune zone affectée au superviseur.")

    requested_center = payload.center
    requested_zone = payload.zone
    requested_sector = payload.sector
    if any([requested_center, requested_zone, requested_sector]):
        if not (requested_center and requested_zone and requested_sector):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Filtre zone incomplet (center/zone/sector).")
        allowed = any(
            z.get("center") == requested_center and z.get("zone") == requested_zone and z.get("sector") == requested_sector
            for z in assigned_zones
        )
        if not allowed:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Zone non autorisée pour ce superviseur.")

    cycle_id = await resolve_cycle_id(db, date_value=payload.date)

    now = datetime.now(timezone.utc)
    created = 0
    skipped = 0
    errors = 0
    error_lines: list[str] = []
    tours_out: list[dict] = []

    mode = payload.mode
    if mode == "A":
        if not payload.agentId:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="agentId requis en mode A.")
        agent_ids = [payload.agentId]
        max_meters_per_tour = None
        manual_assignments: list[tuple[str, int]] = []
    elif mode == "MANUAL":
        raw = payload.assignments or []
        manual_assignments = []
        for a in raw:
            try:
                c = int(a.count)
            except Exception:
                c = 0
            if c > 0 and a.agentId:
                manual_assignments.append((str(a.agentId), c))
        if not manual_assignments:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="assignments requis (au moins 1 agent avec count>0) en mode MANUAL.")
        agent_ids = [aid for aid, _ in manual_assignments]
        max_meters_per_tour = None
    else:
        agent_ids = payload.agentIds or []
        if not agent_ids:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="agentIds requis en mode B2.")
        if not payload.maxMetersPerTour:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="maxMetersPerTour requis en mode B2.")
        max_meters_per_tour = payload.maxMetersPerTour
        manual_assignments = []

    if requested_center and requested_zone and requested_sector:
        zone_filters = [{"center": requested_center, "zone": requested_zone, "sector": requested_sector}]
    else:
        zone_filters = [{"center": z.get("center"), "zone": z.get("zone"), "sector": z.get("sector")} for z in assigned_zones if z.get("center") and z.get("zone") and z.get("sector")]

    for zf in zone_filters:
        center = str(zf["center"])
        zone = str(zf["zone"])
        sector = str(zf["sector"])

        meters = (
            await db.meters.find({"cycleId": cycle_id, "center": center, "zone": zone, "sector": sector})
            .sort("routeOrder", 1)
            .to_list(length=50000)
        )
        if not meters:
            skipped += 1
            continue

        # Exclude meters already assigned to an existing tour for the same date + zone.
        already_assigned: set[str] = set()
        existing_tours = await db.tours.find(
            {
                "cycleId": cycle_id,
                "date": payload.date,
                "center": center,
                "zone": zone,
                "sector": sector,
            },
            {"items.meterNumber": 1},
        ).to_list(length=5000)
        for t in existing_tours:
            for it in t.get("items") or []:
                mn = it.get("meterNumber")
                if mn:
                    already_assigned.add(str(mn))

        if already_assigned:
            meters = [m for m in meters if str(m.get("meterNumber")) not in already_assigned]
            if not meters:
                skipped += 1
                continue

        items: list[dict] = []
        for m in meters:
            items.append(
                TourItem(
                    meterId=str(m.get("_id")) if m.get("_id") else None,
                    meterNumber=str(m.get("meterNumber")),
                    routeOrder=m.get("routeOrder"),
                ).model_dump()
            )

        if mode == "MANUAL":
            remaining = list(items)
            chunks_by_agent: dict[str, list[list[dict]]] = {}
            for aid, count in manual_assignments:
                if not remaining:
                    break
                take = min(int(count), len(remaining))
                chunk = remaining[:take]
                remaining = remaining[take:]
                if chunk:
                    chunks_by_agent[aid] = [chunk]
        elif mode == "A":
            # Single tour for the selected agent
            chunks_by_agent: dict[str, list[list[dict]]] = {agent_ids[0]: [items]}
        else:
            # Fair distribution across all selected agents: round-robin per meter.
            # Example: 10 meters + 3 agents => 4/3/3.
            per_agent_items: dict[str, list[dict]] = {aid: [] for aid in agent_ids}
            for idx, it in enumerate(items):
                aid = agent_ids[idx % len(agent_ids)]
                per_agent_items[aid].append(it)

            # Respect maxMetersPerTour by chunking each agent's assignment
            chunks_by_agent = {}
            for aid, assigned in per_agent_items.items():
                if not assigned:
                    continue
                mmpt = int(max_meters_per_tour or len(assigned))
                chunks_by_agent[aid] = [assigned[i : i + mmpt] for i in range(0, len(assigned), mmpt)]

        tour_seq = 0
        for agent_id, chunks in chunks_by_agent.items():
            for chunk in chunks:
                tour_seq += 1
                tour_doc = {
                    "cycleId": cycle_id,
                    "date": payload.date,
                    "center": center,
                    "zone": zone,
                    "sector": sector,
                    "agentId": agent_id,
                    "items": chunk,
                    "createdAt": now,
                    "updatedAt": now,
                }

                try:
                    res = await db.tours.insert_one(tour_doc)
                    created_doc = await db.tours.find_one({"_id": res.inserted_id})
                    if created_doc:
                        created_doc["_id"] = str(created_doc["_id"])
                        tours_out.append(created_doc)
                    created += 1
                except DuplicateKeyError:
                    skipped += 1
                except Exception as e:
                    errors += 1
                    error_lines.append(f"{center}/{zone}/{sector}#{tour_seq}: {type(e).__name__}")

    tours_public: list[TourPublic] = []
    for t in tours_out:
        tours_public.append(TourPublic(**t))

    return GenerateToursResponse(
        created=created,
        skipped=skipped,
        errors=errors,
        errorLines=error_lines,
        tours=tours_public,
    )


@router.get(
    "/tours",
    response_model=list[TourPublic],
    dependencies=[Depends(require_roles("supervisor"))],
)
async def list_tours(
    token_payload: dict = Depends(get_current_user_payload),
    db: AsyncIOMotorDatabase = Depends(get_database),
    date: str | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=1000),
):
    supervisor_id = token_payload.get("sub")
    if not supervisor_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    try:
        supervisor_oid = ObjectId(str(supervisor_id))
    except Exception:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    supervisor = await db.users.find_one({"_id": supervisor_oid})
    if not supervisor:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Utilisateur introuvable.")

    assigned_zones = supervisor.get("assignedZones") or []
    zones_or = [
        {"center": z.get("center"), "zone": z.get("zone"), "sector": z.get("sector")}
        for z in assigned_zones
        if z.get("center") and z.get("zone") and z.get("sector")
    ]
    if not zones_or:
        return []

    cycle_id = await resolve_cycle_id(db, date_value=date)

    query: dict = {"$or": zones_or, "cycleId": cycle_id}
    if date:
        query["date"] = date

    cursor = db.tours.find(query).sort([("date", -1), ("createdAt", -1)]).limit(limit)
    docs = await cursor.to_list(length=limit)
    for d in docs:
        d["_id"] = str(d["_id"])
    return [TourPublic(**d) for d in docs]
