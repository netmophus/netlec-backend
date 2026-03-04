from datetime import date, datetime, timedelta, timezone
import calendar
import csv
import io
import secrets
import string
import unicodedata

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile, status
from fastapi.responses import StreamingResponse
from motor.motor_asyncio import AsyncIOMotorDatabase

from bson import ObjectId

from app.api.models import (
    CreateStaffUserRequest,
    CreateZoneRequest,
    AdminStatsResponse,
    ImportMetersResponse,
    ImportCustomersResponse,
    PreRegisterCustomerRequest,
    UpdateUserRequest,
    UserPublic,
    ZonePublic,
    TariffPublic,
    PortalSettingsPublic,
    UpsertTariffsRequest,
)
from app.core.deps import get_database, require_roles
from app.core.security import hash_password

router = APIRouter(prefix="/admin", tags=["admin"])

DEFAULT_LATEST_ANNOUNCEMENTS = [
    {
        "id": "ann-1",
        "title": "Maintenance planifiee",
        "message": "Intervention reseau ce samedi de 22h a 01h sur Conakry Nord.",
        "date": "04 Mars 2026",
    },
    {
        "id": "ann-2",
        "title": "Nouveaux points de paiement",
        "message": "Le paiement NITA est disponible dans 12 nouveaux points partenaires.",
        "date": "03 Mars 2026",
    },
    {
        "id": "ann-3",
        "title": "Tournees prioritaires",
        "message": "Les releves des zones Koubia Nord et Bambeto sont prioritaires aujourd'hui.",
        "date": "02 Mars 2026",
    },
]


def _normalize(value: str | None) -> str | None:
    if value is None:
        return None
    v = value.strip()
    return v if v else None


def _pick(row: dict, *keys: str) -> str | None:
    for k in keys:
        if k in row:
            return _normalize(row.get(k))
    return None


def _as_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    v = value.strip()
    return v if v else None


def _normalize_announcements(value: object) -> list[dict]:
    normalized: list[dict] = []
    if isinstance(value, list):
        for i, item in enumerate(value[:3]):
            if not isinstance(item, dict):
                continue

            title = _as_text(item.get("title"))
            message = _as_text(item.get("message"))
            date = _as_text(item.get("date"))
            if not title or not message or not date:
                continue

            normalized.append(
                {
                    "id": _as_text(item.get("id")) or f"ann-{i + 1}",
                    "title": title,
                    "message": message,
                    "date": date,
                }
            )

    return normalized if normalized else DEFAULT_LATEST_ANNOUNCEMENTS


def _normalize_portal_settings(source: object) -> dict:
    data = source if isinstance(source, dict) else {}
    return {
        "logoUrl": _as_text(data.get("logoUrl")),
        "facebookUrl": _as_text(data.get("facebookUrl")),
        "linkedinUrl": _as_text(data.get("linkedinUrl")),
        "xUrl": _as_text(data.get("xUrl")),
        "youtubeUrl": _as_text(data.get("youtubeUrl")),
        "supportPhone": _as_text(data.get("supportPhone")),
        "supportWhatsapp": _as_text(data.get("supportWhatsapp")),
        "latestAnnouncements": _normalize_announcements(data.get("latestAnnouncements")),
    }


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


def _default_tariff_tiers(now: datetime) -> list[dict]:
    return [
        {"code": "T1", "fromKwh": 1, "toKwh": 30, "ratePerKwh": 50, "createdAt": now, "updatedAt": now},
        {"code": "T2", "fromKwh": 31, "toKwh": 100, "ratePerKwh": 75, "createdAt": now, "updatedAt": now},
        {"code": "T3", "fromKwh": 101, "toKwh": None, "ratePerKwh": 100, "createdAt": now, "updatedAt": now},
    ]


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
        tiers.append({"code": d.get("code"), "fromKwh": int(fk), "toKwh": to_kwh, "ratePerKwh": int(rk)})
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


@router.get(
    "/portal-settings",
    response_model=PortalSettingsPublic,
    dependencies=[Depends(require_roles("admin"))],
)
async def get_portal_settings(db: AsyncIOMotorDatabase = Depends(get_database)):
    doc = await db.portal_settings.find_one({"key": "default"})
    source = (doc or {}).get("settings") if isinstance((doc or {}).get("settings"), dict) else (doc or {})
    return _normalize_portal_settings(source)


@router.put(
    "/portal-settings",
    response_model=PortalSettingsPublic,
    dependencies=[Depends(require_roles("admin"))],
)
async def upsert_portal_settings(
    payload: PortalSettingsPublic,
    db: AsyncIOMotorDatabase = Depends(get_database),
):
    settings_payload = _normalize_portal_settings(payload.model_dump())
    now = datetime.now(timezone.utc)
    await db.portal_settings.update_one(
        {"key": "default"},
        {
            "$set": {
                "key": "default",
                "settings": settings_payload,
                "updatedAt": now,
            },
            "$setOnInsert": {"createdAt": now},
        },
        upsert=True,
    )
    return settings_payload


@router.get(
    "/tariffs",
    response_model=list[TariffPublic],
    dependencies=[Depends(require_roles("admin"))],
)
async def list_tariffs(
    db: AsyncIOMotorDatabase = Depends(get_database),
    limit: int = Query(default=50, ge=1, le=200),
):
    cursor = db.tariffs.find({}).sort([("fromKwh", 1), ("code", 1)]).limit(limit)
    docs = await cursor.to_list(length=limit)

    if not docs:
        now = datetime.now(timezone.utc)
        defaults = _default_tariff_tiers(now)
        await db.tariffs.insert_many(defaults)
        cursor = db.tariffs.find({}).sort([("fromKwh", 1), ("code", 1)]).limit(limit)
        docs = await cursor.to_list(length=limit)
    else:
        # Backward-compat: if tariffs exist but are in legacy format (no fromKwh), migrate to progressive tiers.
        has_from = any(isinstance(d.get("fromKwh"), int) for d in docs)
        if not has_from:
            now = datetime.now(timezone.utc)
            defaults = _default_tariff_tiers(now)
            await db.tariffs.delete_many({})
            await db.tariffs.insert_many(defaults)
            cursor = db.tariffs.find({}).sort([("fromKwh", 1), ("code", 1)]).limit(limit)
            docs = await cursor.to_list(length=limit)

    out: list[TariffPublic] = []
    for d in docs:
        d["_id"] = str(d["_id"])
        out.append(TariffPublic(**d))
    return out


@router.put(
    "/tariffs",
    response_model=list[TariffPublic],
    dependencies=[Depends(require_roles("admin"))],
)
async def upsert_tariffs(
    payload: UpsertTariffsRequest,
    db: AsyncIOMotorDatabase = Depends(get_database),
):
    items = payload.items or []
    if not items:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="items requis.")

    now = datetime.now(timezone.utc)
    for it in items:
        code = str(it.code).strip().upper()
        if not code:
            continue
        if it.toKwh is not None and int(it.toKwh) < int(it.fromKwh):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="toKwh doit être >= fromKwh (ou null).")
        await db.tariffs.update_one(
            {"code": code},
            {
                "$set": {
                    "code": code,
                    "fromKwh": int(it.fromKwh),
                    "toKwh": int(it.toKwh) if it.toKwh is not None else None,
                    "ratePerKwh": int(it.ratePerKwh),
                    "updatedAt": now,
                },
                "$setOnInsert": {"createdAt": now},
            },
            upsert=True,
        )

    cursor = db.tariffs.find({}).sort([("fromKwh", 1), ("code", 1)]).limit(50)
    docs = await cursor.to_list(length=50)
    out: list[TariffPublic] = []
    for d in docs:
        d["_id"] = str(d["_id"])
        out.append(TariffPublic(**d))
    return out


def _end_of_month_due_date(reading_date_iso: str, grace_days: int) -> date | None:
    try:
        rd_d = date.fromisoformat(str(reading_date_iso))
    except Exception:
        return None
    last_day = calendar.monthrange(rd_d.year, rd_d.month)[1]
    return date(rd_d.year, rd_d.month, last_day) + timedelta(days=int(grace_days))


@router.post(
    "/zones",
    response_model=ZonePublic,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_roles("admin"))],
)
async def create_zone(
    payload: CreateZoneRequest,
    db: AsyncIOMotorDatabase = Depends(get_database),
):
    existing = await db.zones.find_one(
        {
            "center": payload.center,
            "zone": payload.zone,
            "sector": payload.sector,
        }
    )
    if existing:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Zone déjà existante.")

    now = datetime.now(timezone.utc)
    doc = {
        "center": payload.center,
        "zone": payload.zone,
        "sector": payload.sector,
        "createdAt": now,
        "updatedAt": now,
    }
    res = await db.zones.insert_one(doc)
    created = await db.zones.find_one({"_id": res.inserted_id})
    if not created:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erreur création zone.")

    created["_id"] = str(created["_id"])
    return created


@router.get(
    "/zones",
    response_model=list[ZonePublic],
    dependencies=[Depends(require_roles("admin"))],
)
async def list_zones(
    db: AsyncIOMotorDatabase = Depends(get_database),
    center: str | None = Query(default=None),
    zone: str | None = Query(default=None),
    sector: str | None = Query(default=None),
    limit: int = Query(default=500, ge=1, le=1000),
):
    query: dict = {}
    if center:
        query["center"] = center
    if zone:
        query["zone"] = zone
    if sector:
        query["sector"] = sector

    cursor = db.zones.find(query).sort([("center", 1), ("zone", 1), ("sector", 1)]).limit(limit)
    items = await cursor.to_list(length=limit)
    for z in items:
        z["_id"] = str(z["_id"])
    return items


@router.post(
    "/users",
    response_model=UserPublic,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_roles("admin"))],
)
async def create_staff_user(
    payload: CreateStaffUserRequest,
    db: AsyncIOMotorDatabase = Depends(get_database),
):
    existing = await db.users.find_one({"phone": payload.phone})
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Téléphone déjà utilisé.",
        )

    now = datetime.now(timezone.utc)
    doc = {
        "phone": payload.phone,
        "name": payload.name,
        "role": payload.role,
        "passwordHash": hash_password(payload.password),
        "isActive": payload.isActive,
        "mustChangePassword": False,
        "createdAt": now,
        "updatedAt": now,
    }

    res = await db.users.insert_one(doc)
    created = await db.users.find_one({"_id": res.inserted_id})
    if not created:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erreur création utilisateur.")

    created.pop("passwordHash", None)
    created["_id"] = str(created["_id"])
    return created


@router.post(
    "/customers/import",
    response_model=ImportCustomersResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(require_roles("admin"))],
)
async def import_customers(
    file: UploadFile = File(...),
    delimiter: str = Query(default=",", min_length=1, max_length=1),
    updateExisting: bool = Query(default=False),
    db: AsyncIOMotorDatabase = Depends(get_database),
    token_payload: dict = Depends(require_roles("admin")),
):
    if file.content_type not in {"text/csv", "application/vnd.ms-excel", "application/octet-stream"}:
        # Many browsers send application/octet-stream for CSV; keep it permissive.
        pass

    raw = await file.read()
    try:
        text = raw.decode("utf-8-sig")
    except Exception:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Fichier CSV invalide (encodage).")

    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    if not reader.fieldnames:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Le CSV n'a pas d'en-tête (header).")

    now = datetime.now(timezone.utc)
    inserted = 0
    updated = 0
    skipped = 0
    errors = 0
    error_lines: list[int] = []

    for i, row in enumerate(reader, start=2):
        try:
            phone = _pick(row, "phone", "Phone", "PHONE", "msisdn", "MSISDN")
            meter_number = _pick(row, "meterNumber", "meter_number", "MeterNumber", "METER_NUMBER")
            subscriber_number = _pick(row, "subscriberNumber", "subscriber_number", "SubscriberNumber")
            police = _pick(row, "police", "Police", "POLICE")
            old_index_raw = _pick(row, "oldIndex", "old_index", "ancienIndex", "ancien_index", "OLD_INDEX")

            old_index: int | None = None
            if old_index_raw is not None:
                try:
                    old_index = int(old_index_raw)
                except Exception:
                    old_index = None

            if not phone or not meter_number or not subscriber_number or not police:
                skipped += 1
                continue

            doc = {
                "phone": phone,
                "role": "customer",
                "passwordHash": None,
                "isActive": False,
                "meterNumber": meter_number,
                "subscriberNumber": subscriber_number,
                "police": police,
                "oldIndex": old_index,
                "name": _pick(row, "name", "fullName", "full_name", "customerName"),
                "address": _pick(row, "address", "Address", "locality", "quartier"),
                "tariffCode": _pick(row, "tariffCode", "tariff_code", "TariffCode"),
                "category": _pick(row, "category", "Category"),
                "grouping": _pick(row, "grouping", "Grouping"),
                "center": _pick(row, "center", "Center", "agency"),
                "zone": _pick(row, "zone", "Zone"),
                "sector": _pick(row, "sector", "Sector"),
                "source": "SI_IMPORT",
                "preRegisteredBy": token_payload.get("sub"),
                "createdAt": now,
                "updatedAt": now,
            }

            existing = await db.users.find_one({"phone": phone})
            if existing:
                if existing.get("role") != "customer":
                    skipped += 1
                    continue
                if not updateExisting:
                    skipped += 1
                    continue

                update_doc = {k: v for k, v in doc.items() if v is not None}
                update_doc.pop("createdAt", None)
                update_doc["updatedAt"] = now
                update_doc.pop("passwordHash", None)
                update_doc.pop("isActive", None)

                await db.users.update_one({"_id": existing["_id"]}, {"$set": update_doc})
                updated += 1
                continue

            await db.users.insert_one(doc)
            inserted += 1

        except Exception:
            errors += 1
            error_lines.append(i)

    return ImportCustomersResponse(
        inserted=inserted,
        updated=updated,
        skipped=skipped,
        errors=errors,
        errorLines=error_lines,
    )


@router.post(
    "/meters/import",
    response_model=ImportMetersResponse,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(require_roles("admin"))],
)
async def import_meters(
    file: UploadFile = File(...),
    delimiter: str = Query(default=";", min_length=1, max_length=1),
    upsert: bool = Query(default=True),
    validateZones: bool = Query(default=True),
    db: AsyncIOMotorDatabase = Depends(get_database),
):
    raw = await file.read()
    try:
        text = raw.decode("utf-8-sig")
    except Exception:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Fichier CSV invalide (encodage).")

    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    if not reader.fieldnames:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Le CSV n'a pas d'en-tête (header).")

    now = datetime.now(timezone.utc)
    inserted = 0
    updated = 0
    skipped = 0
    errors = 0
    error_lines: list[int] = []

    seen_meter_numbers: set[str] = set()
    seen_route_orders: set[tuple[str, str, str, int]] = set()

    def _parse_int(v: str | None) -> int | None:
        if v is None:
            return None
        s = v.strip()
        if not s:
            return None
        try:
            return int(s)
        except Exception:
            return None

    for i, row in enumerate(reader, start=2):
        try:
            meter_number = _pick(row, "meterNumber", "meter_number", "MeterNumber", "METER_NUMBER")
            center = _pick(row, "center", "Center", "agency")
            zone = _pick(row, "zone", "Zone")
            sector = _pick(row, "sector", "Sector", "secteur", "Secteur")
            route_order = _parse_int(_pick(row, "routeOrder", "route_order", "sequence", "Sequence", "SEQ"))

            subscriber_number = _pick(row, "subscriberNumber", "subscriber_number", "SubscriberNumber")
            police = _pick(row, "police", "Police", "POLICE")
            address = _pick(row, "address", "Address", "locality", "quartier")

            if not meter_number or route_order is None:
                errors += 1
                error_lines.append(i)
                continue

            if meter_number in seen_meter_numbers:
                errors += 1
                error_lines.append(i)
                continue
            seen_meter_numbers.add(meter_number)

            if center and zone and sector:
                key = (center, zone, sector, route_order)
                if key in seen_route_orders:
                    errors += 1
                    error_lines.append(i)
                    continue
                seen_route_orders.add(key)

                if validateZones:
                    z = await db.zones.find_one({"center": center, "zone": zone, "sector": sector})
                    if not z:
                        errors += 1
                        error_lines.append(i)
                        continue

            existing = await db.meters.find_one({"meterNumber": meter_number})
            if not existing and not upsert:
                skipped += 1
                continue

            doc_set = {
                "routeOrder": route_order,
                "updatedAt": now,
            }
            if center is not None:
                doc_set["center"] = center
            if zone is not None:
                doc_set["zone"] = zone
            if sector is not None:
                doc_set["sector"] = sector
            if subscriber_number is not None:
                doc_set["subscriberNumber"] = subscriber_number
            if police is not None:
                doc_set["police"] = police
            if address is not None:
                doc_set["address"] = address

            if existing:
                await db.meters.update_one({"_id": existing["_id"]}, {"$set": doc_set})
                updated += 1
            else:
                doc = {
                    "meterNumber": meter_number,
                    "center": center,
                    "zone": zone,
                    "sector": sector,
                    "routeOrder": route_order,
                    "subscriberNumber": subscriber_number,
                    "police": police,
                    "address": address,
                    "createdAt": now,
                    "updatedAt": now,
                }
                await db.meters.insert_one(doc)
                inserted += 1

        except Exception:
            errors += 1
            error_lines.append(i)

    return ImportMetersResponse(
        inserted=inserted,
        updated=updated,
        skipped=skipped,
        errors=errors,
        errorLines=error_lines,
    )


@router.get(
    "/customers/report.pdf",
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(require_roles("admin"))],
)
async def customers_report_pdf(
    db: AsyncIOMotorDatabase = Depends(get_database),
    center: str | None = Query(default=None),
    zone: str | None = Query(default=None),
    sector: str | None = Query(default=None),
    limit: int = Query(default=2000, ge=1, le=10000),
):
    try:
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

        query: dict = {"role": "customer"}
        if center:
            query["center"] = center
        if zone:
            query["zone"] = zone
        if sector:
            query["sector"] = sector

        cursor = db.users.find(query, {"passwordHash": 0}).sort([("createdAt", -1)]).limit(limit)
        items = await cursor.to_list(length=limit)

        buf = io.BytesIO()
        c = canvas.Canvas(buf, pagesize=A4)
        width, height = A4

        margin_x = 12 * mm
        margin_y = 12 * mm
        y = height - margin_y

        def _ascii(value: object) -> str:
            if value is None:
                return ""
            s = str(value)
            s = unicodedata.normalize("NFKD", s)
            s = s.encode("ascii", "ignore").decode("ascii")
            return s

        title = "Rapport clients (pre-enregistres)"
        c.setFont("Helvetica-Bold", 14)
        c.drawString(margin_x, y, _ascii(title))
        y -= 6 * mm

        c.setFont("Helvetica", 9)
        subtitle_parts = []
        if center:
            subtitle_parts.append(f"Centre={center}")
        if zone:
            subtitle_parts.append(f"Zone={zone}")
        if sector:
            subtitle_parts.append(f"Secteur={sector}")
        subtitle = " - ".join(subtitle_parts) if subtitle_parts else "Tous"
        c.drawString(margin_x, y, _ascii(f"Filtres: {subtitle}"))
        y -= 4 * mm
        c.drawString(margin_x, y, _ascii(f"Total: {len(items)}"))
        y -= 8 * mm

        headers = ["Telephone", "Compteur", "Abonne", "Police", "Nom", "Centre", "Zone", "Secteur"]
        col_widths = [28, 24, 22, 18, 45, 18, 18, 18]
        col_widths = [w * mm for w in col_widths]
        x_positions = [margin_x]
        for w in col_widths[:-1]:
            x_positions.append(x_positions[-1] + w)

        def draw_header() -> float:
            nonlocal y
            c.setFont("Helvetica-Bold", 8)
            for j, h in enumerate(headers):
                c.drawString(x_positions[j], y, _ascii(h))
            y -= 4 * mm
            c.setLineWidth(0.3)
            c.line(margin_x, y, width - margin_x, y)
            y -= 3 * mm
            c.setFont("Helvetica", 8)
            return y

        draw_header()

        def safe(v: object) -> str:
            return _ascii(v)[:60]

        row_h = 4 * mm
        for it in items:
            if y < margin_y + 20 * mm:
                c.showPage()
                y = height - margin_y
                draw_header()

            values = [
                safe(it.get("phone")),
                safe(it.get("meterNumber")),
                safe(it.get("subscriberNumber")),
                safe(it.get("police")),
                safe(it.get("name")),
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

        filename = "clients_report.pdf"
        return StreamingResponse(
            buf,
            media_type="application/pdf",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    except HTTPException:
        raise
    except Exception as e:
        import traceback

        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erreur PDF: {type(e).__name__}: {e}",
        )


@router.post(
    "/invoices/sync",
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(require_roles("admin"))],
)
async def sync_invoices(
    db: AsyncIOMotorDatabase = Depends(get_database),
    graceDays: int = Query(default=10, ge=0, le=60),
    limit: int = Query(default=50000, ge=1, le=200000),
):
    now = datetime.now(timezone.utc)

    cursor = db.readings.find({}).sort([("date", -1), ("createdAt", -1)]).limit(limit)
    reading_docs = await cursor.to_list(length=limit)

    meter_numbers = list({str(r.get("meterNumber")) for r in reading_docs if r.get("meterNumber")})
    customers_by_meter: dict[str, dict] = {}
    if meter_numbers:
        cust_cursor = db.users.find({"role": "customer", "meterNumber": {"$in": meter_numbers}}, {"passwordHash": 0})
        async for cdoc in cust_cursor:
            mn = cdoc.get("meterNumber")
            if mn:
                customers_by_meter[str(mn)] = cdoc

    invoice_ids = [f"INV-{str(r.get('_id'))}" for r in reading_docs if r.get("_id")]
    paid_invoice_ids: set[str] = set()
    if invoice_ids:
        cursor_paid = db.payments.find({"invoiceId": {"$in": invoice_ids}, "status": "SUCCEEDED"}, {"invoiceId": 1})
        async for pdoc in cursor_paid:
            inv = pdoc.get("invoiceId")
            if inv:
                paid_invoice_ids.add(str(inv))

    inserted = 0
    updated = 0
    skipped = 0
    errors = 0

    for r in reading_docs:
        try:
            rid = r.get("_id")
            if not rid:
                skipped += 1
                continue

            meter_number = r.get("meterNumber")
            if not meter_number:
                skipped += 1
                continue
            meter_number = str(meter_number)

            customer = customers_by_meter.get(meter_number)
            if not customer:
                skipped += 1
                continue

            reading_date = str(r.get("date"))
            period = reading_date[:7] if len(reading_date) >= 7 else reading_date

            due_d = _end_of_month_due_date(reading_date, graceDays)
            due_date_str = due_d.isoformat() if due_d else None

            cons = r.get("consumption")
            tc = r.get("tariffCode") or customer.get("tariffCode")
            amount: int | None = None
            if isinstance(cons, int):
                tiers = await _load_tariff_tiers_db(db)
                amount = _compute_progressive_amount(int(cons), tiers)
                if amount is None:
                    rate = await _tariff_rate_per_kwh_db(db, tc)
                    if rate is not None:
                        amount = int(int(cons) * int(rate))

            invoice_id = f"INV-{str(rid)}"
            if invoice_id in paid_invoice_ids:
                status_str = "PAID"
            else:
                today = now.date()
                if due_d is not None and today > due_d:
                    status_str = "OVERDUE"
                else:
                    status_str = "DUE"

            doc_set = {
                "invoiceId": invoice_id,
                "readingId": str(rid),
                "customerId": str(customer.get("_id")),
                "meterNumber": meter_number,
                "period": period,
                "date": reading_date,
                "dueDate": due_date_str,
                "tariffCode": str(tc) if tc is not None else None,
                "consumption": cons if isinstance(cons, int) else None,
                "amount": amount,
                "status": status_str,
                "center": customer.get("center"),
                "zone": customer.get("zone"),
                "sector": customer.get("sector"),
                "updatedAt": now,
            }

            res = await db.invoices.update_one(
                {"invoiceId": invoice_id},
                {"$set": doc_set, "$setOnInsert": {"createdAt": now}},
                upsert=True,
            )
            if res.upserted_id is not None:
                inserted += 1
            elif res.modified_count:
                updated += 1
            else:
                skipped += 1

        except Exception:
            errors += 1

    return {"inserted": inserted, "updated": updated, "skipped": skipped, "errors": errors}


@router.get(
    "/cutoffs/report.pdf",
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(require_roles("admin"))],
)
async def cutoffs_report_pdf(
    db: AsyncIOMotorDatabase = Depends(get_database),
    asOf: str | None = Query(default=None),
    graceDays: int = Query(default=10, ge=0, le=60),
    center: str | None = Query(default=None),
    zone: str | None = Query(default=None),
    sector: str | None = Query(default=None),
    limit: int = Query(default=2000, ge=1, le=10000),
):
    try:
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

        def _ascii(value: object) -> str:
            if value is None:
                return ""
            s = str(value)
            s = unicodedata.normalize("NFKD", s)
            s = s.encode("ascii", "ignore").decode("ascii")
            return s

        if asOf:
            try:
                asof_d = date.fromisoformat(asOf)
            except Exception:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="asOf invalide (YYYY-MM-DD).")
        else:
            asof_d = datetime.now(timezone.utc).date()

        cutoff_rows: list[dict] = []

        due_cutoff = asof_d.isoformat()
        query: dict = {"status": {"$in": ["DUE", "OVERDUE"]}, "dueDate": {"$lt": due_cutoff}}
        if center:
            query["center"] = center
        if zone:
            query["zone"] = zone
        if sector:
            query["sector"] = sector

        cursor = db.invoices.find(query).sort([("dueDate", 1), ("meterNumber", 1)]).limit(limit)
        docs = await cursor.to_list(length=limit)
        for inv in docs:
            if inv.get("status") == "DUE":
                cutoff_rows.append(
                    {
                        "phone": None,
                        "meterNumber": inv.get("meterNumber"),
                        "subscriberNumber": None,
                        "police": None,
                        "name": None,
                        "center": inv.get("center"),
                        "zone": inv.get("zone"),
                        "sector": inv.get("sector"),
                        "invoiceId": inv.get("invoiceId"),
                        "invoiceDate": inv.get("date"),
                        "dueDate": inv.get("dueDate"),
                        "amount": inv.get("amount"),
                    }
                )
            else:
                cutoff_rows.append(
                    {
                        "phone": None,
                        "meterNumber": inv.get("meterNumber"),
                        "subscriberNumber": None,
                        "police": None,
                        "name": None,
                        "center": inv.get("center"),
                        "zone": inv.get("zone"),
                        "sector": inv.get("sector"),
                        "invoiceId": inv.get("invoiceId"),
                        "invoiceDate": inv.get("date"),
                        "dueDate": inv.get("dueDate"),
                        "amount": inv.get("amount"),
                    }
                )

        buf = io.BytesIO()
        c = canvas.Canvas(buf, pagesize=A4)
        width, height = A4

        margin_x = 12 * mm
        margin_y = 12 * mm
        y = height - margin_y

        title = "Liste de coupure (impayes)"
        c.setFont("Helvetica-Bold", 14)
        c.drawString(margin_x, y, _ascii(title))
        y -= 6 * mm

        c.setFont("Helvetica", 9)
        subtitle_parts = [f"asOf={asof_d.isoformat()}", f"graceDays={graceDays}"]
        if center:
            subtitle_parts.append(f"Centre={center}")
        if zone:
            subtitle_parts.append(f"Zone={zone}")
        if sector:
            subtitle_parts.append(f"Secteur={sector}")
        c.drawString(margin_x, y, _ascii(" - ".join(subtitle_parts)))
        y -= 4 * mm
        c.drawString(margin_x, y, _ascii(f"Total: {len(cutoff_rows)}"))
        y -= 8 * mm

        headers = ["Telephone", "Compteur", "Nom", "Centre", "Zone", "Secteur", "Echeance", "Montant"]
        col_widths = [26, 24, 50, 18, 18, 18, 22, 18]
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

        draw_header()

        row_h = 4 * mm
        for it in cutoff_rows:
            if y < margin_y + 20 * mm:
                c.showPage()
                y = height - margin_y
                draw_header()

            values = [
                safe(it.get("phone")),
                safe(it.get("meterNumber")),
                safe(it.get("name")),
                safe(it.get("center")),
                safe(it.get("zone")),
                safe(it.get("sector")),
                safe(it.get("dueDate")),
                safe(it.get("amount")),
            ]
            for j, v in enumerate(values):
                c.drawString(x_positions[j], y, v)
            y -= row_h

        c.showPage()
        c.save()
        buf.seek(0)

        filename = "cutoffs_report.pdf"
        return StreamingResponse(
            buf,
            media_type="application/pdf",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    except HTTPException:
        raise
    except Exception as e:
        import traceback

        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erreur PDF: {type(e).__name__}: {e}",
        )


@router.get(
    "/stats",
    response_model=AdminStatsResponse,
    dependencies=[Depends(require_roles("admin"))],
)
async def get_admin_stats(
    db: AsyncIOMotorDatabase = Depends(get_database),
):
    internal_users = await db.users.count_documents({"role": {"$in": ["admin", "supervisor", "agent"]}})
    pre_registered_customers = await db.users.count_documents({"role": "customer", "isActive": False})
    return AdminStatsResponse(
        internalUsers=int(internal_users),
        preRegisteredCustomers=int(pre_registered_customers),
    )


@router.get(
    "/users",
    response_model=list[UserPublic],
    dependencies=[Depends(require_roles("admin"))],
)
async def list_users(
    db: AsyncIOMotorDatabase = Depends(get_database),
    q: str | None = Query(default=None),
    role: str | None = Query(default=None),
    active: bool | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=500),
):
    query: dict = {}
    if q:
        query["$or"] = [
            {"phone": {"$regex": q, "$options": "i"}},
            {"name": {"$regex": q, "$options": "i"}},
        ]
    if role:
        query["role"] = role
    if active is not None:
        query["isActive"] = active

    cursor = db.users.find(query, {"passwordHash": 0}).sort("createdAt", -1).limit(limit)
    items = await cursor.to_list(length=limit)
    for u in items:
        u["_id"] = str(u["_id"])
    return items


@router.patch(
    "/users/{user_id}",
    response_model=UserPublic,
    dependencies=[Depends(require_roles("admin"))],
)
async def update_user(
    user_id: str,
    payload: UpdateUserRequest,
    db: AsyncIOMotorDatabase = Depends(get_database),
):
    try:
        oid = ObjectId(user_id)
    except Exception:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Identifiant invalide.")

    user = await db.users.find_one({"_id": oid})
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Utilisateur introuvable.")

    update_doc: dict = {}
    if payload.name is not None:
        update_doc["name"] = payload.name
    if payload.role is not None:
        update_doc["role"] = payload.role
    if payload.isActive is not None:
        update_doc["isActive"] = payload.isActive
    if payload.center is not None:
        update_doc["center"] = payload.center
    if payload.zone is not None:
        update_doc["zone"] = payload.zone
    if payload.sector is not None:
        update_doc["sector"] = payload.sector
    if payload.assignedZones is not None:
        update_doc["assignedZones"] = [z.model_dump() for z in payload.assignedZones]

    if not update_doc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Aucun champ à modifier.")

    update_doc["updatedAt"] = datetime.now(timezone.utc)
    await db.users.update_one({"_id": oid}, {"$set": update_doc})

    updated = await db.users.find_one({"_id": oid}, {"passwordHash": 0})
    if not updated:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erreur mise à jour utilisateur.")

    updated["_id"] = str(updated["_id"])
    return updated


def _generate_temporary_password(length: int = 12) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


@router.post(
    "/users/{user_id}/reset-password",
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(require_roles("admin"))],
)
async def reset_password(
    user_id: str,
    db: AsyncIOMotorDatabase = Depends(get_database),
):
    try:
        oid = ObjectId(user_id)
    except Exception:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Identifiant invalide.")

    user = await db.users.find_one({"_id": oid})
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Utilisateur introuvable.")

    temp_password = _generate_temporary_password()
    now = datetime.now(timezone.utc)
    await db.users.update_one(
        {"_id": user["_id"]},
        {
            "$set": {
                "passwordHash": hash_password(temp_password),
                "mustChangePassword": True,
                "isActive": True,
                "updatedAt": now,
            }
        },
    )

    return {"temporaryPassword": temp_password}


@router.post(
    "/customers/pre-register",
    response_model=UserPublic,
    status_code=status.HTTP_201_CREATED,
)
async def pre_register_customer(
    payload: PreRegisterCustomerRequest,
    db: AsyncIOMotorDatabase = Depends(get_database),
    token_payload: dict = Depends(require_roles("admin")),
):
    existing = await db.users.find_one({"phone": payload.phone})
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Téléphone déjà utilisé.",
        )

    now = datetime.now(timezone.utc)
    doc = {
        "phone": payload.phone,
        "name": payload.name,
        "role": "customer",
        "passwordHash": None,
        "isActive": False,
        "mustChangePassword": False,
        "meterNumber": payload.meterNumber,
        "subscriberNumber": payload.subscriberNumber,
        "police": payload.police,
        "tariffCode": payload.tariffCode,
        "category": payload.category,
        "grouping": payload.grouping,
        "address": payload.address,
        "center": payload.center,
        "zone": payload.zone,
        "sector": payload.sector,
        "source": payload.source,
        "preRegisteredBy": token_payload.get("sub"),
        "createdAt": now,
        "updatedAt": now,
    }

    res = await db.users.insert_one(doc)
    created = await db.users.find_one({"_id": res.inserted_id})
    if not created:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erreur pré-enregistrement client.",
        )

    created.pop("passwordHash", None)
    created["_id"] = str(created["_id"])
    return created
