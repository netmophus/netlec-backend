import logging
from datetime import date, datetime, timedelta, timezone
import calendar
import hashlib
from pathlib import Path
from uuid import uuid4

from bson import ObjectId
from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile, status
import httpx
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.api.models import (
    BillingLineItem,
    CustomerBillingResponse,
    CustomerLoyaltySummary,
    InitiatePaymentRequest,
    InvoicePublic,
    LoyaltyReadingHistoryItem,
    PaymentPublic,
    ReadingPublic,
    SelfReadingAvailabilityResponse,
    TariffPublic,
)
from app.core.cycles import resolve_cycle_id
from app.core.deps import get_current_user_payload, get_database, require_roles
from app.core.settings import settings

router = APIRouter(prefix="/customer", tags=["customer"])
logger = logging.getLogger(__name__)
SELF_READING_UPLOAD_DIR = Path("uploads") / "self-readings"
SELF_READING_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


def _cloudinary_signature(params: dict[str, str], api_secret: str) -> str:
    signing_pairs = [f"{k}={v}" for k, v in sorted(params.items()) if v is not None and str(v) != ""]
    to_sign = "&".join(signing_pairs) + str(api_secret)
    return hashlib.sha1(to_sign.encode("utf-8")).hexdigest()


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
        raw_code = d.get("code")
        if not isinstance(rk, int) or not isinstance(fk, int):
            continue
        to_kwh: int | None = None
        if isinstance(tk, int):
            to_kwh = int(tk)
        code: str | None = None
        if isinstance(raw_code, str):
            normalized = raw_code.strip().upper()
            if normalized:
                code = normalized
        tiers.append({"fromKwh": int(fk), "toKwh": to_kwh, "ratePerKwh": int(rk), "code": code})
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


def _compute_progressive_breakdown(consumption: int, tiers: list[dict]) -> list[dict]:
    if not isinstance(consumption, int) or consumption < 0:
        return []
    if consumption == 0 or not tiers:
        return []

    lines: list[dict] = []
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
        amount = int(kwh_in_tier * rate)
        code_raw = t.get("code")
        code = str(code_raw).strip().upper() if isinstance(code_raw, str) and str(code_raw).strip() else None
        lines.append(
            {
                "label": f"Energie {code}" if code else "Energie",
                "code": code,
                "kwh": kwh_in_tier,
                "ratePerKwh": int(rate),
                "amount": amount,
            }
        )
        if end is None:
            break

    return lines


async def _save_self_reading_photo(photo: UploadFile, meter_number: str, reading_date: str) -> str:
    content_type = str(photo.content_type or "").lower()
    allowed_types = {
        "image/jpeg": ".jpg",
        "image/jpg": ".jpg",
        "image/png": ".png",
        "image/webp": ".webp",
    }
    extension = allowed_types.get(content_type)
    if extension is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Photo invalide. Formats acceptés: JPG, PNG, WEBP.")

    payload = await photo.read()
    if not payload:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Photo vide.")
    if len(payload) > 8 * 1024 * 1024:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Photo trop volumineuse (max 8MB).")

    safe_meter = "".join(ch for ch in str(meter_number) if ch.isalnum() or ch in {"-", "_"}) or "meter"
    safe_date = str(reading_date).replace("/", "-").replace(":", "-")
    filename = f"{safe_date}-{safe_meter}-{uuid4().hex}{extension}"

    cloud_name = (settings.CLOUDINARY_CLOUD_NAME or "").strip()
    api_key = (settings.CLOUDINARY_API_KEY or "").strip()
    api_secret = (settings.CLOUDINARY_API_SECRET or "").strip()
    upload_folder = (settings.CLOUDINARY_UPLOAD_FOLDER or "nigelec/self-readings").strip().strip("/")

    if cloud_name and api_key and api_secret:
        timestamp = str(int(datetime.now(timezone.utc).timestamp()))
        signature_payload = {
            "folder": upload_folder,
            "public_id": filename.rsplit(".", 1)[0],
            "timestamp": timestamp,
        }
        signature = _cloudinary_signature(signature_payload, api_secret)
        upload_url = f"https://api.cloudinary.com/v1_1/{cloud_name}/image/upload"

        form_data = {
            "api_key": api_key,
            "timestamp": timestamp,
            "folder": upload_folder,
            "public_id": signature_payload["public_id"],
            "signature": signature,
        }

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    upload_url,
                    data=form_data,
                    files={"file": (filename, payload, content_type)},
                )
            if response.status_code >= 400:
                logger.error("Cloudinary upload failed: %s", response.text)
                raise HTTPException(
                    status_code=status.HTTP_502_BAD_GATEWAY,
                    detail="Upload photo Cloudinary échoué.",
                )

            data = response.json()
            secure_url = data.get("secure_url")
            if isinstance(secure_url, str) and secure_url.strip():
                return secure_url.strip()

            logger.error("Cloudinary upload response missing secure_url: %s", data)
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Upload photo Cloudinary invalide.",
            )
        except HTTPException:
            raise
        except Exception as e:
            logger.exception("Cloudinary upload exception: %s", e)
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Erreur d'upload photo vers Cloudinary.",
            )

    destination = SELF_READING_UPLOAD_DIR / filename
    destination.write_bytes(payload)
    return f"/uploads/self-readings/{filename}"


def _build_invoice_detail(consumption: int | None, tiers: list[dict], base_amount: int | None) -> dict:
    energy_amount = int(base_amount) if isinstance(base_amount, int) else None
    breakdown: list[dict] = []
    if isinstance(consumption, int):
        breakdown = _compute_progressive_breakdown(int(consumption), tiers)

    if energy_amount is None and breakdown:
        energy_amount = int(sum(int(item.get("amount") or 0) for item in breakdown))

    if energy_amount is None:
        return {
            "energyAmount": None,
            "tvFee": None,
            "fsspFee": None,
            "subtotal": None,
            "taxAmount": None,
            "totalAmount": None,
            "breakdown": [],
            "amount": None,
        }

    vat_rate_percent = max(0, int(settings.VAT_RATE_PERCENT or 0))
    tv_fee = max(0, int(settings.TV_FEE_FCFA or 0))
    fssp_fee = max(0, int(settings.FSSP_FEE_FCFA or 0))
    subtotal = int(energy_amount)
    tax_amount = int(round(subtotal * vat_rate_percent / 100.0))
    total_amount = int(subtotal + tax_amount + tv_fee + fssp_fee)

    if not breakdown:
        breakdown = [
            {
                "label": "Energie",
                "code": None,
                "kwh": int(consumption) if isinstance(consumption, int) else None,
                "ratePerKwh": None,
                "amount": int(energy_amount),
            }
        ]

    breakdown.append(
        {
            "label": f"TVA ({vat_rate_percent}%)",
            "code": "VAT",
            "kwh": None,
            "ratePerKwh": None,
            "amount": int(tax_amount),
        }
    )
    breakdown.append(
        {
            "label": "Taxe fixe",
            "code": "TV",
            "kwh": None,
            "ratePerKwh": None,
            "amount": int(tv_fee),
        }
    )
    breakdown.append(
        {
            "label": "Charge fixe 2",
            "code": "FSSP",
            "kwh": None,
            "ratePerKwh": None,
            "amount": int(fssp_fee),
        }
    )

    return {
        "energyAmount": int(energy_amount),
        "tvFee": int(tv_fee),
        "fsspFee": int(fssp_fee),
        "subtotal": int(subtotal),
        "taxAmount": int(tax_amount),
        "totalAmount": int(total_amount),
        "breakdown": breakdown,
        "amount": int(total_amount),
    }


def _infer_tariff_code_from_consumption(consumption: int, tiers: list[dict]) -> str | None:
    if not isinstance(consumption, int) or consumption < 0:
        return None

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

        if consumption < start:
            continue
        if end is not None and consumption > end:
            continue

        code = t.get("code")
        if isinstance(code, str):
            normalized = code.strip().upper()
            if normalized:
                return normalized
        return None

    return None


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


async def _apply_self_submission_to_tour(
    db: AsyncIOMotorDatabase,
    cycle_id: str,
    meter_number: str,
    reading_date: str,
    submitted_at: datetime,
) -> tuple[str | None, str | None]:
    tour = await db.tours.find_one(
        {"cycleId": cycle_id, "date": str(reading_date), "items.meterNumber": str(meter_number)},
        {"_id": 1, "agentId": 1},
    )
    if not tour:
        return None, None

    await db.tours.update_one(
        {"_id": tour["_id"]},
        {
            "$set": {
                "items.$[it].selfSubmittedByCustomer": True,
                "items.$[it].selfSubmittedAt": submitted_at,
                "updatedAt": submitted_at,
            }
        },
        array_filters=[{"it.meterNumber": str(meter_number)}],
    )
    return str(tour.get("_id")), str(tour.get("agentId")) if tour.get("agentId") is not None else None


async def _upsert_invoice_from_reading(
    db: AsyncIOMotorDatabase,
    reading: dict,
    customer: dict,
    tiers: list[dict],
    now: datetime,
) -> None:
    reading_id = str(reading.get("_id"))
    cons = reading.get("consumption") if isinstance(reading.get("consumption"), int) else None
    tc = reading.get("tariffCode") if isinstance(reading.get("tariffCode"), str) else None
    amount: int | None = None

    if isinstance(cons, int):
        amount = _compute_progressive_amount(int(cons), tiers)
        if amount is None:
            rate = await _tariff_rate_per_kwh_db(db, tc)
            if rate is not None:
                amount = int(int(cons) * int(rate))

    grace_days = 10
    due_d = _end_of_month_due_date(str(reading.get("date")), grace_days)
    due_date_str = due_d.isoformat() if due_d else None

    status_str = "DUE"
    if due_d is not None and now.date() > due_d:
        status_str = "OVERDUE"

    detail = _build_invoice_detail(cons if isinstance(cons, int) else None, tiers, amount)
    invoice_id = f"INV-{reading_id}"

    await db.invoices.update_one(
        {"invoiceId": invoice_id},
        {
            "$set": {
                "invoiceId": invoice_id,
                "cycleId": str(reading.get("cycleId")) if isinstance(reading.get("cycleId"), str) else None,
                "readingId": reading_id,
                "customerId": str(customer.get("_id")),
                "meterNumber": str(reading.get("meterNumber")),
                "period": str(reading.get("date"))[:7] if len(str(reading.get("date"))) >= 7 else str(reading.get("date")),
                "date": str(reading.get("date")),
                "dueDate": due_date_str,
                "tariffCode": str(tc) if tc is not None else None,
                "consumption": cons if isinstance(cons, int) else None,
                "amount": detail["amount"],
                "energyAmount": detail["energyAmount"],
                "tvFee": detail["tvFee"],
                "fsspFee": detail["fsspFee"],
                "subtotal": detail["subtotal"],
                "taxAmount": detail["taxAmount"],
                "totalAmount": detail["totalAmount"],
                "breakdown": detail["breakdown"],
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


@router.post(
    "/readings/self",
    response_model=ReadingPublic,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_roles("customer"))],
)
async def create_self_reading(
    date: str = Form(...),
    newIndex: int = Form(...),
    photo: UploadFile | None = File(default=None),
    gpsLat: float | None = Form(default=None),
    gpsLng: float | None = Form(default=None),
    gpsAccuracy: float | None = Form(default=None),
    gpsMissingReason: str | None = Form(default=None),
    token_payload: dict = Depends(get_current_user_payload),
    db: AsyncIOMotorDatabase = Depends(get_database),
):
    customer_id = token_payload.get("sub")
    if not customer_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    try:
        customer_oid = ObjectId(str(customer_id))
    except Exception:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    customer = await db.users.find_one({"_id": customer_oid, "role": "customer"})
    if not customer:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Utilisateur introuvable.")

    meter_number = customer.get("meterNumber")
    if not meter_number:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Aucun compteur rattaché à ce compte.")

    cycle_id = await resolve_cycle_id(db, date_value=str(date))

    existing = await db.readings.find_one({"cycleId": cycle_id, "meterNumber": str(meter_number)})
    if existing:
        existing_source = str(existing.get("source") or "AGENT").upper()
        existing_date = str(existing.get("date") or "-")
        if existing_source == "CUSTOMER":
            detail = f"Vous avez déjà envoyé un relevé pour ce cycle (date: {existing_date})."
        else:
            detail = f"Relevé déjà effectué par un agent pour ce cycle (date: {existing_date})."
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=detail)

    old_index = customer.get("oldIndex")
    if isinstance(old_index, int) and int(newIndex) < old_index:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Nouvel index inférieur à l'ancien index.")

    consumption: int | None = None
    if isinstance(old_index, int):
        consumption = int(int(newIndex) - old_index)

    tiers = await _load_tariff_tiers_db(db)
    tariff_code: str | None = None
    raw_tariff = customer.get("tariffCode")
    if isinstance(raw_tariff, str) and raw_tariff.strip():
        tariff_code = raw_tariff.strip().upper()
    if tariff_code is None and isinstance(consumption, int):
        tariff_code = _infer_tariff_code_from_consumption(int(consumption), tiers)

    now = datetime.now(timezone.utc)
    tour_id, assigned_agent_id = await _apply_self_submission_to_tour(
        db,
        cycle_id=cycle_id,
        meter_number=str(meter_number),
        reading_date=str(date),
        submitted_at=now,
    )
    if not tour_id:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Aucune tournée active ne contient ce compteur pour cette date. Relevé client non autorisé.",
        )

    photo_url: str | None = None
    if photo is not None:
        photo_url = await _save_self_reading_photo(photo, meter_number=str(meter_number), reading_date=str(date))

    has_photo = isinstance(photo_url, str) and bool(photo_url)
    points_per_conform = max(0, int(settings.LOYALTY_POINTS_PER_CONFORM_READING or 20))
    points_awarded = points_per_conform if has_photo else 0
    self_status = "CONFORM" if points_awarded > 0 else "PHOTO_MISSING"

    gps_doc: dict | None = None
    gps_missing = True
    if isinstance(gpsLat, float) and isinstance(gpsLng, float):
        gps_doc = {
            "lat": float(gpsLat),
            "lng": float(gpsLng),
            "accuracy": float(gpsAccuracy) if isinstance(gpsAccuracy, float) else None,
        }
        gps_missing = False

    reading_doc = {
        "cycleId": cycle_id,
        "date": str(date),
        "tourId": tour_id,
        "agentId": assigned_agent_id,
        "meterNumber": str(meter_number),
        "oldIndex": old_index,
        "newIndex": int(newIndex),
        "consumption": consumption,
        "tariffCode": tariff_code,
        "source": "CUSTOMER",
        "selfReadingStatus": self_status,
        "photoUrl": photo_url,
        "gps": gps_doc,
        "gpsMissing": gps_missing,
        "gpsMissingReason": gpsMissingReason.strip() if isinstance(gpsMissingReason, str) and gpsMissingReason.strip() else None,
        "loyaltyPointsAwarded": int(points_awarded),
        "createdAt": now,
        "updatedAt": now,
    }

    insert_res = await db.readings.insert_one(reading_doc)
    created = await db.readings.find_one({"_id": insert_res.inserted_id})
    if not created:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erreur création relevé.")

    user_update: dict = {"oldIndex": int(newIndex), "updatedAt": now}
    await db.users.update_one(
        {"_id": customer_oid},
        {
            "$set": user_update,
            "$inc": {
                "loyalty.pointsSemester": int(points_awarded),
                "loyalty.pointsLifetime": int(points_awarded),
            },
        },
    )

    created["_id"] = insert_res.inserted_id
    await _upsert_invoice_from_reading(db, created, customer, tiers, now)

    created["_id"] = str(created["_id"])
    return ReadingPublic(**created)


@router.get(
    "/loyalty",
    response_model=CustomerLoyaltySummary,
    dependencies=[Depends(require_roles("customer"))],
)
async def get_customer_loyalty(
    token_payload: dict = Depends(get_current_user_payload),
    db: AsyncIOMotorDatabase = Depends(get_database),
    limit: int = Query(default=30, ge=1, le=120),
):
    customer_id = token_payload.get("sub")
    if not customer_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    try:
        customer_oid = ObjectId(str(customer_id))
    except Exception:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    customer = await db.users.find_one({"_id": customer_oid, "role": "customer"})
    if not customer:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Utilisateur introuvable.")

    loyalty = customer.get("loyalty") if isinstance(customer.get("loyalty"), dict) else {}
    points_semester = int(loyalty.get("pointsSemester") or 0)
    points_lifetime = int(loyalty.get("pointsLifetime") or 0)
    excluded = bool(loyalty.get("excluded") or False)
    threshold = max(1, int(settings.LOYALTY_DRAW_THRESHOLD or 120))
    points_per = max(0, int(settings.LOYALTY_POINTS_PER_CONFORM_READING or 20))

    meter_number = customer.get("meterNumber")
    history: list[LoyaltyReadingHistoryItem] = []
    if meter_number:
        cursor = (
            db.readings.find(
                {"meterNumber": str(meter_number), "source": "CUSTOMER"},
                {"date": 1, "meterNumber": 1, "newIndex": 1, "selfReadingStatus": 1, "loyaltyPointsAwarded": 1, "createdAt": 1},
            )
            .sort([("createdAt", -1)])
            .limit(limit)
        )
        async for d in cursor:
            history.append(
                LoyaltyReadingHistoryItem(
                    date=str(d.get("date")),
                    meterNumber=str(d.get("meterNumber")),
                    newIndex=int(d.get("newIndex") or 0),
                    status=str(d.get("selfReadingStatus") or "UNKNOWN"),
                    pointsAwarded=int(d.get("loyaltyPointsAwarded") or 0),
                    createdAt=d.get("createdAt"),
                )
            )

    return CustomerLoyaltySummary(
        pointsSemester=points_semester,
        pointsLifetime=points_lifetime,
        eligibleForDraw=(not excluded and points_semester >= threshold),
        excluded=excluded,
        threshold=threshold,
        pointsPerConformReading=points_per,
        history=history,
    )


@router.get(
    "/readings/self/availability",
    response_model=SelfReadingAvailabilityResponse,
    dependencies=[Depends(require_roles("customer"))],
)
async def get_self_reading_availability(
    token_payload: dict = Depends(get_current_user_payload),
    db: AsyncIOMotorDatabase = Depends(get_database),
    dateValue: str | None = Query(default=None),
):
    customer_id = token_payload.get("sub")
    if not customer_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    try:
        customer_oid = ObjectId(str(customer_id))
    except Exception:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    customer = await db.users.find_one({"_id": customer_oid, "role": "customer"})
    if not customer:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Utilisateur introuvable.")

    meter_number = customer.get("meterNumber")
    old_index = customer.get("oldIndex") if isinstance(customer.get("oldIndex"), int) else None
    target_date = str(dateValue).strip() if isinstance(dateValue, str) and str(dateValue).strip() else date.today().isoformat()
    cycle_id = await resolve_cycle_id(db, date_value=target_date)
    if not meter_number:
        return SelfReadingAvailabilityResponse(
            date=target_date,
            meterNumber=None,
            oldIndex=old_index,
            canSubmit=False,
            reason="Aucun compteur rattaché à ce compte.",
        )

    existing_reading = await db.readings.find_one(
        {"cycleId": cycle_id, "meterNumber": str(meter_number)},
        {"_id": 1, "source": 1, "date": 1},
    )
    if existing_reading:
        existing_source = str(existing_reading.get("source") or "AGENT").upper()
        existing_date = str(existing_reading.get("date") or target_date)
        if existing_source == "CUSTOMER":
            reason = f"Vous avez déjà envoyé votre relevé pour ce cycle (date: {existing_date})."
        else:
            reason = f"Un agent a déjà relevé votre compteur pour ce cycle (date: {existing_date})."
        return SelfReadingAvailabilityResponse(
            date=target_date,
            meterNumber=str(meter_number),
            oldIndex=old_index,
            canSubmit=False,
            reason=reason,
        )

    has_tour = await db.tours.find_one(
        {"cycleId": cycle_id, "date": target_date, "items.meterNumber": str(meter_number)},
        {"_id": 1},
    )
    if not has_tour:
        return SelfReadingAvailabilityResponse(
            date=target_date,
            meterNumber=str(meter_number),
            oldIndex=old_index,
            canSubmit=False,
            reason="Tournée non générée pour ce compteur à cette date.",
        )

    return SelfReadingAvailabilityResponse(
        date=target_date,
        meterNumber=str(meter_number),
        oldIndex=old_index,
        canSubmit=True,
        reason=None,
    )


@router.get(
    "/tariffs",
    response_model=list[TariffPublic],
    dependencies=[Depends(require_roles("customer"))],
)
async def list_customer_tariffs(
    db: AsyncIOMotorDatabase = Depends(get_database),
    limit: int = Query(default=50, ge=1, le=200),
):
    cursor = db.tariffs.find({}).sort([("fromKwh", 1), ("code", 1)]).limit(limit)
    docs = await cursor.to_list(length=limit)
    out: list[TariffPublic] = []
    for d in docs:
        d["_id"] = str(d["_id"])
        out.append(TariffPublic(**d))
    return out


@router.get(
    "/billing",
    response_model=CustomerBillingResponse,
    dependencies=[Depends(require_roles("customer"))],
)
async def get_my_billing(
    token_payload: dict = Depends(get_current_user_payload),
    db: AsyncIOMotorDatabase = Depends(get_database),
    limit: int = Query(default=50, ge=1, le=200),
):
    customer_id = token_payload.get("sub")
    if not customer_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    try:
        customer_oid = ObjectId(str(customer_id))
    except Exception:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    customer = await db.users.find_one({"_id": customer_oid})
    if not customer:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Utilisateur introuvable.")

    meter_number = customer.get("meterNumber")
    tariff_code = customer.get("tariffCode")

    if not meter_number:
        return CustomerBillingResponse(meterNumber=None, tariffCode=tariff_code, totalConsumption=None, totalAmount=None, items=[])

    cursor = db.readings.find({"meterNumber": str(meter_number)}).sort([("date", -1), ("createdAt", -1)]).limit(limit)
    docs = await cursor.to_list(length=limit)

    tiers = await _load_tariff_tiers_db(db)
    logger.info(
        "customer.billing.start customer_id=%s meter=%s readings=%s tiers=%s customer_tariff=%s",
        str(customer_oid),
        str(meter_number),
        len(docs),
        len(tiers),
        str(tariff_code) if tariff_code is not None else None,
    )
    items: list[BillingLineItem] = []
    total_consumption = 0
    total_amount = 0
    has_consumption = False
    has_amount = False
    inferred_top_tariff_code: str | None = None

    for d in docs:
        cons = d.get("consumption")
        tc = d.get("tariffCode") or tariff_code
        if tc is None and isinstance(cons, int):
            tc = _infer_tariff_code_from_consumption(int(cons), tiers)
        if inferred_top_tariff_code is None and isinstance(tc, str) and tc.strip():
            inferred_top_tariff_code = tc.strip().upper()
        amount: int | None = None
        if isinstance(cons, int):
            amount = _compute_progressive_amount(int(cons), tiers)
            if amount is None:
                rate = await _tariff_rate_per_kwh_db(db, tc)
                if rate is not None:
                    amount = int(int(cons) * int(rate))
            if amount is not None:
                total_amount += int(amount)
                has_amount = True

        if isinstance(cons, int):
            total_consumption += cons
            has_consumption = True

        items.append(
            BillingLineItem(
                date=str(d.get("date")),
                meterNumber=str(d.get("meterNumber")),
                oldIndex=d.get("oldIndex"),
                newIndex=int(d.get("newIndex")),
                consumption=cons if isinstance(cons, int) else None,
                tariffCode=str(tc) if tc is not None else None,
                amount=amount,
                createdAt=d.get("createdAt"),
            )
        )
        logger.info(
            "customer.billing.item meter=%s date=%s consumption=%s tariff=%s amount=%s",
            str(d.get("meterNumber")),
            str(d.get("date")),
            int(cons) if isinstance(cons, int) else None,
            str(tc).strip().upper() if tc is not None else None,
            int(amount) if isinstance(amount, int) else None,
        )

    response = CustomerBillingResponse(
        meterNumber=str(meter_number),
        tariffCode=(str(tariff_code).strip().upper() if tariff_code is not None and str(tariff_code).strip() else inferred_top_tariff_code),
        totalConsumption=total_consumption if has_consumption else None,
        totalAmount=total_amount if has_amount else None,
        items=items,
    )
    logger.info(
        "customer.billing.summary meter=%s tariff=%s total_consumption=%s total_amount=%s items=%s",
        response.meterNumber,
        response.tariffCode,
        response.totalConsumption,
        response.totalAmount,
        len(response.items),
    )
    return response


@router.get(
    "/invoices",
    response_model=list[InvoicePublic],
    dependencies=[Depends(require_roles("customer"))],
)
async def list_my_invoices(
    token_payload: dict = Depends(get_current_user_payload),
    db: AsyncIOMotorDatabase = Depends(get_database),
    limit: int = Query(default=24, ge=1, le=120),
):
    customer_id = token_payload.get("sub")
    if not customer_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    try:
        customer_oid = ObjectId(str(customer_id))
    except Exception:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    customer = await db.users.find_one({"_id": customer_oid})
    if not customer:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Utilisateur introuvable.")

    meter_number = customer.get("meterNumber")
    if not meter_number:
        return []

    now_iso = datetime.now(timezone.utc).date().isoformat()
    refreshed = await db.invoices.update_many(
        {"customerId": str(customer.get("_id")), "status": "DUE", "dueDate": {"$lt": now_iso}},
        {"$set": {"status": "OVERDUE", "updatedAt": datetime.now(timezone.utc)}},
    )

    cursor = (
        db.invoices.find({"customerId": str(customer.get("_id"))})
        .sort([("date", -1), ("createdAt", -1)])
        .limit(limit)
    )
    docs = await cursor.to_list(length=limit)

    tiers = await _load_tariff_tiers_db(db)
    invoices: list[InvoicePublic] = []
    for d in docs:
        detail = _build_invoice_detail(
            d.get("consumption") if isinstance(d.get("consumption"), int) else None,
            tiers,
            d.get("energyAmount") if isinstance(d.get("energyAmount"), int) else (d.get("amount") if isinstance(d.get("amount"), int) else None),
        )
        breakdown = d.get("breakdown") if isinstance(d.get("breakdown"), list) and d.get("breakdown") else detail["breakdown"]
        amount_total = d.get("totalAmount") if isinstance(d.get("totalAmount"), int) else detail["totalAmount"]
        energy_amount = d.get("energyAmount") if isinstance(d.get("energyAmount"), int) else detail["energyAmount"]
        tv_fee = d.get("tvFee") if isinstance(d.get("tvFee"), int) else detail["tvFee"]
        fssp_fee = d.get("fsspFee") if isinstance(d.get("fsspFee"), int) else detail["fsspFee"]
        subtotal = d.get("subtotal") if isinstance(d.get("subtotal"), int) else detail["subtotal"]
        tax_amount = d.get("taxAmount") if isinstance(d.get("taxAmount"), int) else detail["taxAmount"]
        invoices.append(
            InvoicePublic(
                id=str(d.get("invoiceId")),
                period=str(d.get("period")),
                date=str(d.get("date")),
                dueDate=d.get("dueDate"),
                meterNumber=str(d.get("meterNumber")),
                tariffCode=d.get("tariffCode"),
                consumption=d.get("consumption"),
                amount=amount_total,
                energyAmount=energy_amount,
                tvFee=tv_fee,
                fsspFee=fssp_fee,
                subtotal=subtotal,
                taxAmount=tax_amount,
                totalAmount=amount_total,
                breakdown=breakdown,
                status=str(d.get("status")),
                readingId=str(d.get("readingId")),
            )
        )

    return invoices


@router.post(
    "/payments/initiate",
    response_model=PaymentPublic,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_roles("customer"))],
)
async def initiate_payment(
    payload: InitiatePaymentRequest,
    token_payload: dict = Depends(get_current_user_payload),
    db: AsyncIOMotorDatabase = Depends(get_database),
):
    customer_id = token_payload.get("sub")
    if not customer_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    try:
        customer_oid = ObjectId(str(customer_id))
    except Exception:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    customer = await db.users.find_one({"_id": customer_oid})
    if not customer:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Utilisateur introuvable.")

    invoice_id = payload.invoiceId
    if not invoice_id.startswith("INV-"):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="invoiceId invalide.")
    reading_id = invoice_id.replace("INV-", "", 1)

    try:
        reading_oid = ObjectId(reading_id)
    except Exception:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="invoiceId invalide.")

    reading = await db.readings.find_one({"_id": reading_oid})
    if not reading:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Facture introuvable.")

    invoice = await db.invoices.find_one({"invoiceId": invoice_id, "customerId": str(customer.get("_id"))})

    meter_number = customer.get("meterNumber")
    if not meter_number or str(reading.get("meterNumber")) != str(meter_number):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Facture non autorisée.")

    cons = reading.get("consumption")
    tc = reading.get("tariffCode") or customer.get("tariffCode")
    amount: int | None = None
    if invoice and isinstance(invoice.get("totalAmount"), int):
        amount = int(invoice.get("totalAmount"))
    elif invoice and isinstance(invoice.get("amount"), int):
        amount = int(invoice.get("amount"))
    elif isinstance(cons, int):
        tiers = await _load_tariff_tiers_db(db)
        amount = _compute_progressive_amount(int(cons), tiers)
        if amount is None:
            rate = await _tariff_rate_per_kwh_db(db, tc)
            if rate is not None:
                amount = int(int(cons) * int(rate))

    existing = await db.payments.find_one({"customerId": str(customer.get("_id")), "invoiceId": invoice_id, "status": "SUCCEEDED"})
    if existing:
        existing["_id"] = str(existing["_id"])
        return PaymentPublic(**existing)

    now = datetime.now(timezone.utc)
    doc = {
        "invoiceId": invoice_id,
        "customerId": str(customer.get("_id")),
        "provider": payload.provider,
        "amount": amount,
        "status": "SUCCEEDED",
        "readingId": reading_id,
        "createdAt": now,
        "updatedAt": now,
        "providerRef": f"SIM-{payload.provider}-{reading_id[-6:]}",
    }

    res = await db.payments.insert_one(doc)
    created = await db.payments.find_one({"_id": res.inserted_id})
    if not created:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erreur paiement.")

    await db.invoices.update_one(
        {"invoiceId": invoice_id, "customerId": str(customer.get("_id"))},
        {"$set": {"status": "PAID", "updatedAt": datetime.now(timezone.utc)}},
    )

    created["_id"] = str(created["_id"])
    return PaymentPublic(**created)


@router.get(
    "/payments",
    response_model=list[PaymentPublic],
    dependencies=[Depends(require_roles("customer"))],
)
async def list_my_payments(
    token_payload: dict = Depends(get_current_user_payload),
    db: AsyncIOMotorDatabase = Depends(get_database),
    limit: int = Query(default=50, ge=1, le=200),
):
    customer_id = token_payload.get("sub")
    if not customer_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    try:
        customer_oid = ObjectId(str(customer_id))
    except Exception:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    customer = await db.users.find_one({"_id": customer_oid})
    if not customer:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Utilisateur introuvable.")

    cursor = db.payments.find({"customerId": str(customer.get("_id"))}).sort([("createdAt", -1)]).limit(limit)
    docs = await cursor.to_list(length=limit)
    out: list[PaymentPublic] = []
    for d in docs:
        d["_id"] = str(d["_id"])
        out.append(PaymentPublic(**d))
    return out
