from datetime import datetime, timezone

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query, status
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.api.models import BillingLineItem, CustomerBillingResponse, InitiatePaymentRequest, InvoicePublic, PaymentPublic, TariffPublic
from app.core.deps import get_current_user_payload, get_database, require_roles

router = APIRouter(prefix="/customer", tags=["customer"])


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

    items: list[BillingLineItem] = []
    total_consumption = 0
    total_amount = 0
    has_consumption = False
    has_amount = False

    for d in docs:
        cons = d.get("consumption")
        tc = d.get("tariffCode") or tariff_code
        amount: int | None = None
        if isinstance(cons, int):
            tiers = await _load_tariff_tiers_db(db)
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

    return CustomerBillingResponse(
        meterNumber=str(meter_number),
        tariffCode=str(tariff_code) if tariff_code is not None else None,
        totalConsumption=total_consumption if has_consumption else None,
        totalAmount=total_amount if has_amount else None,
        items=items,
    )


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

    invoices: list[InvoicePublic] = []
    for d in docs:
        invoices.append(
            InvoicePublic(
                id=str(d.get("invoiceId")),
                period=str(d.get("period")),
                date=str(d.get("date")),
                dueDate=d.get("dueDate"),
                meterNumber=str(d.get("meterNumber")),
                tariffCode=d.get("tariffCode"),
                consumption=d.get("consumption"),
                amount=d.get("amount"),
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

    meter_number = customer.get("meterNumber")
    if not meter_number or str(reading.get("meterNumber")) != str(meter_number):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Facture non autorisée.")

    cons = reading.get("consumption")
    tc = reading.get("tariffCode") or customer.get("tariffCode")
    amount: int | None = None
    if isinstance(cons, int):
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
