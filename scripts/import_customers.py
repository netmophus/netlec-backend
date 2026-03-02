import argparse
import asyncio
import csv
from datetime import datetime, timezone
from pathlib import Path
import sys

from motor.motor_asyncio import AsyncIOMotorClient

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.core.settings import settings


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


async def main() -> None:
    parser = argparse.ArgumentParser(description="Import SI customers into MongoDB as pre-registered users.")
    parser.add_argument("csv_path", help="Path to CSV file")
    parser.add_argument("--delimiter", default=",", help="CSV delimiter (default: ,)")
    parser.add_argument("--dry-run", action="store_true", help="Do not write to DB")
    parser.add_argument(
        "--update-existing",
        action="store_true",
        help="If phone exists and role=customer, update fields (keeps passwordHash)",
    )
    args = parser.parse_args()

    path = Path(args.csv_path)
    if not path.exists():
        raise SystemExit(f"CSV not found: {path}")

    client = AsyncIOMotorClient(settings.MONGO_URI)
    db = client[settings.MONGO_DB]

    now = datetime.now(timezone.utc)

    inserted = 0
    updated = 0
    skipped = 0
    errors = 0

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=args.delimiter)
        if not reader.fieldnames:
            raise SystemExit("CSV has no header.")

        for i, row in enumerate(reader, start=2):
            try:
                phone = _pick(row, "phone", "Phone", "PHONE", "msisdn", "MSISDN")
                meter_number = _pick(row, "meterNumber", "meter_number", "MeterNumber", "METER_NUMBER")
                subscriber_number = _pick(row, "subscriberNumber", "subscriber_number", "SubscriberNumber")
                police = _pick(row, "police", "Police", "POLICE")

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
                    "name": _pick(row, "name", "fullName", "full_name", "customerName"),
                    "address": _pick(row, "address", "Address", "locality", "quartier"),
                    "tariffCode": _pick(row, "tariffCode", "tariff_code", "TariffCode"),
                    "category": _pick(row, "category", "Category"),
                    "grouping": _pick(row, "grouping", "Grouping"),
                    "center": _pick(row, "center", "Center", "agency"),
                    "zone": _pick(row, "zone", "Zone"),
                    "source": "SI_IMPORT",
                    "preRegisteredBy": None,
                    "createdAt": now,
                    "updatedAt": now,
                }

                if args.dry_run:
                    inserted += 1
                    continue

                existing = await db.users.find_one({"phone": phone})
                if existing:
                    if existing.get("role") != "customer":
                        skipped += 1
                        continue

                    if not args.update_existing:
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
                print(f"Error at CSV line {i}")

    print("Import finished")
    print(f"inserted={inserted}")
    print(f"updated={updated}")
    print(f"skipped={skipped}")
    print(f"errors={errors}")


if __name__ == "__main__":
    asyncio.run(main())
