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


def _parse_int(value: str | None) -> int | None:
    if value is None:
        return None
    v = value.strip()
    if not v:
        return None
    try:
        return int(v)
    except Exception:
        return None


async def main() -> None:
    parser = argparse.ArgumentParser(description="Import meter sequences (routeOrder) into MongoDB meters collection.")
    parser.add_argument("csv_path", help="Path to CSV file")
    parser.add_argument("--delimiter", default=";", help="CSV delimiter (default: ;)")
    parser.add_argument("--dry-run", action="store_true", help="Do not write to DB")
    parser.add_argument(
        "--upsert",
        action="store_true",
        help="Create meter doc if not exists (otherwise skip missing meters)",
    )
    args = parser.parse_args()

    path = Path(args.csv_path)
    if not path.exists():
        raise SystemExit(f"CSV not found: {path}")

    client = AsyncIOMotorClient(settings.MONGO_URI)
    db = client[settings.MONGO_DB]

    now = datetime.now(timezone.utc)

    updated = 0
    inserted = 0
    skipped = 0
    errors = 0

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=args.delimiter)
        if not reader.fieldnames:
            raise SystemExit("CSV has no header.")

        for i, row in enumerate(reader, start=2):
            try:
                meter_number = _pick(row, "meterNumber", "meter_number", "MeterNumber", "METER_NUMBER")
                center = _pick(row, "center", "Center", "agency")
                zone = _pick(row, "zone", "Zone")
                sector = _pick(row, "sector", "Sector", "secteur", "Secteur")
                route_order = _parse_int(_pick(row, "routeOrder", "route_order", "sequence", "Sequence", "SEQ"))

                if not meter_number or route_order is None:
                    skipped += 1
                    continue

                if args.dry_run:
                    updated += 1
                    continue

                existing = await db.meters.find_one({"meterNumber": meter_number})
                if not existing and not args.upsert:
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
                        "subscriberNumber": None,
                        "police": None,
                        "address": None,
                        "createdAt": now,
                        "updatedAt": now,
                    }
                    await db.meters.insert_one(doc)
                    inserted += 1

            except Exception:
                errors += 1
                print(f"Error at CSV line {i}")

    print("Import finished")
    print(f"updated={updated}")
    print(f"inserted={inserted}")
    print(f"skipped={skipped}")
    print(f"errors={errors}")


if __name__ == "__main__":
    asyncio.run(main())
