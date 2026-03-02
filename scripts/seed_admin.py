import asyncio
from datetime import datetime, timezone
from pathlib import Path
import sys

from motor.motor_asyncio import AsyncIOMotorClient

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.core.security import hash_password
from app.core.settings import settings


async def main() -> None:
    client = AsyncIOMotorClient(settings.MONGO_URI)
    db = client[settings.MONGO_DB]

    await db.users.create_index("phone", unique=True)

    admin_phone = settings.ADMIN_PHONE
    admin_password = settings.ADMIN_PASSWORD
    admin_name = settings.ADMIN_NAME

    existing = await db.users.find_one({"phone": admin_phone})
    if existing:
        print("Admin already exists.")
        return

    now = datetime.now(timezone.utc)
    doc = {
        "phone": admin_phone,
        "name": admin_name,
        "role": "admin",
        "passwordHash": hash_password(admin_password),
        "isActive": True,
        "createdAt": now,
        "updatedAt": now,
    }
    await db.users.insert_one(doc)
    print("Admin created.")
    print(f"phone={admin_phone} password={admin_password}")


if __name__ == "__main__":
    asyncio.run(main())
