from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Header, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from jose import JWTError, jwt
from motor.motor_asyncio import AsyncIOMotorDatabase

from bson import ObjectId

from app.api.models import ChangePasswordRequest, PortalSettingsPublic, RegisterLookupRequest, RegisterRequest, TokenResponse, UserPublic
from app.core.deps import get_current_user_payload, get_database
from app.core.security import create_access_token, hash_password, verify_password
from app.core.settings import settings

router = APIRouter(prefix="/auth", tags=["auth"])

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


def _as_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    return text if text else None


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


@router.get("/portal-settings", response_model=PortalSettingsPublic)
async def get_portal_settings(db: AsyncIOMotorDatabase = Depends(get_database)):
    doc = await db.portal_settings.find_one({"key": "default"})
    source = (doc or {}).get("settings") if isinstance((doc or {}).get("settings"), dict) else (doc or {})

    return {
        "logoUrl": _as_text(source.get("logoUrl")),
        "facebookUrl": _as_text(source.get("facebookUrl")),
        "linkedinUrl": _as_text(source.get("linkedinUrl")),
        "xUrl": _as_text(source.get("xUrl")),
        "youtubeUrl": _as_text(source.get("youtubeUrl")),
        "supportPhone": _as_text(source.get("supportPhone")),
        "supportWhatsapp": _as_text(source.get("supportWhatsapp")),
        "latestAnnouncements": _normalize_announcements(source.get("latestAnnouncements")),
    }


@router.get("/me", response_model=UserPublic)
async def me(
    payload: dict = Depends(get_current_user_payload),
    db: AsyncIOMotorDatabase = Depends(get_database),
):
    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    try:
        oid = ObjectId(str(user_id))
    except Exception:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    user = await db.users.find_one({"_id": oid})
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Utilisateur introuvable.")

    user.pop("passwordHash", None)
    user["_id"] = str(user["_id"])
    return user


@router.post("/register", response_model=UserPublic, status_code=status.HTTP_201_CREATED)
async def register(payload: RegisterRequest, db: AsyncIOMotorDatabase = Depends(get_database)):
    existing = await db.users.find_one({"phone": payload.phone})
    if not existing:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Compte non pré-enregistré. Contactez NIGELEC.",
        )

    if existing.get("role") != "customer":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Ce numéro est déjà utilisé par un compte interne.",
        )

    if existing.get("passwordHash"):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Compte déjà activé.",
        )

    now = datetime.now(timezone.utc)
    update_doc = {
        "passwordHash": hash_password(payload.password),
        "isActive": True,
        "updatedAt": now,
    }
    if payload.name and not existing.get("name"):
        update_doc["name"] = payload.name

    await db.users.update_one({"_id": existing["_id"]}, {"$set": update_doc})

    created = await db.users.find_one({"_id": existing["_id"]})
    if not created:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erreur activation utilisateur.")

    created.pop("passwordHash", None)
    created["_id"] = str(created["_id"])
    return created


@router.post("/register/lookup", response_model=UserPublic)
async def register_lookup(payload: RegisterLookupRequest, db: AsyncIOMotorDatabase = Depends(get_database)):
    existing = await db.users.find_one({"phone": payload.phone})
    if not existing:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Compte non pré-enregistré. Contactez NIGELEC.",
        )

    if existing.get("role") != "customer":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Ce numéro est déjà utilisé par un compte interne.",
        )

    if existing.get("passwordHash"):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Compte déjà activé.",
        )

    existing.pop("passwordHash", None)
    existing["_id"] = str(existing["_id"])
    return existing


@router.post("/login", response_model=TokenResponse)
async def login(form: OAuth2PasswordRequestForm = Depends(), db: AsyncIOMotorDatabase = Depends(get_database)):
    user = await db.users.find_one({"phone": form.username})
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Identifiants invalides.")
    if not user.get("isActive", False):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Compte désactivé.")
    if not user.get("passwordHash"):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Compte non activé.")
    if not verify_password(form.password, user.get("passwordHash", "")):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Identifiants invalides.")

    token = create_access_token(subject=str(user["_id"]), role=user["role"])
    return TokenResponse(access_token=token, mustChangePassword=bool(user.get("mustChangePassword", False)))


@router.post("/change-password", status_code=status.HTTP_204_NO_CONTENT)
async def change_password(
    payload: ChangePasswordRequest,
    db: AsyncIOMotorDatabase = Depends(get_database),
    authorization: str | None = Header(default=None),
):
    if not authorization:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token manquant.")

    parts = authorization.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    token = parts[1]
    try:
        token_payload = jwt.decode(token, settings.JWT_SECRET, algorithms=["HS256"])
    except JWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    user_id = token_payload.get("sub")
    if not user_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    try:
        oid = ObjectId(str(user_id))
    except Exception:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token invalide.")

    user = await db.users.find_one({"_id": oid})
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Utilisateur introuvable.")

    if not user.get("passwordHash"):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Compte non activé.")
    if not verify_password(payload.currentPassword, user.get("passwordHash", "")):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Mot de passe actuel invalide.")

    now = datetime.now(timezone.utc)
    await db.users.update_one(
        {"_id": user["_id"]},
        {"$set": {"passwordHash": hash_password(payload.newPassword), "mustChangePassword": False, "updatedAt": now}},
    )

    return None
