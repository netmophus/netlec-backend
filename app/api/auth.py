from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Header, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from jose import JWTError, jwt
from motor.motor_asyncio import AsyncIOMotorDatabase

from bson import ObjectId

from app.api.models import ChangePasswordRequest, RegisterLookupRequest, RegisterRequest, TokenResponse, UserPublic
from app.core.deps import get_current_user_payload, get_database
from app.core.security import create_access_token, hash_password, verify_password
from app.core.settings import settings

router = APIRouter(prefix="/auth", tags=["auth"])


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
