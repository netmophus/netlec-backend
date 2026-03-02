from typing import Annotated

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.core.settings import settings
from app.db.mongo import get_db

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")


async def get_database() -> AsyncIOMotorDatabase:
    return get_db()


def _decode_token(token: str) -> dict:
    try:
        payload = jwt.decode(token, settings.JWT_SECRET, algorithms=["HS256"])
        return payload
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token invalide.",
        )


async def get_current_user_payload(
    token: Annotated[str, Depends(oauth2_scheme)],
) -> dict:
    return _decode_token(token)


def require_roles(*allowed_roles: str):
    async def _dep(payload: Annotated[dict, Depends(get_current_user_payload)]) -> dict:
        role = payload.get("role")
        if role not in allowed_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Accès interdit.",
            )
        return payload

    return _dep
