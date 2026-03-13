from datetime import date, datetime, timezone

from fastapi import HTTPException, status
from motor.motor_asyncio import AsyncIOMotorDatabase


def cycle_id_from_date(value: str) -> str:
    """Convertit une date YYYY-MM-DD en cycleId YYYY-MM."""
    try:
        parsed = date.fromisoformat(str(value))
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Date invalide (format attendu YYYY-MM-DD).",
        )
    return f"{parsed.year:04d}-{parsed.month:02d}"


def suggest_cycle_id() -> str:
    """
    Retourne un cycleId basé sur la date courante (YYYY-MM).
    Usage UNIQUEMENT pour proposer une valeur par défaut dans l'UI lors de la création.
    Ne doit JAMAIS servir de source de vérité pour le cycle actif.
    """
    now = datetime.now(timezone.utc)
    return f"{now.year:04d}-{now.month:02d}"


async def require_open_cycle(db: AsyncIOMotorDatabase) -> dict:
    """
    Garde centrale métier.
    Retourne le document complet du cycle actuellement OPEN.
    Lève une 409 si aucun cycle n'est ouvert.

    À appeler avant toute opération métier :
      - import clients / compteurs
      - génération de tournées
      - création de relevé (agent ou client)
    """
    cycle = await db.billing_cycles.find_one({"status": "OPEN"})
    if not cycle:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                "Aucun cycle n'est actuellement ouvert. "
                "L'administrateur doit ouvrir un cycle avant de continuer."
            ),
        )
    return cycle


async def assert_cycle_is_open(db: AsyncIOMotorDatabase, cycle_id: str) -> dict:
    """
    Vérifie qu'un cycle spécifique existe et est à l'état OPEN.
    Lève 404 si le cycle est introuvable, 409 s'il n'est pas OPEN.
    """
    normalized = str(cycle_id).strip()
    cycle = await db.billing_cycles.find_one({"cycleId": normalized})
    if not cycle:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Cycle {normalized} introuvable.",
        )
    if cycle.get("status") != "OPEN":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Le cycle {normalized} n'est pas ouvert (statut actuel : {cycle.get('status')}).",
        )
    return cycle


async def get_active_cycle_id(db: AsyncIOMotorDatabase) -> str:
    """
    Retourne le cycleId du cycle actuellement OPEN.
    Lève une 409 si aucun cycle n'est ouvert.

    Plus aucun fallback sur la date système.
    """
    cycle = await require_open_cycle(db)
    return str(cycle["cycleId"])


async def resolve_cycle_id(
    db: AsyncIOMotorDatabase,
    *,
    date_value: str | None = None,
    cycle_id: str | None = None,
) -> str:
    """
    Résout le cycleId pour une opération d'ÉCRITURE métier.

    Priorité :
      1. cycle_id explicitement fourni → vérifie qu'il est bien OPEN
      2. date_value fournie            → convertit en YYYY-MM et vérifie que ce cycle est OPEN
      3. Aucun des deux                → retourne le cycle actuellement OPEN (ou 409)

    N'auto-crée plus jamais un cycle.
    """
    if isinstance(cycle_id, str) and cycle_id.strip():
        resolved = cycle_id.strip()
        await assert_cycle_is_open(db, resolved)
        return resolved

    if isinstance(date_value, str) and date_value.strip():
        resolved = cycle_id_from_date(date_value)
        await assert_cycle_is_open(db, resolved)
        return resolved

    return await get_active_cycle_id(db)


def resolve_cycle_id_for_read(
    *,
    date_value: str | None = None,
    cycle_id: str | None = None,
    active_cycle_id: str | None = None,
) -> str | None:
    """
    Résout le cycleId pour une opération de LECTURE (rapport, listing).
    N'exige PAS que le cycle soit OPEN : les données des cycles CLOSED restent consultables.

    Priorité :
      1. cycle_id explicitement fourni → valide uniquement le format YYYY-MM
      2. date_value fournie            → convertit en YYYY-MM (validation format)
      3. active_cycle_id fourni        → utilise le cycle actif déjà résolu en amont
      4. Aucun                         → retourne None (pas de filtre cycle)

    N'effectue aucune requête MongoDB — appeler require_open_cycle() en amont si besoin
    du cycle actif (active_cycle_id).
    """
    if isinstance(cycle_id, str) and cycle_id.strip():
        normalized = cycle_id.strip()
        if len(normalized) != 7 or normalized[4] != "-" or not normalized[:4].isdigit() or not normalized[5:].isdigit():
            from fastapi import HTTPException, status as http_status
            raise HTTPException(
                status_code=http_status.HTTP_400_BAD_REQUEST,
                detail="cycleId invalide (format attendu YYYY-MM).",
            )
        return normalized

    if isinstance(date_value, str) and date_value.strip():
        return cycle_id_from_date(date_value)

    if isinstance(active_cycle_id, str) and active_cycle_id.strip():
        return active_cycle_id.strip()

    return None
