from typing import Any

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    APP_NAME: str = "nigelec-backend"
    ENV: str = "dev"

    MONGO_URI: str
    MONGO_DB: str

    JWT_SECRET: str
    JWT_EXPIRES_MIN: int = 1440

    CORS_ORIGINS: list[str] = [
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:3001",
        "http://127.0.0.1:3001",
    ]
    CORS_ORIGIN_REGEX: str | None = r"^https?://(localhost|127\.0\.0\.1)(:\d+)?$"

    ADMIN_PHONE: str = "90000000"
    ADMIN_PASSWORD: str = "Admin@123"
    ADMIN_NAME: str = "Admin"

    OCR_PROVIDER: str = "ocr_space"
    OCR_SPACE_API_KEY: str | None = None
    OCR_SPACE_ENDPOINT: str = "https://api.ocr.space/parse/image"
    GOOGLE_VISION_API_KEY: str | None = None

    CLOUDINARY_CLOUD_NAME: str | None = None
    CLOUDINARY_API_KEY: str | None = None
    CLOUDINARY_API_SECRET: str | None = None
    CLOUDINARY_UPLOAD_FOLDER: str = "nigelec/self-readings"

    VAT_RATE_PERCENT: int = 19
    TV_FEE_FCFA: int = 7000
    FSSP_FEE_FCFA: int = 1500
    LOYALTY_POINTS_PER_CONFORM_READING: int = 20
    LOYALTY_DRAW_THRESHOLD: int = 120

    @field_validator("CORS_ORIGINS", mode="before")
    @classmethod
    def parse_cors_origins(cls, value: Any) -> Any:
        if isinstance(value, str):
            return [origin.strip() for origin in value.split(",") if origin.strip()]
        return value


settings = Settings()
