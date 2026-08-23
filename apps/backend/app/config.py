import os
from dataclasses import dataclass, field


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(
        f"{name} must be one of true/false, 1/0, yes/no, or on/off"
    )


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return default if value is None else int(value)


def _env_csv(name: str, default: tuple[str, ...] = ()) -> tuple[str, ...]:
    value = os.getenv(name)
    if value is None:
        return default
    return tuple(item.strip().rstrip("/") for item in value.split(",") if item.strip())


@dataclass(frozen=True, slots=True)
class Settings:
    database_url: str = field(
        default_factory=lambda: os.getenv(
            "DATABASE_URL",
            "postgresql+psycopg2://postgres:postgres@db:5432/geo",
        ),
        repr=False,
    )
    readiness_check_database: bool = field(default_factory=lambda: _env_bool("READINESS_CHECK_DATABASE", False))
    localization_service_factory: str = field(
        default_factory=lambda: os.getenv(
            "GEOSNAP_LOCALIZATION_FACTORY",
            "ml.localization.service:create_localization_service",
        )
    )
    max_upload_bytes: int = field(default_factory=lambda: _env_int("MAX_UPLOAD_BYTES", 15 * 1024 * 1024))
    max_image_pixels: int = field(default_factory=lambda: _env_int("MAX_IMAGE_PIXELS", 40_000_000))
    max_image_dimension: int = field(default_factory=lambda: _env_int("MAX_IMAGE_DIMENSION", 12_000))
    multipart_overhead_bytes: int = field(default_factory=lambda: _env_int("MULTIPART_OVERHEAD_BYTES", 256 * 1024))
    localization_concurrency: int = field(default_factory=lambda: _env_int("LOCALIZATION_CONCURRENCY", 2))
    cors_origins: tuple[str, ...] = field(
        default_factory=lambda: _env_csv(
            "CORS_ORIGINS",
            (
                "http://localhost:3000",
                "http://127.0.0.1:3000",
                "http://localhost:5173",
                "http://127.0.0.1:5173",
            ),
        )
    )

    def __post_init__(self) -> None:
        positive_fields = (
            "max_upload_bytes",
            "max_image_pixels",
            "max_image_dimension",
            "multipart_overhead_bytes",
            "localization_concurrency",
        )
        for name in positive_fields:
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")
        for origin in self.cors_origins:
            if not origin.startswith(("http://", "https://")) or "*" in origin:
                raise ValueError("CORS_ORIGINS must contain explicit http(s) origins")


settings = Settings()
