from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    database_url: str = "sqlite:///./taskqueue.db"
    output_dir: Path = Path("reports/generated")
    webhook_secret: str = "local-development-secret"
    allow_loopback_webhooks: bool = True
    retry_base_seconds: float = 0.1
    retry_max_seconds: float = 5.0


settings = Settings()

