from sqlalchemy import create_engine, event, inspect, text
from sqlalchemy.orm import sessionmaker

from .config import settings
from .models import Base


def make_engine(url: str = settings.database_url):
    engine = create_engine(url, connect_args={"check_same_thread": False} if url.startswith("sqlite") else {})
    if url.startswith("sqlite"):
        @event.listens_for(engine, "connect")
        def sqlite_pragmas(connection, _):
            cursor = connection.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA busy_timeout=5000")
            cursor.close()
    return engine


engine = make_engine()
SessionLocal = sessionmaker(engine, expire_on_commit=False)


def init_db(target=engine) -> None:
    Base.metadata.create_all(target)
    # Keep databases created by earlier TaskQueue versions usable without
    # introducing a migration framework into this educational MVP.
    if target.dialect.name == "sqlite":
        columns = {item["name"] for item in inspect(target).get_columns("jobs")}
        additions = {
            "retry_count": "INTEGER NOT NULL DEFAULT 0",
            "expired_recovery_count": "INTEGER NOT NULL DEFAULT 0",
        }
        with target.begin() as connection:
            for name, declaration in additions.items():
                if name not in columns:
                    connection.execute(text(f"ALTER TABLE jobs ADD COLUMN {name} {declaration}"))


def get_session():
    with SessionLocal() as session:
        yield session
