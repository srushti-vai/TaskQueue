from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session, sessionmaker

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


def get_session():
    with SessionLocal() as session:
        yield session

