import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import sessionmaker

from taskqueue.api import app
from taskqueue.database import get_session, init_db, make_engine


@pytest.fixture()
def session(tmp_path):
    engine = make_engine(f"sqlite:///{(tmp_path / 'test.db').as_posix()}")
    init_db(engine)
    factory = sessionmaker(engine, expire_on_commit=False)
    with factory() as value: yield value


@pytest.fixture()
def client(session):
    app.dependency_overrides[get_session] = lambda: session
    with TestClient(app) as value: yield value
    app.dependency_overrides.clear()
