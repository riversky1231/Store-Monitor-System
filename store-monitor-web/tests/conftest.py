import sys
from pathlib import Path

# Allow test modules to import from the store-monitor-web package root.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from fastapi import FastAPI
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from database import Base, get_db


@pytest.fixture
def auth_disabled(monkeypatch):
    monkeypatch.setenv("MONITOR_WEB_DISABLE_AUTH", "1")


@pytest.fixture
def session_factory():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)
    try:
        yield TestingSessionLocal
    finally:
        engine.dispose()


@pytest.fixture
def app_factory(auth_disabled, session_factory):
    def _create(*routers):
        app = FastAPI()
        app.state.setup_complete_cache = None

        def _override_get_db():
            db = session_factory()
            try:
                yield db
            finally:
                db.close()

        app.dependency_overrides[get_db] = _override_get_db
        for router in routers:
            app.include_router(router)
        return app

    return _create
