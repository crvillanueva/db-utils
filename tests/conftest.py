import pytest
from sqlalchemy import create_engine, inspect

DB_TEST = "sqlite:///test_db.db"


@pytest.fixture(scope="session")
def test_engine():
    return create_engine(DB_TEST)


@pytest.fixture(scope="session")
def test_inspector(test_engine):
    return inspect(test_engine)
