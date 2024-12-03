# pylint: disable=missing-docstring,redefined-outer-name
import os

import pytest
from _base import ChangeStream, MongoLink
from pymongo import MongoClient


@pytest.fixture
def db():
    return "test"


@pytest.fixture
def coll():
    return "coll_name"


@pytest.fixture(scope="session")
def source_conn():
    conn = MongoClient(os.environ["TEST_SOURCE_URI"])
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def target_conn():
    conn = MongoClient(os.environ["TEST_TARGET_URI"])
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def mlink():
    return MongoLink(os.environ["TEST_MLINK_URI"])


@pytest.fixture
def change_stream(target_conn):
    return ChangeStream(target_conn)


@pytest.fixture(scope="class")
def cls_source(request: pytest.FixtureRequest, source_conn: MongoClient):
    request.cls.source = source_conn


@pytest.fixture(scope="class")
def cls_target(request: pytest.FixtureRequest, target_conn: MongoClient):
    request.cls.target = target_conn
