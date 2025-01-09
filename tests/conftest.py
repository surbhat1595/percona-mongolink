# pylint: disable=missing-docstring,redefined-outer-name
import os

import pytest
from pymongo import MongoClient


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


@pytest.fixture(scope="class")
def cls_source(request: pytest.FixtureRequest, source_conn: MongoClient):
    request.cls.source = source_conn


@pytest.fixture(scope="class")
def cls_target(request: pytest.FixtureRequest, target_conn: MongoClient):
    request.cls.target = target_conn
