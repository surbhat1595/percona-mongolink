# pylint: disable=missing-docstring,redefined-outer-name
import os

import pytest
from pymongo import MongoClient

from mlink import MLink


def pytest_addoption(parser):
    parser.addoption("--src-uri", help="MongoDB URI for source")
    parser.addoption("--dest-uri", help="MongoDB URI for destination")
    parser.addoption("--mlink-url", help="mongolink url")


@pytest.fixture(scope="session")
def source_conn(request: pytest.FixtureRequest):
    uri = request.config.getoption("--src-uri") or os.environ["TEST_SRC_URI"]
    with MongoClient(uri) as conn:
        yield conn


@pytest.fixture(scope="session")
def target_conn(request: pytest.FixtureRequest):
    uri = request.config.getoption("--dest-uri") or os.environ["TEST_DEST_URI"]
    with MongoClient(uri) as conn:
        yield conn


@pytest.fixture(scope="class")
def cls_source(request: pytest.FixtureRequest, source_conn: MongoClient):
    request.cls.source = source_conn


@pytest.fixture(scope="class")
def cls_target(request: pytest.FixtureRequest, target_conn: MongoClient):
    request.cls.target = target_conn


@pytest.fixture(scope="class")
def cls__mlink(request: pytest.FixtureRequest):
    url = request.config.getoption("--mlink-url") or os.environ["TEST_MLINK_URL"]
    request.cls._mlink = MLink(url)  # pylint: disable=protected-access
