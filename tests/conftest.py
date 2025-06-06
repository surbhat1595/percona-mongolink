# pylint: disable=missing-docstring,redefined-outer-name
import os
import subprocess
import time

import pytest
import testing
from mlink import MongoLink
from pymongo import MongoClient


def pytest_addoption(parser):
    """Add custom command-line options to pytest."""
    parser.addoption("--source-uri", help="MongoDB URI for source")
    parser.addoption("--target-uri", help="MongoDB URI for target")
    parser.addoption("--mongolink-url", help="MongoLink url")
    parser.addoption("--mongolink-bin", help="Path to the MongoLink binary")
    parser.addoption("--runslow", action="store_true", default=False, help="run slow tests")


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: mark test as slow")


def pytest_collection_modifyitems(config, items):
    """This allows users to control whether slow tests are included in the test run.

    If the `--runslow` option is not provided, tests marked with the "slow" keyword
    will be skipped with a message indicating the need for the `--runslow` option.
    """
    if not config.getoption("--runslow"):
        skip_slow = pytest.mark.skip(reason="need --runslow option to run")
        for item in items:
            if "slow" in item.keywords:
                item.add_marker(skip_slow)


@pytest.fixture(scope="session")
def source_conn(request: pytest.FixtureRequest):
    """Provide a MongoClient connection to the source MongoDB."""
    uri = request.config.getoption("--source-uri") or os.environ["TEST_SOURCE_URI"]
    with MongoClient(uri) as conn:
        yield conn


@pytest.fixture(scope="session")
def target_conn(request: pytest.FixtureRequest):
    """Provide a MongoClient connection to the target MongoDB."""
    uri = request.config.getoption("--target-uri") or os.environ["TEST_TARGET_URI"]
    with MongoClient(uri) as conn:
        yield conn


@pytest.fixture(scope="session")
def mlink(request: pytest.FixtureRequest):
    """Provide a mongolink instance."""
    url = request.config.getoption("--mongolink-url") or os.environ["TEST_MONGOLINK_URL"]
    return MongoLink(url)


@pytest.fixture(scope="session")
def mlink_bin(request: pytest.FixtureRequest):
    """Provide the path to the MongoLink binary."""
    return request.config.getoption("--mongolink-bin") or os.getenv("TEST_MONGOLINK_BIN")


@pytest.fixture(scope="session")
def t(source_conn: MongoClient, target_conn: MongoClient, mlink: MongoLink):
    return testing.Testing(source_conn, target_conn, mlink)


@pytest.fixture(autouse=True)
def drop_all_database(source_conn: MongoClient, target_conn: MongoClient):
    """Drop all databases in the source and target MongoDB before each test."""
    testing.drop_all_database(source_conn)
    testing.drop_all_database(target_conn)


MLINK_PROC: subprocess.Popen = None


def start_mongolink(mlink_bin: str):
    rv = subprocess.Popen([mlink_bin, "--reset-state", "--log-level=trace"])
    time.sleep(1)
    return rv


def stop_mongolink(proc: subprocess.Popen):
    proc.terminate()
    return proc.wait()


@pytest.fixture(scope="session", autouse=True)
def manage_mongolink_process(request: pytest.FixtureRequest, mlink_bin: str):
    """Start mongolink before tests and terminate it after all tests."""
    if not mlink_bin:
        yield
        return

    global MLINK_PROC  # pylint: disable=W0603
    MLINK_PROC = start_mongolink(mlink_bin)

    def teardown():
        if MLINK_PROC and MLINK_PROC.poll() is None:
            stop_mongolink(MLINK_PROC)

    request.addfinalizer(teardown)
    yield MLINK_PROC


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):  # pylint: disable=W0613
    """Attach test results to each test item for later inspection."""
    outcome = yield
    rep = outcome.get_result()
    setattr(item, "rep_" + rep.when, rep)


@pytest.fixture(autouse=True)
def restart_mongolink_on_failure(request: pytest.FixtureRequest, mlink_bin: str):
    yield

    if hasattr(request.node, "rep_call") and request.node.rep_call.failed:
        # the test failed. restart mongolink process with a new state
        global MLINK_PROC  # pylint: disable=W0603
        if MLINK_PROC and mlink_bin:
            stop_mongolink(MLINK_PROC)
            MLINK_PROC = start_mongolink(mlink_bin)
