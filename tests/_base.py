# pylint: disable=missing-docstring,redefined-outer-name
import hashlib
import os
import time
from enum import StrEnum

import bson
import pytest
import requests
from pymongo import MongoClient
from pymongo.collection import Collection


@pytest.mark.usefixtures("cls_source", "cls_target")
class BaseTesting:
    source: MongoClient
    target: MongoClient

    def perform(self):
        return MongoLinkRunner(self)

    @classmethod
    def compare_coll_options(cls, coll_name):
        src_coll_options = cls.source.test[coll_name].options()
        dst_coll_options = cls.target.test[coll_name].options()
        assert src_coll_options == dst_coll_options

    @classmethod
    def compare_coll_indexes(cls, coll_name):
        src_coll_indexes = cls.source.test[coll_name].index_information()
        dst_coll_indexes = cls.target.test[coll_name].index_information()
        assert src_coll_indexes == dst_coll_indexes

    @classmethod
    def compare_coll_content(cls, coll_name):
        src_docs, src_hash = cls._coll_content(cls.source.test[coll_name])
        dst_docs, dst_hash = cls._coll_content(cls.target.test[coll_name])
        assert len(src_docs) == len(dst_docs)
        assert src_docs == dst_docs
        assert src_hash == dst_hash

    @staticmethod
    def _coll_content(coll: Collection):
        docs, md5 = [], hashlib.md5()
        for data in coll.find_raw_batches():
            md5.update(data)
            docs.extend(bson.decode_all(data))
        return docs, md5.hexdigest()

    def ensure_no_collection(self, coll_name):
        self.source.test.drop_collection(coll_name)
        self.target.test.drop_collection(coll_name)

    def ensure_empty_collection(self, coll_name):
        # todo: ensure the same collection options
        self.ensure_no_collection(coll_name)
        self.source.test.create_collection(coll_name)
        self.target.test.create_collection(coll_name)

    def ensure_no_indexes(self, coll_name):
        self.source.test[coll_name].drop_indexes()
        self.target.test[coll_name].drop_indexes()

    def create_index(self, coll_name, keys, **kwargs):
        self.source.test[coll_name].create_index(keys, **kwargs)
        self.source.test[coll_name].create_index(keys, **kwargs)

    def ensure_view(self, view_name, source, pipeline):
        # todo: ensure the same view options
        self.ensure_no_collection(view_name)
        self.source.test.create_collection(view_name, viewOn=source, pipeline=pipeline)
        self.target.test.create_collection(view_name, viewOn=source, pipeline=pipeline)

    def insert_documents(self, coll_name, documents):
        self.source.test[coll_name].insert_many(documents)
        self.target.test[coll_name].insert_many(documents)

    def drop_all_collections(self):
        for coll_name in self.source.test.list_collection_names():
            if not coll_name.startswith("system."):
                self.ensure_no_collection(coll_name)
        for coll_name in self.target.test.list_collection_names():
            if not coll_name.startswith("system."):
                self.ensure_no_collection(coll_name)

    def drop_database(self):
        self.source.drop_database("test")
        self.target.drop_database("test")


class MongoLinkRunner:
    def __init__(self, t: BaseTesting):
        self.source: MongoClient = t.source
        self.mlink = MongoLink(os.environ["TEST_MLINK_URI"])

    def __enter__(self):
        status = self.mlink.status()
        if status["state"] == MongoLink.State.FINALIZING:
            self.mlink.wait_for_state(MongoLink.State.FINALIZED)
        elif status["state"] == MongoLink.State.RUNNING:
            self.mlink.finalize()
            self.mlink.wait_for_state(MongoLink.State.FINALIZED)

        self.mlink.start()
        self.mlink.wait_for_state(MongoLink.State.RUNNING)
        return self

    def __exit__(self, _t, exc, _tb):
        if exc:
            return

        status = self.mlink.status()
        if status["state"] == MongoLink.State.FINALIZING:
            self.mlink.wait_for_state(MongoLink.State.FINALIZED)
        elif status["state"] == MongoLink.State.RUNNING:
            optime = self.source.server_info()["operationTime"]
            self.mlink.wait_for_optime(optime)
            self.mlink.finalize()
            self.mlink.wait_for_state(MongoLink.State.FINALIZED)


class MongoLink:
    class State(StrEnum):
        IDLE = "idle"
        RUNNING = "running"
        FINALIZING = "finalizing"
        FINALIZED = "finalized"

    def __init__(self, uri):
        self.uri = uri

    def status(self, **kwargs):
        res = requests.get(f"{self.uri}/status", timeout=kwargs.get("timeout", 5))
        res.raise_for_status()
        return res.json()

    def start(self, **kwargs):
        res = requests.post(f"{self.uri}/start", json={}, timeout=kwargs.get("timeout", 5))
        res.raise_for_status()
        return res.json()

    def finalize(self, **kwargs):
        res = requests.post(f"{self.uri}/finalize", timeout=kwargs.get("timeout", 5))
        res.raise_for_status()
        return res.json()

    def wait_for_state(self, state):
        status = self.status()
        while status["state"] != state:
            time.sleep(0.2)
            status = self.status()

    def wait_for_optime(self, ts: bson.Timestamp):
        status = self.status()
        assert status["state"] == MongoLink.State.RUNNING

        for _ in range(10):  # ~2 secs timeout
            applied_optime: str = status.get("lastAppliedOpTime")
            if applied_optime:
                t_s, i_s = applied_optime.split(".")
                if ts <= bson.Timestamp(int(t_s), int(i_s)):
                    break

            time.sleep(0.2)
            status = self.status()
