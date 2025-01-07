# pylint: disable=missing-docstring,redefined-outer-name
import hashlib
import time

import bson
import pytest
import requests
from pymongo import MongoClient
from pymongo.collection import Collection


@pytest.mark.usefixtures("cls_source", "cls_target")
class BaseTesting:
    source: MongoClient
    target: MongoClient

    def prepare(self):
        return Prepare(self)

    @classmethod
    def expect_create_event(cls, event: dict, db_name, coll_name):
        if event["operationType"] != "create":
            pytest.fail(f"operationType={event["operationType"]}, expected 'create'")
        if event["ns"]["db"] != db_name or event["ns"]["coll"] != coll_name:
            pytest.fail(
                f"ns='{event['ns']['db']}.{event['ns']['coll']}', "
                + f"expected '{db_name}.{coll_name}'"
            )

    @classmethod
    def expect_insert_event(cls, event: dict, db_name, coll_name, oid=None):
        if event["operationType"] != "insert":
            pytest.fail(f"operationType={event["operationType"]}, expected 'create'")
        if event["ns"]["db"] != db_name or event["ns"]["coll"] != coll_name:
            pytest.fail(
                f"ns='{event['ns']['db']}.{event['ns']['coll']}', "
                + f"expected '{db_name}.{coll_name}'"
            )
        if oid and event["documentKey"]["_id"] != oid:
            pytest.fail(f"{event['documentKey']}, expected {oid}")

    @classmethod
    def expect_update_event(cls, event: dict, db_name, coll_name, oid=None):
        if event["operationType"] != "update":
            pytest.fail(f"operationType={event["operationType"]}, expected 'create'")
        if event["ns"]["db"] != db_name or event["ns"]["coll"] != coll_name:
            pytest.fail(
                f"ns='{event['ns']['db']}.{event['ns']['coll']}', "
                + f"expected '{db_name}.{coll_name}'"
            )
        if oid and event["documentKey"]["_id"] != oid:
            pytest.fail(f"{event['documentKey']}, expected {oid}")

    @classmethod
    def expect_replace_event(cls, event: dict, db_name, coll_name, oid=None):
        if event["operationType"] != "replace":
            pytest.fail(f"operationType={event["operationType"]}, expected 'create'")
        if event["ns"]["db"] != db_name or event["ns"]["coll"] != coll_name:
            pytest.fail(
                f"ns='{event['ns']['db']}.{event['ns']['coll']}', "
                + f"expected '{db_name}.{coll_name}'"
            )
        if oid and event["documentKey"]["_id"] != oid:
            pytest.fail(f"{event['documentKey']}, expected {oid}")

    @classmethod
    def expect_delete_event(cls, event: dict, db_name, coll_name, oid=None):
        if event["operationType"] != "delete":
            pytest.fail(f"operationType={event["operationType"]}, expected 'create'")
        if event["ns"]["db"] != db_name or event["ns"]["coll"] != coll_name:
            pytest.fail(
                f"ns='{event['ns']['db']}.{event['ns']['coll']}', "
                + f"expected '{db_name}.{coll_name}'"
            )
        if oid and event["documentKey"]["_id"] != oid:
            pytest.fail(f"{event['documentKey']}, expected {oid}")

    @staticmethod
    def expect_drop_event(event: dict, db_name, coll_name):
        if event["operationType"] != "drop":
            pytest.fail(f"operationType={event["operationType"]}, expected 'drop'")
        if event["ns"]["db"] != db_name or event["ns"]["coll"] != coll_name:
            pytest.fail(
                f"ns='{event['ns']['db']}.{event['ns']['coll']}', "
                + f"expected '{db_name}.{coll_name}'"
            )

    @staticmethod
    def expect_drop_database_event(event: dict, db_name):
        if event["operationType"] != "dropDatabase":
            pytest.fail(f"operationType={event["operationType"]}, expected 'dropDatabase'")
        if event["ns"]["db"] != db_name:
            pytest.fail(f"ns='{event['ns']['db']}', expected '{db_name}'")

    @classmethod
    def compare_coll_options(cls, db_name, coll_name):
        src_coll_options = cls.source[db_name][coll_name].options()
        dst_coll_options = cls.target[db_name][coll_name].options()
        if src_coll_options != dst_coll_options:
            pytest.fail(f"{dst_coll_options=}, expected {src_coll_options}")

    @classmethod
    def compare_coll_indexes(cls, db_name, coll_name):
        src_coll_indexes = cls.source[db_name][coll_name].index_information()
        dst_coll_indexes = cls.target[db_name][coll_name].index_information()
        if src_coll_indexes != dst_coll_indexes:
            pytest.fail(f"{dst_coll_indexes=}, expected {src_coll_indexes}")

    @classmethod
    def compare_coll_content(cls, db_name, coll_name, **kwargs):
        src_docs, src_hash = cls.coll_content(cls.source[db_name][coll_name])
        dst_docs, dst_hash = cls.coll_content(cls.target[db_name][coll_name])
        if "count" in kwargs and len(src_docs) != kwargs["count"]:
            pytest.fail(f"{len(src_docs)=}, expected {kwargs["count"]}")
        if len(src_docs) != len(dst_docs):
            pytest.fail(f"{len(dst_docs)=}, expected {len(src_docs)}")
        for a, b in zip(src_docs, dst_docs):
            if a != b:
                print(f"{a=} {b=}")
        if src_hash != dst_hash:
            pytest.fail(f"{dst_hash=}, expected {src_hash}")

    @staticmethod
    def coll_content(coll: Collection):
        docs, md5 = [], hashlib.md5()
        for data in coll.find_raw_batches():
            md5.update(data)
            docs.extend(bson.decode_all(data))
        return docs, md5.hexdigest()


class MongoLink:
    def __init__(self, uri):
        self.uri = uri
        self.status()

    def __enter__(self):
        status = self.status()
        if status["state"] == "running":
            self.finalize()
            for _ in range(5):
                time.sleep(0.5)
                status = self.status()
                if status["state"] == "completed":
                    break
        self.start()

        time.sleep(1)
        return self

    def __exit__(self, _t, _v, _tb):
        status = self.status()
        if status["state"] == "running":
            self.finalize()
            for _ in range(5):
                time.sleep(0.5)
                status = self.status()
                if status["state"] == "completed":
                    break

    def status(self, **kwargs):
        res = requests.get(f"{self.uri}/status", timeout=kwargs.get("timeout", 5))
        res.raise_for_status()
        return res.json()

    def start(self, **kwargs):
        res = requests.post(f"{self.uri}/start", timeout=kwargs.get("timeout", 5))
        res.raise_for_status()
        return res.json()

    def finalize(self, **kwargs):
        res = requests.post(f"{self.uri}/finalize", timeout=kwargs.get("timeout", 5))
        res.raise_for_status()
        return res.json()


class Prepare:
    def __init__(self, t: BaseTesting):
        self.source: MongoClient = t.source
        self.target: MongoClient = t.target

    def __enter__(self):
        return self

    def __exit__(self, _t, _v, _tb):
        pass

    def ensure_no_collection(self, db_name, coll_name):
        self.source[db_name].drop_collection(coll_name)
        self.target[db_name].drop_collection(coll_name)

    def ensure_empty_collection(self, db_name, coll_name, **kwargs):
        # todo: ensure the same collection options
        self.ensure_no_collection(db_name, coll_name)
        self.source[db_name].create_collection(coll_name, **kwargs)
        self.target[db_name].create_collection(coll_name, **kwargs)

    def ensure_view(self, db_name, view_name, source, pipeline):
        # todo: ensure the same view options
        self.ensure_empty_collection(db_name, view_name, viewOn=source, pipeline=pipeline)

    def insert_documents(self, db_name, coll_name, documents):
        self.source[db_name][coll_name].insert_many(documents)
        self.target[db_name][coll_name].insert_many(documents)

    def drop_all_collections(self, db_name):
        for coll_name in self.source[db_name].list_collection_names():
            if not coll_name.startswith("system."):
                self.ensure_no_collection(db_name, coll_name)
        for coll_name in self.target[db_name].list_collection_names():
            if not coll_name.startswith("system."):
                self.ensure_no_collection(db_name, coll_name)

    def drop_database(self, db_name):
        self.source.drop_database(db_name)
        self.target.drop_database(db_name)


class ChangeStream:
    def __init__(self, target: MongoClient):
        self.target: MongoClient = target
        self.stream = None

    def __enter__(self):
        self.stream = self.target.watch(show_expanded_events=True)
        return self

    def __exit__(self, _t, _v, _tb):
        self.stream.close()

    def next(self):
        return self.stream.next()
