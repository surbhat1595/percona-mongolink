# pylint: disable=missing-docstring,redefined-outer-name
import hashlib
from typing import List

import bson
import pytest
from pymongo import MongoClient
from pymongo.collection import Collection

from mlink import MLink, Runner


@pytest.mark.usefixtures("cls_source", "cls_target", "cls__mlink")
class BaseTesting:
    source: MongoClient
    target: MongoClient
    _mlink: MLink

    def perform(self, phase: Runner.Phase):
        return Runner(self.source, self._mlink, phase)

    @classmethod
    def compare_all(cls):
        src_dbs = cls.list_databases(cls.source)
        dst_dbs = cls.list_databases(cls.target)
        assert src_dbs == dst_dbs

        for db in src_dbs:
            src_colls = cls.list_namespaces(cls.source, db)
            dst_colls = cls.list_namespaces(cls.target, db)
            assert src_colls == dst_colls

            for coll in src_colls:
                cls.compare_namespace(db, coll)

    @staticmethod
    def list_databases(client: MongoClient):
        rv = set()
        for name in client.list_database_names():
            if name not in ("local", "admin", "config"):
                rv.add(name)
        return rv

    @staticmethod
    def list_namespaces(client: MongoClient, db: str):
        rv = set()
        for name in client[db].list_collection_names():
            if not name.startswith("system."):
                rv.add(name)
        return rv

    @classmethod
    def compare_namespace(cls, db: str, coll: str):
        src_options = cls.source[db][coll].options()
        dst_options = cls.target[db][coll].options()
        assert src_options == dst_options, f"{src_options=} != {dst_options=}"

        if "viewOn" not in src_options:
            src_indexes = cls.source[db][coll].index_information()
            dst_indexes = cls.target[db][coll].index_information()
            assert src_indexes == dst_indexes, f"{src_indexes=} != {dst_indexes=}"

        src_docs, src_hash = cls._coll_content(cls.source[db][coll])
        dst_docs, dst_hash = cls._coll_content(cls.target[db][coll])
        assert len(src_docs) == len(dst_docs), f"{src_docs=} != {dst_docs=}"
        assert src_docs == dst_docs, f"{src_docs=} != {dst_docs=}"
        assert src_hash == dst_hash, f"{src_hash=} != {dst_hash=}"

    @staticmethod
    def _coll_content(coll: Collection):
        docs, md5 = [], hashlib.md5()
        for data in coll.find_raw_batches():
            md5.update(data)
            docs.extend(bson.decode_all(data))
        return docs, md5.hexdigest()

    def drop_database(self, db: str):
        self.source.drop_database(db)
        self.target.drop_database(db)

    def create_collection(self, db: str, coll: str, **kwargs):
        self.source[db].create_collection(coll, **kwargs)
        self.target[db].create_collection(coll, **kwargs)

    def create_index(self, db: str, coll: str, keys, **kwargs):
        self.source[db][coll].create_index(keys, **kwargs)
        self.source[db][coll].create_index(keys, **kwargs)

    def create_view(self, db: str, view_name: str, source: str, pipeline: List[dict]):
        self.source[db].create_collection(view_name, viewOn=source, pipeline=pipeline)
        self.target[db].create_collection(view_name, viewOn=source, pipeline=pipeline)

    def insert_documents(self, db: str, coll: str, documents: List[dict]):
        self.source[db][coll].insert_many(documents)
        self.target[db][coll].insert_many(documents)
