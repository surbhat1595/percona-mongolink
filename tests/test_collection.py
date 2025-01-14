# pylint: disable=missing-docstring,redefined-outer-name
import pytest
from _base import BaseTesting


class TestCollection(BaseTesting):
    def test_create_implicitly(self):
        self.ensure_no_collection("coll_name")

        with self.perform():
            self.source.test.coll_name.insert_one({})

        self.compare_coll_options("coll_name")
        self.compare_coll_indexes("coll_name")

    def test_create(self):
        self.ensure_no_collection("coll_name")

        with self.perform():
            self.source.test.create_collection("coll_name")

        self.compare_coll_options("coll_name")
        self.compare_coll_indexes("coll_name")

    def test_create_equal_uuid(self):
        self.ensure_no_collection("coll_name")

        with self.perform():
            self.source.test.create_collection("coll_name")

        self.compare_coll_options("coll_name")
        self.compare_coll_indexes("coll_name")

        source_info = next(self.source.test.list_collections(filter={"name": "coll_name"}))
        target_info = next(self.target.test.list_collections(filter={"name": "coll_name"}))
        if source_info["info"]["uuid"] != target_info["info"]["uuid"]:
            pytest.xfail("collection UUID may not be equal")

    def test_create_with_clustered_index(self):
        self.ensure_no_collection("coll_name")

        with self.perform():
            self.source.test.create_collection(
                "coll_name",
                clusteredIndex={
                    "key": {"_id": 1},
                    "unique": True,
                },
            )

        self.compare_coll_options("coll_name")
        self.compare_coll_indexes("coll_name")

    def test_create_capped(self):
        self.ensure_no_collection("coll_name")

        with self.perform():
            self.source.test.create_collection("coll_name", capped=True, size=54321, max=12345)
            self.source.test.coll_name.insert_many({"i": i} for i in range(10))

        self.compare_coll_options("coll_name")
        self.compare_coll_indexes("coll_name")

    def test_create_view(self):
        self.ensure_empty_collection("coll_name")
        self.insert_documents("coll_name", [{"i": i} for i in range(10)])
        self.ensure_no_collection("view_name")

        with self.perform():
            self.source.test.create_collection(
                "view_name",
                viewOn="coll_name",
                pipeline=[{"$match": {"i": {"$gt": 3}}}],
            )

        assert "view_name" in self.target.test.list_collection_names()

        self.compare_coll_options("view_name")
        self.compare_coll_content("view_name")

    def test_drop_collection(self):
        self.drop_database()
        self.ensure_empty_collection("coll_name")

        with self.perform():
            self.source.test.drop_collection("coll_name")

        assert "coll_name" not in self.target.test.list_collection_names()
        assert "test" not in self.target.list_database_names()

    def test_drop_capped(self):
        self.drop_database()
        self.ensure_no_collection("coll_name")
        self.source.test.create_collection("coll_name", capped=True, size=54321, max=12345)
        self.source.test.coll_name.insert_many({"i": i} for i in range(10))

        with self.perform():
            self.source.test.drop_collection("coll_name")

        assert "coll_name" not in self.target.test.list_collection_names()
        assert "test" not in self.target.list_database_names()

    def test_drop_view(self):
        self.ensure_empty_collection("coll_name")
        self.ensure_view("view_name", "coll_name", [{"$match": {"i": {"$gt": 3}}}])

        with self.perform():
            self.source.test.drop_collection("view_name")

        assert "view_name" not in self.target.test.list_collection_names()
        assert "coll_name" in self.target.test.list_collection_names()

    def test_drop_database(self):
        self.drop_database()
        self.ensure_empty_collection("coll_name")

        with self.perform():
            self.source.drop_database("test")

        assert "test" not in self.target.list_database_names()
