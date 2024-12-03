# pylint: disable=missing-docstring,redefined-outer-name
import pytest
from _base import BaseTesting


class TestCollection(BaseTesting):
    def test_create_implicitly(self, change_stream, mlink, db, coll):
        with self.prepare() as it:
            it.ensure_no_collection(db, coll)

        with mlink, change_stream:
            self.source[db][coll].insert_one({})
            self.expect_create_event(change_stream.next(), db, coll)

        self.compare_coll_options(db, coll)
        self.compare_coll_indexes(db, coll)

    def test_create(self, change_stream, mlink, db, coll):
        with self.prepare() as it:
            it.ensure_no_collection(db, coll)

        with mlink, change_stream:
            self.source[db].create_collection(coll)
            self.expect_create_event(change_stream.next(), db, coll)

        self.compare_coll_options(db, coll)
        self.compare_coll_indexes(db, coll)

    def test_create_equal_uuid(self, change_stream, mlink, db, coll):
        with self.prepare() as it:
            it.ensure_no_collection(db, coll)

        with mlink, change_stream:
            self.source[db].create_collection(coll)
            self.expect_create_event(change_stream.next(), db, coll)

        self.compare_coll_options(db, coll)
        self.compare_coll_indexes(db, coll)

        source_info = next(self.source[db].list_collections(filter={"name": coll}))
        target_info = next(self.target[db].list_collections(filter={"name": coll}))
        if source_info["info"]["uuid"] != target_info["info"]["uuid"]:
            pytest.xfail("collection UUID may not be equal")

    def test_create_with_clustered_index(self, change_stream, mlink, db, coll):
        with self.prepare() as it:
            it.ensure_no_collection(db, coll)

        with mlink, change_stream:
            self.source[db].create_collection(
                coll,
                clusteredIndex={
                    "key": {"_id": 1},
                    "unique": True,
                },
            )
            self.expect_create_event(change_stream.next(), db, coll)

        self.compare_coll_options(db, coll)
        self.compare_coll_indexes(db, coll)

    @pytest.mark.skip("capped collection is not unsupported yet")
    def test_create_capped(self, change_stream, mlink, db, coll):
        with self.prepare() as it:
            it.ensure_no_collection(db, coll)

        with mlink, change_stream:
            self.source[db].create_collection(coll, capped=True, size=54321, max=12345)
            self.expect_create_event(change_stream.next(), db, coll)

        self.compare_coll_options(db, coll)
        self.compare_coll_indexes(db, coll)

    def test_drop_collection(self, change_stream, mlink, db, coll):
        with self.prepare() as it:
            it.drop_all_collections(db)
            it.ensure_empty_collection(db, coll)

        with mlink, change_stream:
            self.source[db].drop_collection(coll)
            self.expect_drop_event(change_stream.next(), db, coll)

        if coll in self.target[db].list_collection_names():
            pytest.fail(f"'{db}.{coll}' must be dropped")
        if db in self.target.list_database_names():
            pytest.fail(f"'{db}' database must be dropped")

    def test_drop_database(self, change_stream, mlink, db, coll):
        with self.prepare() as it:
            it.drop_all_collections(db)
            it.ensure_empty_collection(db, coll)

        with mlink, change_stream:
            self.source.drop_database(db)
            self.expect_drop_event(change_stream.next(), db, coll)
            self.expect_drop_database_event(change_stream.next(), db)

        if coll in self.target[db].list_collection_names():
            pytest.fail(f"'{db}.{coll}' must be dropped")
        if db in self.target.list_database_names():
            pytest.fail(f"'{db}' database must be dropped")
