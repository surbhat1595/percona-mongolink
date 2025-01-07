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

    @pytest.mark.xfail
    def test_create_with_unique_index(self, change_stream, mlink, db, coll):
        with self.prepare() as it:
            it.ensure_no_collection(db, coll)

        with mlink, change_stream:
            self.source[db].create_collection(coll)
            self.source[db][coll].create_index({"i": 1, "unique": 1})
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

    def test_create_view(self, change_stream, mlink, db, coll):
        view_name = f"{coll}_view"
        with self.prepare() as it:
            it.ensure_empty_collection(db, coll)
            it.insert_documents(db, coll, [{"i": i for i in range(10)}])
            it.ensure_no_collection(db, view_name)

        with mlink, change_stream:
            self.source[db].create_collection(
                view_name,
                viewOn=coll,
                pipeline=[{"$match": {"i": {"$gt": 3}}}],
            )
            self.expect_create_event(change_stream.next(), db, view_name)

        if view_name not in self.target[db].list_collection_names():
            pytest.fail(f"'{db}.{coll}' must be present")

        self.compare_coll_options(db, view_name)
        # self.compare_coll_indexes(db, view_name)
        self.compare_coll_content(db, view_name)

    def test_drop_collection(self, change_stream, mlink, db, coll):
        with self.prepare() as it:
            it.drop_database(db)
            it.ensure_empty_collection(db, coll)

        with mlink, change_stream:
            self.source[db].drop_collection(coll)
            self.expect_drop_event(change_stream.next(), db, coll)

        if coll in self.target[db].list_collection_names():
            pytest.fail(f"'{db}.{coll}' must be dropped")
        if db in self.target.list_database_names():
            pytest.fail(f"'{db}' database must be dropped")

    def test_drop_view(self, change_stream, mlink, db, coll):
        view_name = f"{coll}_view"
        with self.prepare() as it:
            it.ensure_empty_collection(db, coll)
            it.ensure_view(db, view_name, coll, [{"$match": {"i": {"$gt": 3}}}])

        with mlink, change_stream:
            self.source[db].drop_collection(view_name)
            self.expect_drop_event(change_stream.next(), db, view_name)

        if view_name in self.target[db].list_collection_names():
            pytest.fail(f"'{db}.{coll}' must be dropped")
        if coll not in self.target[db].list_collection_names():
            pytest.fail(f"'{db}.{coll}' must not be dropped")

    def test_drop_database(self, change_stream, mlink, db, coll):
        with self.prepare() as it:
            it.drop_database(db)
            it.ensure_empty_collection(db, coll)

        with mlink, change_stream:
            self.source.drop_database(db)
            self.expect_drop_event(change_stream.next(), db, coll)
            self.expect_drop_database_event(change_stream.next(), db)

        if db in self.target.list_database_names():
            pytest.fail(f"'{db}' database must be dropped")
