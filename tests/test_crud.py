# pylint: disable=missing-docstring,redefined-outer-name
from _base import BaseTesting


class TestCRUDOperation(BaseTesting):
    def test_insert_one(self, change_stream, mlink, db, coll):
        with self.prepare() as it:
            it.ensure_empty_collection(db, coll)

        with mlink, change_stream:
            inserted_ids = []
            for i in range(5):
                res = self.source[db][coll].insert_one({"i": i})
                inserted_ids.append(res.inserted_id)

            for i in range(5):
                self.expect_insert_event(change_stream.next(), db, coll, inserted_ids[i])

        self.compare_coll_options(db, coll)
        self.compare_coll_indexes(db, coll)
        self.compare_coll_content(db, coll)

    def test_insert_many(self, change_stream, mlink, db, coll):
        with self.prepare() as it:
            it.ensure_empty_collection(db, coll)

        with mlink, change_stream:
            res = self.source[db][coll].insert_many([{"i": i} for i in range(5)])

            for i in range(5):
                self.expect_insert_event(change_stream.next(), db, coll, res.inserted_ids[i])

        self.compare_coll_options(db, coll)
        self.compare_coll_indexes(db, coll)
        self.compare_coll_content(db, coll)

    def test_update_one(self, change_stream, mlink, db, coll):
        with self.prepare() as it:
            it.ensure_empty_collection(db, coll)
            it.insert_documents(db, coll, [{"i": i} for i in range(5)])

        with mlink, change_stream:
            for i in range(5):
                self.source[db][coll].update_one(
                    {"i": i},
                    {"$inc": {"i": (i * 100) - i}, "$set": {f"field_{i}": f"value_{i}"}},
                )
            for i in range(5):
                self.expect_update_event(change_stream.next(), db, coll)

        self.compare_coll_options(db, coll)
        self.compare_coll_indexes(db, coll)
        self.compare_coll_content(db, coll)

    def test_update_many(self, change_stream, mlink, db, coll):
        with self.prepare() as it:
            it.ensure_empty_collection(db, coll)
            it.insert_documents(db, coll, [{"i": i} for i in range(5)])

        with mlink, change_stream:
            self.source[db][coll].update_many({}, {"$inc": {"i": 100}})
            for _ in range(5):
                self.expect_update_event(change_stream.next(), db, coll)

        self.compare_coll_options(db, coll)
        self.compare_coll_indexes(db, coll)
        self.compare_coll_content(db, coll)

    def test_replace_one(self, change_stream, mlink, db, coll):
        with self.prepare() as it:
            it.ensure_empty_collection(db, coll)
            it.insert_documents(db, coll, [{"i": i} for i in range(5)])

        with mlink, change_stream:
            for i in range(5):
                self.source[db][coll].replace_one(
                    {"i": i},
                    {"i": (i * 100) - i, f"field_{i}": f"value_{i}"},
                )
            for i in range(5):
                self.expect_replace_event(change_stream.next(), db, coll)

        self.compare_coll_options(db, coll)
        self.compare_coll_indexes(db, coll)
        self.compare_coll_content(db, coll)

    def test_delete_one(self, change_stream, mlink, db, coll):
        with self.prepare() as it:
            it.ensure_empty_collection(db, coll)
            it.insert_documents(db, coll, [{"i": i} for i in range(5)])

        with mlink, change_stream:
            self.source[db][coll].delete_one({"i": 4})
            self.expect_delete_event(change_stream.next(), db, coll)

        self.compare_coll_options(db, coll)
        self.compare_coll_indexes(db, coll)
        self.compare_coll_content(db, coll)

    def test_delete_many(self, change_stream, mlink, db, coll):
        with self.prepare() as it:
            it.ensure_empty_collection(db, coll)
            it.insert_documents(db, coll, [{"i": i} for i in range(5)])

        with mlink, change_stream:
            self.source[db][coll].delete_many({})
            for i in range(5):
                self.expect_delete_event(change_stream.next(), db, coll)

        self.compare_coll_options(db, coll)
        self.compare_coll_indexes(db, coll)
        self.compare_coll_content(db, coll)

    def test_find_one_and_update(self, change_stream, mlink, db, coll):
        with self.prepare() as it:
            it.ensure_empty_collection(db, coll)
            it.insert_documents(db, coll, [{"i": i} for i in range(5)])

        with mlink, change_stream:
            for i in range(5):
                self.source[db][coll].find_one_and_update(
                    {"i": i},
                    {
                        "$inc": {"i": (i * 100) - i},
                        "$set": {f"field_{i}": f"value_{i}"},
                    },
                )
            for i in range(5):
                self.expect_update_event(change_stream.next(), db, coll)

        self.compare_coll_options(db, coll)
        self.compare_coll_indexes(db, coll)
        self.compare_coll_content(db, coll)

    def test_find_one_and_replace(self, change_stream, mlink, db, coll):
        with self.prepare() as it:
            it.ensure_empty_collection(db, coll)
            it.insert_documents(db, coll, [{"i": i} for i in range(5)])

        with mlink, change_stream:
            for i in range(5):
                self.source[db][coll].find_one_and_replace(
                    {"i": i},
                    {
                        "i": (i * 100) - i,
                        f"field_{i}": f"value_{i}",
                    },
                )
            for i in range(5):
                self.expect_replace_event(change_stream.next(), db, coll)

        self.compare_coll_options(db, coll)
        self.compare_coll_indexes(db, coll)
        self.compare_coll_content(db, coll)

    def test_find_one_and_delete(self, change_stream, mlink, db, coll):
        with self.prepare() as it:
            it.ensure_empty_collection(db, coll)
            it.insert_documents(db, coll, [{"i": i} for i in range(5)])

        with mlink, change_stream:
            self.source[db][coll].find_one_and_delete({"i": 4})
            self.expect_delete_event(change_stream.next(), db, coll)

        self.compare_coll_options(db, coll)
        self.compare_coll_indexes(db, coll)
        self.compare_coll_content(db, coll)
