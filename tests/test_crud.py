# pylint: disable=missing-docstring,redefined-outer-name
from _base import BaseTesting


class TestCRUDOperation(BaseTesting):
    def test_insert_one(self):
        with self.prepare() as it:
            it.ensure_empty_collection("coll_name")

        with self.perform():
            for i in range(5):
                self.source.test["coll_name"].insert_one({"i": i})

        self.compare_coll_options("coll_name")
        self.compare_coll_indexes("coll_name")
        self.compare_coll_content("coll_name")

    def test_insert_many(self):
        with self.prepare() as it:
            it.ensure_empty_collection("coll_name")

        with self.perform():
            self.source.test["coll_name"].insert_many([{"i": i} for i in range(5)])

        self.compare_coll_options("coll_name")
        self.compare_coll_indexes("coll_name")
        self.compare_coll_content("coll_name")

    def test_update_one(self):
        with self.prepare() as it:
            it.ensure_empty_collection("coll_name")
            it.insert_documents("coll_name", [{"i": i} for i in range(5)])

        with self.perform():
            for i in range(5):
                self.source.test["coll_name"].update_one(
                    {"i": i},
                    {
                        "$inc": {"i": (i * 100) - i},
                        "$set": {f"field_{i}": f"value_{i}"},
                    },
                )

        self.compare_coll_options("coll_name")
        self.compare_coll_indexes("coll_name")
        self.compare_coll_content("coll_name")

    def test_update_one_upsert(self):
        with self.prepare() as it:
            it.ensure_empty_collection("coll_name")

        with self.perform():
            for i in range(5):
                self.source.test["coll_name"].update_one(
                    {"i": i},
                    {
                        "$inc": {"i": (i * 100) - i},
                        "$set": {f"field_{i}": f"value_{i}"},
                    },
                    upsert=True,
                )

        self.compare_coll_options("coll_name")
        self.compare_coll_indexes("coll_name")
        self.compare_coll_content("coll_name")

    def test_update_many(self):
        with self.prepare() as it:
            it.ensure_empty_collection("coll_name")
            it.insert_documents("coll_name", [{"i": i} for i in range(5)])

        with self.perform():
            self.source.test["coll_name"].update_many({}, {"$inc": {"i": 100}})

        self.compare_coll_options("coll_name")
        self.compare_coll_indexes("coll_name")
        self.compare_coll_content("coll_name")

    def test_replace_one(self):
        with self.prepare() as it:
            it.ensure_empty_collection("coll_name")
            it.insert_documents("coll_name", [{"i": i} for i in range(5)])

        with self.perform():
            for i in range(5):
                self.source.test["coll_name"].replace_one(
                    {"i": i},
                    {
                        "i": (i * 100) - i,
                        f"field_{i}": f"value_{i}",
                    },
                )

        self.compare_coll_options("coll_name")
        self.compare_coll_indexes("coll_name")
        self.compare_coll_content("coll_name")

    def test_replace_one_upsert(self):
        with self.prepare() as it:
            it.ensure_empty_collection("coll_name")

        with self.perform():
            for i in range(5):
                self.source.test["coll_name"].replace_one(
                    {"i": i},
                    {
                        "i": (i * 100) - i,
                        f"field_{i}": f"value_{i}",
                    },
                    upsert=True,
                )

        self.compare_coll_options("coll_name")
        self.compare_coll_indexes("coll_name")
        self.compare_coll_content("coll_name")

    def test_delete_one(self):
        with self.prepare() as it:
            it.ensure_empty_collection("coll_name")
            it.insert_documents("coll_name", [{"i": i} for i in range(5)])

        with self.perform():
            self.source.test["coll_name"].delete_one({"i": 4})

        self.compare_coll_options("coll_name")
        self.compare_coll_indexes("coll_name")
        self.compare_coll_content("coll_name")

    def test_delete_many(self):
        with self.prepare() as it:
            it.ensure_empty_collection("coll_name")
            it.insert_documents("coll_name", [{"i": i} for i in range(5)])

        with self.perform():
            self.source.test["coll_name"].delete_many({})

        self.compare_coll_options("coll_name")
        self.compare_coll_indexes("coll_name")
        self.compare_coll_content("coll_name")

    def test_find_one_and_update(self):
        with self.prepare() as it:
            it.ensure_empty_collection("coll_name")
            it.insert_documents("coll_name", [{"i": i} for i in range(5)])

        with self.perform():
            for i in range(5):
                self.source.test["coll_name"].find_one_and_update(
                    {"i": i},
                    {
                        "$inc": {"i": (i * 100) - i},
                        "$set": {f"field_{i}": f"value_{i}"},
                    },
                )

        self.compare_coll_options("coll_name")
        self.compare_coll_indexes("coll_name")
        self.compare_coll_content("coll_name")

    def test_find_one_and_update_upsert(self):
        with self.prepare() as it:
            it.ensure_empty_collection("coll_name")

        with self.perform():
            for i in range(5):
                self.source.test["coll_name"].find_one_and_update(
                    {"i": i},
                    {
                        "$inc": {"i": (i * 100) - i},
                        "$set": {f"field_{i}": f"value_{i}"},
                    },
                    upsert=True,
                )

        self.compare_coll_options("coll_name")
        self.compare_coll_indexes("coll_name")
        self.compare_coll_content("coll_name")

    def test_find_one_and_replace(self):
        with self.prepare() as it:
            it.ensure_empty_collection("coll_name")
            it.insert_documents("coll_name", [{"i": i} for i in range(5)])

        with self.perform():
            for i in range(5):
                self.source.test["coll_name"].find_one_and_replace(
                    {"i": i},
                    {
                        "i": (i * 100) - i,
                        f"field_{i}": f"value_{i}",
                    },
                )

        self.compare_coll_options("coll_name")
        self.compare_coll_indexes("coll_name")
        self.compare_coll_content("coll_name")

    def test_find_one_and_replace_upsert(self):
        with self.prepare() as it:
            it.ensure_empty_collection("coll_name")

        with self.perform():
            for i in range(5):
                self.source.test["coll_name"].find_one_and_replace(
                    {"i": i},
                    {
                        "i": (i * 100) - i,
                        f"field_{i}": f"value_{i}",
                    },
                    upsert=True,
                )

        self.compare_coll_options("coll_name")
        self.compare_coll_indexes("coll_name")
        self.compare_coll_content("coll_name")

    def test_find_one_and_delete(self):
        with self.prepare() as it:
            it.ensure_empty_collection("coll_name")
            it.insert_documents("coll_name", [{"i": i} for i in range(5)])

        with self.perform():
            self.source.test["coll_name"].find_one_and_delete({"i": 4})

        self.compare_coll_options("coll_name")
        self.compare_coll_indexes("coll_name")
        self.compare_coll_content("coll_name")
