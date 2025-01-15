# pylint: disable=missing-docstring,redefined-outer-name
import pytest
from _base import BaseTesting

from mlink import Runner


@pytest.mark.parametrize("phase", [Runner.Phase.CLONE, Runner.Phase.APPLY])
class TestCRUDOperation(BaseTesting):
    def test_insert_one(self, phase):
        self.drop_database("db_1")

        with self.perform(phase):
            for i in range(5):
                self.source["db_1"]["coll_1"].insert_one({"i": i})

        self.compare_all()

    def test_insert_many(self, phase):
        self.drop_database("db_1")

        with self.perform(phase):
            self.source["db_1"]["coll_1"].insert_many([{"i": i} for i in range(5)])

        self.compare_all()

    def test_update_one(self, phase):
        self.drop_database("db_1")
        self.insert_documents("db_1", "coll_1", [{"i": i} for i in range(5)])

        with self.perform(phase):
            for i in range(5):
                self.source["db_1"]["coll_1"].update_one(
                    {"i": i},
                    {
                        "$inc": {"i": (i * 100) - i},
                        "$set": {f"field_{i}": f"value_{i}"},
                    },
                )

        self.compare_all()

    def test_update_many(self, phase):
        self.drop_database("db_1")
        self.insert_documents("db_1", "coll_1", [{"i": i} for i in range(5)])

        with self.perform(phase):
            self.source["db_1"]["coll_1"].update_many({}, {"$inc": {"i": 100}})

        self.compare_all()

    def test_replace_one(self, phase):
        self.drop_database("db_1")
        self.insert_documents("db_1", "coll_1", [{"i": i} for i in range(5)])

        with self.perform(phase):
            for i in range(5):
                self.source["db_1"]["coll_1"].replace_one(
                    {"i": i},
                    {
                        "i": (i * 100) - i,
                        f"field_{i}": f"value_{i}",
                    },
                )

        self.compare_all()

    def test_delete_one(self, phase):
        self.drop_database("db_1")
        self.insert_documents("db_1", "coll_1", [{"i": i} for i in range(5)])

        with self.perform(phase):
            self.source["db_1"]["coll_1"].delete_one({"i": 4})

        self.compare_all()

    def test_delete_many(self, phase):
        self.drop_database("db_1")
        self.insert_documents("db_1", "coll_1", [{"i": i} for i in range(5)])

        with self.perform(phase):
            self.source["db_1"]["coll_1"].delete_many({"i": {"$gt": 2}})

        self.compare_all()

    def test_find_one_and_update(self, phase):
        self.drop_database("db_1")
        self.insert_documents("db_1", "coll_1", [{"i": i} for i in range(5)])

        with self.perform(phase):
            for i in range(5):
                self.source["db_1"]["coll_1"].find_one_and_update(
                    {"i": i},
                    {
                        "$inc": {"i": (i * 100) - i},
                        "$set": {f"field_{i}": f"value_{i}"},
                    },
                )

        self.compare_all()

    def test_find_one_and_replace(self, phase):
        self.drop_database("db_1")
        self.insert_documents("db_1", "coll_1", [{"i": i} for i in range(5)])

        with self.perform(phase):
            for i in range(5):
                self.source["db_1"]["coll_1"].find_one_and_replace(
                    {"i": i},
                    {
                        "i": (i * 100) - i,
                        f"field_{i}": f"value_{i}",
                    },
                )

        self.compare_all()

    def test_find_one_and_delete(self, phase):
        self.drop_database("db_1")
        self.insert_documents("db_1", "coll_1", [{"i": i} for i in range(5)])

        with self.perform(phase):
            self.source["db_1"]["coll_1"].find_one_and_delete({"i": 4})

        self.compare_all()

    def test_upsert_by_update_one(self, phase):
        self.drop_database("db_1")

        with self.perform(phase):
            for i in range(5):
                self.source["db_1"]["coll_1"].update_one(
                    {"i": i},
                    {
                        "$inc": {"i": (i * 100) - i},
                        "$set": {f"field_{i}": f"value_{i}"},
                    },
                    upsert=True,
                )

        self.compare_all()

    def test_upsert_by_update_many(self, phase):
        self.drop_database("db_1")

        with self.perform(phase):
            self.source["db_1"]["coll_1"].update_many({}, {"$inc": {"i": 100}}, upsert=True)

        self.compare_all()

    def test_upsert_by_replace_one(self, phase):
        self.drop_database("db_1")

        with self.perform(phase):
            for i in range(5):
                self.source["db_1"]["coll_1"].replace_one(
                    {"i": i},
                    {
                        "i": (i * 100) - i,
                        f"field_{i}": f"value_{i}",
                    },
                    upsert=True,
                )

        self.compare_all()

    def test_upsert_by_find_one_and_update(self, phase):
        self.drop_database("db_1")

        with self.perform(phase):
            for i in range(5):
                self.source["db_1"]["coll_1"].find_one_and_update(
                    {"i": i},
                    {
                        "$inc": {"i": (i * 100) - i},
                        "$set": {f"field_{i}": f"value_{i}"},
                    },
                    upsert=True,
                )

        self.compare_all()

    def test_upsert_by_find_one_and_replace(self, phase):
        self.drop_database("db_1")

        with self.perform(phase):
            for i in range(5):
                self.source["db_1"]["coll_1"].find_one_and_replace(
                    {"i": i},
                    {
                        "i": (i * 100) - i,
                        f"field_{i}": f"value_{i}",
                    },
                    upsert=True,
                )

        self.compare_all()
