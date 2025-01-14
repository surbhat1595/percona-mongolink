# pylint: disable=missing-docstring,redefined-outer-name
from _base import BaseTesting


class TestClone(BaseTesting):
    def test_clone_regular(self):
        self.ensure_empty_collection("coll_name")
        self.ensure_no_indexes("coll_name")
        self.create_index("coll_name", {"i": 1})
        self.create_index("coll_name", {"j": -1})

        with self.perform():
            pass

        self.compare_coll_options("coll_name")
        self.compare_coll_indexes("coll_name")

    def test_clone_unique(self):
        self.ensure_empty_collection("coll_name")
        self.ensure_no_indexes("coll_name")
        self.create_index("coll_name", {"i": 1}, unique=True)

        with self.perform():
            pass

        self.compare_coll_options("coll_name")
        self.compare_coll_indexes("coll_name")

    def test_clone_sparse(self):
        self.ensure_empty_collection("coll_name")
        self.ensure_no_indexes("coll_name")
        self.create_index("coll_name", {"i": 1}, sparse=True)

        with self.perform():
            pass

        self.compare_coll_options("coll_name")
        self.compare_coll_indexes("coll_name")

    def test_clone_partial(self):
        self.ensure_empty_collection("coll_name")
        self.ensure_no_indexes("coll_name")
        self.insert_documents("coll_name", [{"i": i, "j": i} for i in range(10)])
        self.create_index("coll_name", {"i": 1}, partialFilterExpression={"j": {"$gt": 5}})

        with self.perform():
            pass

        self.compare_coll_options("coll_name")
        self.compare_coll_indexes("coll_name")


class TestApply(BaseTesting):
    def test_create_regular(self):
        self.ensure_empty_collection("coll_name")
        self.ensure_no_indexes("coll_name")

        with self.perform():
            self.create_index("coll_name", {"i": 1})

        self.compare_coll_options("coll_name")
        self.compare_coll_indexes("coll_name")

    def test_create_unique(self):
        self.ensure_empty_collection("coll_name")
        self.ensure_no_indexes("coll_name")

        with self.perform():
            self.create_index("coll_name", {"i": 1}, unique=True)

        self.compare_coll_options("coll_name")
        self.compare_coll_indexes("coll_name")

    def test_create_sparse(self):
        self.ensure_no_indexes("coll_name")

        with self.perform():
            self.create_index("coll_name", {"i": 1}, sparse=True)

        self.compare_coll_options("coll_name")
        self.compare_coll_indexes("coll_name")

    def test_create_partial(self):
        self.ensure_no_indexes("coll_name")
        self.insert_documents("coll_name", [{"i": i, "j": i} for i in range(10)])

        with self.perform():
            self.create_index("coll_name", {"i": 1}, partialFilterExpression={"j": {"$gt": 5}})

        self.compare_coll_options("coll_name")
        self.compare_coll_indexes("coll_name")

    def test_drop_cloned(self):
        self.ensure_no_indexes("coll_name")
        index_name = self.source.test.coll_name.create_index({"i": 1})

        with self.perform():
            self.source.test.coll_name.drop_index(index_name)

        self.compare_coll_options("coll_name")
        self.compare_coll_indexes("coll_name")

    def test_drop_created(self):
        self.ensure_no_indexes("coll_name")

        with self.perform():
            index_name = self.source.test.coll_name.create_index({"i": 1})
            self.source.test.coll_name.drop_index(index_name)

        self.compare_coll_options("coll_name")
        self.compare_coll_indexes("coll_name")
