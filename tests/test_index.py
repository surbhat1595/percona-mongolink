# pylint: disable=missing-docstring,redefined-outer-name
from _base import BaseTesting


class TestClone(BaseTesting):
    def test_regular(self):
        self.ensure_no_indexes("coll_name")
        self.create_index("coll_name", {"i": 1})
        self.create_index("coll_name", {"j": -1})

        with self.perform():
            pass

        self.compare_coll_options("coll_name")
        self.compare_coll_indexes("coll_name")

    def test_create_unique(self):
        self.ensure_no_indexes("coll_name")
        self.create_index("coll_name", {"i": 1}, unique=True)

        with self.perform():
            pass

        self.compare_coll_options("coll_name")
        self.compare_coll_indexes("coll_name")

    def test_create_sparse(self):
        self.ensure_no_indexes("coll_name")
        self.create_index("coll_name", {"i": 1}, sparse=True)

        with self.perform():
            pass

        self.compare_coll_options("coll_name")
        self.compare_coll_indexes("coll_name")


class TestApply(BaseTesting):
    def test_create(self):
        self.ensure_no_indexes("coll_name")

        with self.perform():
            self.create_index("coll_name", {"i": 1})

        self.compare_coll_options("coll_name")
        self.compare_coll_indexes("coll_name")

    def test_create_unique(self):
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

    def test_drop(self):
        self.ensure_no_indexes("coll_name")
        index_name = self.source.test.coll_name.create_index({"i": 1})

        with self.perform():
            self.source.test.coll_name.drop_index(index_name)

        self.compare_coll_options("coll_name")
        self.compare_coll_indexes("coll_name")
