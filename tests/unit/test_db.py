import pytest

from retail_pipeline.db import transaction


class FakeConnection:
    def __init__(self):
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        raise NotImplementedError

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def test_transaction_commits_when_block_succeeds():
    connection = FakeConnection()

    with transaction(connection):
        pass

    assert (connection.commits, connection.rollbacks) == (1, 0)


def test_transaction_rolls_back_and_reraises_on_error():
    connection = FakeConnection()

    with pytest.raises(ValueError, match="boom"), transaction(connection):
        raise ValueError("boom")

    assert (connection.commits, connection.rollbacks) == (0, 1)
