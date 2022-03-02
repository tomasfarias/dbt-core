import pytest
from tests.functional.simple_snapshot.fixtures import (  # noqa: F401
    models,
    seeds,
    macros,
    snapshots_pg,
)
from tests.functional.simple_snapshot.common_tests import (  # noqa: F401
    basic_snapshot_test,
    basic_ref_test,
)


@pytest.fixture
def snapshots(snapshots_pg):  # noqa: F811
    return snapshots_pg


def test_ref_snapshot(basic_ref_test):  # noqa: F811
    basic_ref_test


def test_simple_snapshot(basic_snapshot_test):  # noqa: F811
    basic_snapshot_test
