import pytest
from tests.functional.simple_snapshot.fixtures import (  # noqa: F401
    models,
    seeds,
    macros_custom_snapshot,
    snapshots_pg_custom,
)
from tests.functional.simple_snapshot.common_tests import (  # noqa: F401
    basic_snapshot_test,
    basic_ref_test,
)


@pytest.fixture
def snapshots(snapshots_pg_custom):  # noqa: F811
    return snapshots_pg_custom


@pytest.fixture
def macros(macros_custom_snapshot):  # noqa: F811
    return macros_custom_snapshot


def test_ref_custom_snapshot(basic_ref_test):  # noqa: F811
    basic_ref_test


def test_simple_custom_snapshot(basic_snapshot_test):  # noqa: F811
    basic_snapshot_test
