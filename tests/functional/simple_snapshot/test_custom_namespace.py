import pytest
from tests.functional.simple_snapshot.fixtures import (  # noqa: F401
    models,
    seeds,
    macros_custom_snapshot,
    snapshots_pg_custom_namespaced,
)
from tests.functional.simple_snapshot.common_tests import basic_snapshot_test  # noqa: F401


@pytest.fixture
def snapshots(snapshots_pg_custom_namespaced):  # noqa: F811
    return snapshots_pg_custom_namespaced


@pytest.fixture
def macros(macros_custom_snapshot):  # noqa: F811
    return macros_custom_snapshot


def test_simple_custom_snapshot_namespaced(basic_snapshot_test):  # noqa: F811
    basic_snapshot_test
