import os
import pytest
from dbt.tests.util import run_dbt
from dbt.tests.tables import TableComparison
from tests.functional.simple_snapshot.fixtures import (  # noqa: F401
    models,
    seeds,
    macros,
    snapshots_pg,
    macros_custom_snapshot,
    snapshots_pg_custom_namespaced,
    snapshots_pg_custom,
)

NUM_SNAPSHOT_MODELS = 1


class BasicSetup:
    """
    This tests is reused for multiple test cases
    """

    def test_basic_snapshot(self, project):
        """
        This exact test is run multiple times with various macors/tests/snapshots
        """
        path = os.path.join(project.test_data_dir, "seed_pg.sql")
        project.run_sql_file(path)
        results = run_dbt(["snapshot"])
        assert len(results) == NUM_SNAPSHOT_MODELS

        table_comp = TableComparison(
            adapter=project.adapter, unique_schema=project.test_schema, database=project.database
        )

        run_dbt(["test"])
        table_comp.assert_tables_equal("snapshot_actual", "snapshot_expected")

        path = os.path.join(project.test_data_dir, "invalidate_postgres.sql")
        project.run_sql_file(path)

        path = os.path.join(project.test_data_dir, "update.sql")
        project.run_sql_file(path)

        results = run_dbt(["snapshot"])
        assert len(results) == 1

        run_dbt(["test"])
        table_comp.assert_tables_equal("snapshot_actual", "snapshot_expected")


class RefSetup:
    """
    This tests is reused for multiple test cases
    """

    def test_basic_ref(self, project):
        path = os.path.join(project.test_data_dir, "seed_pg.sql")
        project.run_sql_file(path)
        results = run_dbt(["snapshot"])
        assert len(results) == NUM_SNAPSHOT_MODELS

        results = run_dbt(["run"])
        assert len(results) == 1


# all of the tests below use one of both of the above tests with
# various combinations of snapshots and macros
class TestBasic(BasicSetup, RefSetup):  # test_basic.py
    @pytest.fixture
    def snapshots(self, snapshots_pg):  # noqa: F811
        return snapshots_pg


class TestCustomNamespace(BasicSetup):  # test_custom_namespace.py
    @pytest.fixture
    def snapshots(self, snapshots_pg_custom_namespaced):  # noqa: F811
        return snapshots_pg_custom_namespaced

    @pytest.fixture
    def macros(self, macros_custom_snapshot):  # noqa: F811
        return macros_custom_snapshot


class TestCustomSnapshot(BasicSetup, RefSetup):  # test_custom_snapshot.py
    @pytest.fixture
    def snapshots(self, snapshots_pg_custom):  # noqa: F811
        return snapshots_pg_custom

    @pytest.fixture
    def macros(self, macros_custom_snapshot):  # noqa: F811
        return macros_custom_snapshot
