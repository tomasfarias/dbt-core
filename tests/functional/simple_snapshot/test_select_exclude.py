import os
import pytest
from dbt.tests.util import run_dbt
from dbt.tests.tables import TableComparison
from tests.functional.simple_snapshot.fixtures import (  # noqa: F401
    models,
    seeds,
    macros,
    snapshots_select,
)


@pytest.fixture
def snapshots(snapshots_select):  # noqa: F811
    return snapshots_select


def test_select_snapshots(project):
    path = os.path.join(project.test_data_dir, "seed_pg.sql")
    project.run_sql_file(path)

    results = run_dbt(["snapshot"])
    assert len(results) == 4

    table_comp = TableComparison(
        adapter=project.adapter, unique_schema=project.test_schema, database=project.database
    )
    table_comp.assert_tables_equal("snapshot_castillo", "snapshot_castillo_expected")
    table_comp.assert_tables_equal("snapshot_alvarez", "snapshot_alvarez_expected")
    table_comp.assert_tables_equal("snapshot_kelly", "snapshot_kelly_expected")
    table_comp.assert_tables_equal("snapshot_actual", "snapshot_expected")

    path = os.path.join(project.test_data_dir, "invalidate_postgres.sql")
    project.run_sql_file(path)

    path = os.path.join(project.test_data_dir, "update.sql")
    project.run_sql_file(path)

    results = run_dbt(["snapshot"])
    assert len(results) == 4
    table_comp.assert_tables_equal("snapshot_castillo", "snapshot_castillo_expected")
    table_comp.assert_tables_equal("snapshot_alvarez", "snapshot_alvarez_expected")
    table_comp.assert_tables_equal("snapshot_kelly", "snapshot_kelly_expected")
    table_comp.assert_tables_equal("snapshot_actual", "snapshot_expected")


def test_exclude_snapshots(project):
    path = os.path.join(project.test_data_dir, "seed_pg.sql")
    project.run_sql_file(path)
    results = run_dbt(["snapshot", "--exclude", "snapshot_castillo"])
    assert len(results) == 3

    table_comp = TableComparison(
        adapter=project.adapter, unique_schema=project.test_schema, database=project.database
    )
    table_comp.assert_table_does_not_exist("snapshot_castillo")
    table_comp.assert_tables_equal("snapshot_alvarez", "snapshot_alvarez_expected")
    table_comp.assert_tables_equal("snapshot_kelly", "snapshot_kelly_expected")
    table_comp.assert_tables_equal("snapshot_actual", "snapshot_expected")


def test__select_snapshots(project):
    path = os.path.join(project.test_data_dir, "seed_pg.sql")
    project.run_sql_file(path)
    results = run_dbt(["snapshot", "--select", "snapshot_castillo"])
    assert len(results) == 1

    table_comp = TableComparison(
        adapter=project.adapter, unique_schema=project.test_schema, database=project.database
    )
    table_comp.assert_tables_equal("snapshot_castillo", "snapshot_castillo_expected")
    table_comp.assert_table_does_not_exist("snapshot_alvarez")
    table_comp.assert_table_does_not_exist("snapshot_kelly")
    table_comp.assert_table_does_not_exist("snapshot_actual")
