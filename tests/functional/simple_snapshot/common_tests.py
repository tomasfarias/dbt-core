import os
import pytest
from dbt.tests.util import run_dbt
from dbt.tests.tables import TableComparison


NUM_SNAPSHOT_MODELS = 1


@pytest.fixture
def basic_snapshot_test(project):
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


@pytest.fixture
def basic_ref_test(project):
    path = os.path.join(project.test_data_dir, "seed_pg.sql")
    project.run_sql_file(path)
    results = run_dbt(["snapshot"])
    assert len(results) == NUM_SNAPSHOT_MODELS

    results = run_dbt(["run"])
    assert len(results) == 1
