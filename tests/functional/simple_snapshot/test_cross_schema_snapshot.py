import os
import pytest
from dbt.tests.util import run_dbt
from tests.functional.simple_snapshot.fixtures import models, macros, snapshots_pg  # noqa: F401


NUM_SNAPSHOT_MODELS = 1


@pytest.fixture(scope="class")
def snapshots(snapshots_pg):  # noqa: F811
    return snapshots_pg


def test_cross_schema_snapshot(project):
    # populate seed and snapshot tables
    path = os.path.join(project.test_data_dir, "seed_pg.sql")
    project.run_sql_file(path)

    target_schema = "{}_snapshotted".format(project.test_schema)

    # create a snapshot using the new schema
    results = run_dbt(["snapshot", "--vars", '{{"target_schema": "{}"}}'.format(target_schema)])
    assert len(results) == NUM_SNAPSHOT_MODELS

    # run dbt from test_schema with a ref to to new target_schema
    results = run_dbt(["run", "--vars", '{{"target_schema": {}}}'.format(target_schema)])
    assert len(results) == 1
