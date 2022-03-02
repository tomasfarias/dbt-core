import os
import pytest
from dbt.tests.util import run_dbt
from tests.functional.simple_snapshot.fixtures import models, macros, snapshots_pg  # noqa: F401
from tests.functional.simple_snapshot.common_tests import (  # noqa: F401
    basic_snapshot_test,
    basic_ref_test,
    NUM_SNAPSHOT_MODELS,
)


@pytest.fixture
def snapshots(snapshots_pg):  # noqa: F811
    return snapshots_pg


def target_schema(self):
    return "{}_snapshotted".format(self.unique_schema())


# TODO: this seems significant - come back after more tests are converted
# def setUp(self):
#     super().setUp()
#     self._created_schemas.add(
#         self._get_schema_fqn(self.default_database, self.target_schema()),
#     )


def test_cross_schema_snapshot(project):
    path = os.path.join(project.test_data_dir, "seed_pg.sql")
    project.run_sql_file(path)

    target_schema = "{}_snapshotted".format(project.test_schema)

    results = run_dbt(["snapshot", "--vars", '{{"target_schema": {}}}'.format(target_schema)])
    assert len(results) == NUM_SNAPSHOT_MODELS

    results = run_dbt(["run", "--vars", '{{"target_schema": {}}}'.format(target_schema)])
    assert len(results) == 999  # 1 - failing this to come back to the test


# ORIGINAL
# def project_config(self):
#     return {
#         "config-version": 2,
#         "seed-paths": ["seeds"],
#         "snapshot-paths": ["snapshots-select-noconfig"],
#         "snapshots": {
#             "test": {
#                 "target_schema": self.unique_schema(),
#                 "unique_key": "id || '-' || first_name",
#                 "strategy": "timestamp",
#                 "updated_at": "updated_at",
#             },
#         },
#         "macro-paths": ["macros"],
#     }


# NUM_SNAPSHOT_MODELS = 1


# def setUp(self):
#     super().setUp()
#     self._created_schemas.add(
#         self._get_schema_fqn(self.default_database, self.target_schema()),
#     )


# def schema(self):
#     return "simple_snapshot_004"


# def models(self):
#     return "models"


# def project_config(self):
#     paths = ["snapshots-pg"]
#     return {
#         "config-version": 2,
#         "snapshot-paths": paths,
#         "macro-paths": ["macros"],
#     }


# def target_schema(self):
#     return "{}_snapshotted".format(self.unique_schema())


# def run_snapshot(self):
#     return self.run_dbt(
#         ["snapshot", "--vars", '{{"target_schema": {}}}'.format(self.target_schema())]
#     )


# def test__postgres__cross_schema_snapshot(self):
#     self.run_sql_file("seed_pg.sql")

#     results = self.run_snapshot()
#     assert len(results) == self.NUM_SNAPSHOT_MODELS

#     results = self.run_dbt(
#         ["run", "--vars", '{{"target_schema": {}}}'.format(self.target_schema())]
#     )
#     assert len(results) == 1
