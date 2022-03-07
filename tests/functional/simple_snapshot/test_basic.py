import os
from datetime import datetime
import pytz
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

snapshots_check_col__snapshot_sql = """
    {% snapshot snapshot_actual %}

        {{
            config(
                target_database=var('target_database', database),
                target_schema=schema,
                unique_key='id || ' ~ "'-'" ~ ' || first_name',
                strategy='check',
                check_cols=['email'],
            )
        }}
        select * from {{target.database}}.{{schema}}.seed

    {% endsnapshot %}
"""


snapshots_check_col_noconfig__snapshot_sql = """
{% snapshot snapshot_actual %}
    select * from {{target.database}}.{{schema}}.seed
{% endsnapshot %}

{# This should be exactly the same #}
{% snapshot snapshot_checkall %}
    {{ config(check_cols='all') }}
    select * from {{target.database}}.{{schema}}.seed
{% endsnapshot %}
"""


class BasicSetup:
    """
    This tests is reused for multiple test cases
    """

    NUM_SNAPSHOT_MODELS = 1

    def assert_expected(self, table_comp):
        """
        This is pulled out so it can be overridden in subclasses
        """

        run_dbt(["test"])
        table_comp.assert_tables_equal("snapshot_actual", "snapshot_expected")

    def test_basic_snapshot(self, project):
        """
        This exact test is run multiple times with various macors/tests/snapshots
        """
        path = os.path.join(project.test_data_dir, "seed_pg.sql")
        project.run_sql_file(path)
        results = run_dbt(["snapshot"])
        assert len(results) == self.NUM_SNAPSHOT_MODELS
        table_comp = TableComparison(
            adapter=project.adapter, unique_schema=project.test_schema, database=project.database
        )

        self.assert_expected(table_comp)

        path = os.path.join(project.test_data_dir, "invalidate_postgres.sql")
        project.run_sql_file(path)

        path = os.path.join(project.test_data_dir, "update.sql")
        project.run_sql_file(path)

        results = run_dbt(["snapshot"])
        assert len(results) == 1

        self.assert_expected(table_comp)


class RefSetup:
    """
    This tests is reused for multiple test cases
    """

    NUM_SNAPSHOT_MODELS = 1

    def test_basic_ref(self, project):
        path = os.path.join(project.test_data_dir, "seed_pg.sql")
        project.run_sql_file(path)
        results = run_dbt(["snapshot"])
        assert len(results) == self.NUM_SNAPSHOT_MODELS

        results = run_dbt(["run"])
        assert len(results) == 1


# all of the tests below use one of both of the above tests with
# various combinations of snapshots and macros
class TestBasic(BasicSetup, RefSetup):
    @pytest.fixture
    def snapshots(self, snapshots_pg):  # noqa: F811
        return snapshots_pg


class TestCustomNamespace(BasicSetup):
    @pytest.fixture
    def snapshots(self, snapshots_pg_custom_namespaced):  # noqa: F811
        return snapshots_pg_custom_namespaced

    @pytest.fixture
    def macros(self, macros_custom_snapshot):  # noqa: F811
        return macros_custom_snapshot


class TestCustomSnapshot(BasicSetup, RefSetup):
    @pytest.fixture
    def snapshots(self, snapshots_pg_custom):  # noqa: F811
        return snapshots_pg_custom

    @pytest.fixture
    def macros(self, macros_custom_snapshot):  # noqa: F811
        return macros_custom_snapshot


class TestCheckCols(BasicSetup, RefSetup):
    # TODO: this test overrides how we check equality - it's broken
    NUM_SNAPSHOT_MODELS = 2

    def _assert_tables_equal_sql(self, relation_a, relation_b, columns=None):
        # When building the equality tests, only test columns that don't start
        # with 'dbt_', because those are time-sensitive
        if columns is None:
            columns = [
                c
                for c in self.get_relation_columns(relation_a)
                if not c[0].lower().startswith("dbt_")
            ]
        return super()._assert_tables_equal_sql(relation_a, relation_b, columns=columns)

    def assert_expected(self, table_comp):
        super().assert_expected(table_comp)
        self.assert_case_tables_equal("snapshot_checkall", "snapshot_expected")

    @pytest.fixture
    def snapshots(self):
        return {"snapshot.sql": snapshots_check_col__snapshot_sql}


# TODO: can't test below until TestCheckCols is fixed
class TestConfiguredCheckCols(TestCheckCols):
    @pytest.fixture
    def snapshots(self):
        return {"snapshot.sql": snapshots_check_col_noconfig__snapshot_sql}

    @pytest.fixture
    def project_config_update(self):
        snapshot_config = {
            "snapshots": {
                "test": {
                    "target_schema": self.project.test_schema,
                    "unique_key": "id || '-' || first_name",
                    "strategy": "check",
                    "check_cols": ["email"],
                }
            }
        }
        return snapshot_config


class TestUpdatedAtCheckCols(TestCheckCols):
    def _assert_tables_equal_sql(self, relation_a, relation_b, columns=None):
        revived_records = self.run_sql(
            """
            select
                id,
                updated_at,
                dbt_valid_from
            from {}
            """.format(
                relation_b
            ),
            fetch="all",
        )

        for result in revived_records:
            # result is a tuple, the updated_at is second and dbt_valid_from is latest
            assert isinstance(result[1], datetime)
            assert isinstance(result[2], datetime)
            assert result[1].replace(tzinfo=pytz.UTC) == result[2].replace(tzinfo=pytz.UTC)

        if columns is None:
            columns = [
                c
                for c in self.get_relation_columns(relation_a)
                if not c[0].lower().startswith("dbt_")
            ]
        return super()._assert_tables_equal_sql(relation_a, relation_b, columns=columns)

    def assert_expected(self, table_comp):
        super().assert_expected(table_comp)
        table_comp.assert_tables_equal("snapshot_checkall", "snapshot_expected")

    @pytest.fixture
    def snapshots(self):
        return {"snapshot.sql": snapshots_check_col_noconfig__snapshot_sql}

    @pytest.fixture
    def project_config_update(self):
        snapshot_config = {
            "snapshots": {
                "test": {
                    "target_schema": self.unique_schema(),
                    "unique_key": "id || '-' || first_name",
                    "strategy": "check",
                    "check_cols": "all",
                    "updated_at": "updated_at",
                }
            }
        }
        return snapshot_config
