import os
import pytest
from dbt.tests.util import run_dbt
from dbt.tests.tables import TableComparison
from tests.functional.simple_snapshot.fixtures import (  # noqa: F401
    models,
    seeds,
    macros,
    snapshots_pg__snapshot_sql,
    snapshots_select_noconfig,
)


snapshots_select__snapshot_sql = """
{% snapshot snapshot_castillo %}

    {{
        config(
            target_database=var('target_database', database),
            target_schema=schema,
            unique_key='id || ' ~ "'-'" ~ ' || first_name',
            strategy='timestamp',
            updated_at='"1-updated_at"',
        )
    }}
    select id,first_name,last_name,email,gender,ip_address,updated_at as "1-updated_at" from {{target.database}}.{{schema}}.seed where last_name = 'Castillo'

{% endsnapshot %}

{% snapshot snapshot_alvarez %}

    {{
        config(
            target_database=var('target_database', database),
            target_schema=schema,
            unique_key='id || ' ~ "'-'" ~ ' || first_name',
            strategy='timestamp',
            updated_at='updated_at',
        )
    }}
    select * from {{target.database}}.{{schema}}.seed where last_name = 'Alvarez'

{% endsnapshot %}


{% snapshot snapshot_kelly %}
    {# This has no target_database set, which is allowed! #}
    {{
        config(
            target_schema=schema,
            unique_key='id || ' ~ "'-'" ~ ' || first_name',
            strategy='timestamp',
            updated_at='updated_at',
        )
    }}
    select * from {{target.database}}.{{schema}}.seed where last_name = 'Kelly'

{% endsnapshot %}
"""


class SelectExcludeSetup:
    """
    This tests is reused for multiple test cases
    """

    def test_all_snapshots(self, project):
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

    def test_exclude_snapshots(self, project):
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

    def test_select_snapshots(self, project):
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


# all of the tests below use one of both of the above tests with
# various combinations of snapshots and macros
class TestSelectBasic(SelectExcludeSetup):
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "snapshot.sql": snapshots_pg__snapshot_sql,
            "snapshot_select.sql": snapshots_select__snapshot_sql,
        }


class TestSelectConfigured(SelectExcludeSetup):
    @pytest.fixture(scope="class")
    def snapshots(self, snapshots_select_noconfig):  # noqa: F811
        return snapshots_select_noconfig

    # TODO: don't have access to project here so this breaks
    @pytest.fixture(scope="class")
    def project_config_update(self):
        snapshot_config = {
            "snapshots": {
                "test": {
                    "target_schema": self.project.test_schema,
                    "unique_key": "id || '-' || first_name",
                    "strategy": "timestamp",
                    "updated_at": "updated_at",
                }
            }
        }
        return snapshot_config
