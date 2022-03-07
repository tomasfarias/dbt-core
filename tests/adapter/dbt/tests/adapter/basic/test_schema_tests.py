import pytest
from dbt.tests.util import run_dbt
from dbt.tests.adapter.basic.files import (
    seeds_base_csv,
    schema_test_seed_yml,
    base_view_sql,
    base_table_sql,
    schema_base_yml,
    schema_test_view_yml,
    schema_test_table_yml,
)


class TestSchemaTests:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "schema_test"}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv,
            "schema.yml": schema_test_seed_yml,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "view_model.sql": base_view_sql,
            "table_model.sql": base_table_sql,
            "schema.yml": schema_base_yml,
            "schema_view.yml": schema_test_view_yml,
            "schema_table.yml": schema_test_table_yml,
        }

    def test_schema_tests(self, project):
        # seed command
        results = run_dbt(["seed"])

        # test command selecting base model
        results = run_dbt(["test", "-m", "base"])
        assert len(results) == 1

        # run command
        results = run_dbt(["run"])
        assert len(results) == 2

        # test command, all tests
        results = run_dbt(["test"])
        assert len(results) == 3
