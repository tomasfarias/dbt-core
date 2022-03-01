import os
import pytest
from dbt.tests.util import run_dbt, read_file
from dbt.tests.tables import TableComparison


NUM_SNAPSHOT_MODELS = 1

model_sql = "select * from {{ ref('snapshot_actual') }}"

snapshot_actual_sql = """
{% snapshot snapshot_actual %}

    {{
        config(
            target_database=var('target_database', database),
            target_schema=var('target_schema', schema),
            unique_key='id || ' ~ "'-'" ~ ' || first_name',
            strategy='timestamp',
            updated_at='updated_at',
        )
    }}

    {% if var('invalidate_hard_deletes', 'false') | as_bool %}
        {{ config(invalidate_hard_deletes=True) }}
    {% endif %}

    select * from {{target.database}}.{{target.schema}}.seed

{% endsnapshot %}
"""


@pytest.fixture
def models():
    return {"my_model.sql": model_sql}


@pytest.fixture
def snapshots():
    return {"snapshot_actual.sql": snapshot_actual_sql}


@pytest.fixture
def seeds(test_data_dir):
    # Read seed files and return
    seed_csv = read_file(test_data_dir, "seed.csv")
    seed_newcol_csv = read_file(test_data_dir, "seed_newcol.csv")
    return {
        "seed.csv": seed_csv,
        "seed_newcol.csv": seed_newcol_csv,
        }


@pytest.fixture
def macros(test_data_dir):
    shared_macros = read_file(test_data_dir, "shared_macros.sql")
    return {"shared_macros.sql": shared_macros}


def test_ref_snapshot(project):
    path = os.path.join(project.test_data_dir, "seed_pg.sql")
    project.run_sql_file(path)
    results = run_dbt(['snapshot'])
    assert len(results) == NUM_SNAPSHOT_MODELS

    results = run_dbt(['run'])
    assert len(results) == 1

def test_simple_snapshot(project):
    
    path = os.path.join(project.test_data_dir, "seed_pg.sql")
    project.run_sql_file(path)
    results = run_dbt(['snapshot'])
    assert len(results) == NUM_SNAPSHOT_MODELS

    table_comp = TableComparison(
        adapter=project.adapter, unique_schema=project.test_schema, database=project.database
    )

    run_dbt(['test'])
    table_comp.assert_tables_equal('snapshot_actual', 'snapshot_expected')

    path = os.path.join(project.test_data_dir, "invalidate_postgres.sql")
    project.run_sql_file(path)

    path = os.path.join(project.test_data_dir, "update.sql")
    project.run_sql_file(path)

    results = run_dbt(['snapshot'])
    assert len(results) == 1

    run_dbt(['test'])
    table_comp.assert_tables_equal('snapshot_actual', 'snapshot_expected')