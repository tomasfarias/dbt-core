import os
import pytest
from dbt.tests.util import run_dbt, read_file


macros_custom_snapshot_sql = """
{# A "custom" strategy that's really just the timestamp one #}
{% macro snapshot_custom_strategy(node, snapshotted_rel, current_rel, config, target_exists) %}
    {% set primary_key = config['unique_key'] %}
    {% set updated_at = config['updated_at'] %}

    {% set row_changed_expr -%}
        ({{ snapshotted_rel }}.{{ updated_at }} < {{ current_rel }}.{{ updated_at }})
    {%- endset %}

    {% set scd_id_expr = snapshot_hash_arguments([primary_key, updated_at]) %}

    {% do return({
        "unique_key": primary_key,
        "updated_at": updated_at,
        "row_changed": row_changed_expr,
        "scd_id": scd_id_expr
    }) %}
{% endmacro %}
"""

my_snapshot_sql = """
{% snapshot my_snapshot %}
    {{ config(check_cols='all', unique_key='id', strategy='check', target_database=database, target_schema=schema) }}
    select * from {{ ref(var('seed_name', 'seed')) }}
{% endsnapshot %}
"""


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
def snapshots():
    return {"my_snapshot.sql": my_snapshot_sql}

@pytest.fixture
def macros(test_data_dir):
    shared_macros = read_file(test_data_dir, "shared_macros.sql")
    return {
        "shared_macros.sql": shared_macros,
        "macros_custom_snapshot_sql.sql": macros_custom_snapshot_sql
        }





# def models(self):   -- TODO: what is this???
#     return "models-checkall"


def test_renamed_source(project):
    run_dbt(['seed'])

    run_dbt(['snapshot'])
    results = project.run_sql(
        'select * from {}.{}.my_snapshot'.format(project.adapter.quote(project.database), project.test_schema),
        fetch='all'
    )
    assert len(results) == 3
    for result in results:
        assert len(result) == 6

    # over ride the ref var in the snapshot definition to use a seed with an additional column, last_name
    breakpoint()
    run_dbt(['snapshot', '--vars', '{seed_name: seed_newcol}'])
    results = project.run_sql(
        'select * from {}.{}.my_snapshot where last_name is not NULL'.format(project.adapter.quote(project.database), project.test_schema),
        fetch='all'
    )
    assert len(results) == 3

    for result in results:
        # new column
        assert len(result) == 7
        assert result[-1] is not None

    results = project.run_sql(
        'select * from {}.{}.my_snapshot where last_name is NULL'.format(project.database, project.test_schema),
        fetch='all'
    )
    assert len(results) == 3
    for result in results:
        # new column
        assert len(result) == 7