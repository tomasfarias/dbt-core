import pytest
from dbt.tests.util import run_dbt
from tests.functional.simple_snapshot.fixtures import seeds, macros, macros_custom_snapshot  # noqa

# @property
# def models(self):
#     return "models-checkall"

# @property
# def project_config(self):
#     return {
#         'config-version': 2,
#         'seed-paths': ['seeds'],
#         'macro-paths': ['macros-custom-snapshot', 'macros'],
#         'snapshot-paths': ['snapshots-checkall'],
#         'seeds': {
#             'quote_columns': False,
#         }
#     }

snapshots_checkall__snapshot_sql = """
{% snapshot my_snapshot %}
    {{ config(check_cols='all', unique_key='id', strategy='check', target_database=var('database', 'dbt'), target_schema=var('schema', 'schema')) }}
    select * from {{ ref(var('seed_name', 'seed')) }}
{% endsnapshot %}
"""


@pytest.fixture
def snapshots():
    return {"snapshot.sql": snapshots_checkall__snapshot_sql}


# @pytest.fixture
# def seeds(seeds):
#     return seeds


@pytest.fixture
def macros(macros, macros_custom_snapshot):  # noqa
    return macros.update(macros_custom_snapshot)


def test_renamed_source(project):
    run_dbt(["seed"])
    run_dbt(["snapshot", "--vars", f"{{schema: {project.test_schema}}}"])
    breakpoint()
    results = project.run_sql(
        "select * from {}.{}.my_snapshot".format(project.database, project.test_schema),
        fetch="all",
    )
    assert len(results) == 3
    for result in results:
        assert len(result) == 6

    # over ride the ref var in the snapshot definition to use a seed with an additional column, last_name

    run_dbt(["snapshot", "--vars", "{seed_name: seed_newcol}"])
    results = project.run_sql(
        "select * from {}.{}.my_snapshot where last_name is not NULL".format(
            project.database, project.test_schema
        ),
        fetch="all",
    )
    assert len(results) == 3

    for result in results:
        # new column
        assert len(result) == 7
        assert result[-1] is not None

    results = project.run_sql(
        "select * from {}.{}.my_snapshot where last_name is NULL".format(
            project.database, project.test_schema
        ),
        fetch="all",
    )
    assert len(results) == 3
    for result in results:
        # new column
        assert len(result) == 7
