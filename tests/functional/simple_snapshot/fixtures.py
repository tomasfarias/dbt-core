import pytest
from dbt.tests.fixtures.project import write_project_files


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

snapshots_pg_custom__snapshot_sql = """
{% snapshot snapshot_actual %}

    {{
        config(
            target_database=var('target_database', database),
            target_schema=var('target_schema', schema),
            unique_key='id || ' ~ "'-'" ~ ' || first_name',
            strategy='custom',
            updated_at='updated_at',
        )
    }}
    select * from {{target.database}}.{{target.schema}}.seed

{% endsnapshot %}
"""


# models_collision__snapshot_actual_sql = """
# select 1 as id
# """

macros_custom_snapshot__custom_sql = """
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

# test_snapshots_slow__test_timestamps_sql = """

# /*
#     Assert that the dbt_valid_from of the latest record
#     is equal to the dbt_valid_to of the previous record
# */

# with snapshot as (

#     select * from {{ ref('my_slow_snapshot') }}

# )

# select
#     snap1.id,
#     snap1.dbt_valid_from as new_valid_from,
#     snap2.dbt_valid_from as old_valid_from,
#     snap2.dbt_valid_to as old_valid_to

# from snapshot as snap1
# join snapshot as snap2 on snap1.id = snap2.id
# where snap1.dbt_valid_to is null
#   and snap2.dbt_valid_to is not null
#   and snap1.dbt_valid_from != snap2.dbt_valid_to
# """

# snapshots_check_col__snapshot_sql = """
# {% snapshot snapshot_actual %}

#     {{
#         config(
#             target_database=var('target_database', database),
#             target_schema=schema,
#             unique_key='id || ' ~ "'-'" ~ ' || first_name',
#             strategy='check',
#             check_cols=['email'],
#         )
#     }}
#     select * from {{target.database}}.{{schema}}.seed

# {% endsnapshot %}

# {# This should be exactly the same #}
# {% snapshot snapshot_checkall %}
#     {{
#         config(
#             target_database=var('target_database', database),
#             target_schema=schema,
#             unique_key='id || ' ~ "'-'" ~ ' || first_name',
#             strategy='check',
#             check_cols='all',
#         )
#     }}
#     select * from {{target.database}}.{{schema}}.seed
# {% endsnapshot %}
# """

# snapshots_invalid__snapshot_sql = """
# {# make sure to never name this anything with `target_schema` in the name, or the test will be invalid! #}
# {% snapshot missing_field_target_underscore_schema %}
# 	{# missing the mandatory target_schema parameter #}
#     {{
#         config(
#             unique_key='id || ' ~ "'-'" ~ ' || first_name',
#             strategy='timestamp',
#             updated_at='updated_at',
#         )
#     }}
#     select * from {{target.database}}.{{schema}}.seed

# {% endsnapshot %}
# """

models__schema_yml = """
version: 2
snapshots:
  - name: snapshot_actual
    tests:
      - mutually_exclusive_ranges
    config:
      meta:
        owner: 'a_owner'
"""

models__ref_snapshot_sql = """
select * from {{ ref('snapshot_actual') }}
"""

macros__test_no_overlaps_sql = """
{% macro get_snapshot_unique_id() -%}
    {{ return(adapter.dispatch('get_snapshot_unique_id')()) }}
{%- endmacro %}

{% macro default__get_snapshot_unique_id() -%}
  {% do return("id || '-' || first_name") %}
{%- endmacro %}

{#
    mostly copy+pasted from dbt_utils, but I removed some parameters and added
    a query that calls get_snapshot_unique_id
#}
{% test mutually_exclusive_ranges(model) %}

with base as (
    select {{ get_snapshot_unique_id() }} as dbt_unique_id,
    *
    from {{ model }}
),
window_functions as (

    select
        dbt_valid_from as lower_bound,
        coalesce(dbt_valid_to, '2099-1-1T00:00:01') as upper_bound,

        lead(dbt_valid_from) over (
            partition by dbt_unique_id
            order by dbt_valid_from
        ) as next_lower_bound,

        row_number() over (
            partition by dbt_unique_id
            order by dbt_valid_from desc
        ) = 1 as is_last_record

    from base

),

calc as (
    -- We want to return records where one of our assumptions fails, so we'll use
    -- the `not` function with `and` statements so we can write our assumptions nore cleanly
    select
        *,

        -- For each record: lower_bound should be < upper_bound.
        -- Coalesce it to return an error on the null case (implicit assumption
        -- these columns are not_null)
        coalesce(
            lower_bound < upper_bound,
            is_last_record
        ) as lower_bound_less_than_upper_bound,

        -- For each record: upper_bound {{ allow_gaps_operator }} the next lower_bound.
        -- Coalesce it to handle null cases for the last record.
        coalesce(
            upper_bound = next_lower_bound,
            is_last_record,
            false
        ) as upper_bound_equal_to_next_lower_bound

    from window_functions

),

validation_errors as (

    select
        *
    from calc

    where not(
        -- THE FOLLOWING SHOULD BE TRUE --
        lower_bound_less_than_upper_bound
        and upper_bound_equal_to_next_lower_bound
    )
)

select * from validation_errors
{% endtest %}
"""

# test_snapshots_changing_strategy__test_snapshot_sql = """

# {# /*
#     Given the repro case for the snapshot build, we'd
#     expect to see both records have color='pink'
#     in their most recent rows.
# */ #}

# with expected as (

#     select 1 as id, 'pink' as color union all
#     select 2 as id, 'pink' as color

# ),

# actual as (

#     select id, color
#     from {{ ref('my_snapshot') }}
#     where color = 'pink'
#       and dbt_valid_to is null

# )

# select * from expected
# except
# select * from actual

# union all

# select * from actual
# except
# select * from expected
# """

# snapshots_slow__snapshot_sql = """

# {% snapshot my_slow_snapshot %}

#     {{
#         config(
#             target_database=var('target_database', database),
#             target_schema=schema,
#             unique_key='id',
#             strategy='timestamp',
#             updated_at='updated_at'
#         )
#     }}

#     select
#         id,
#         updated_at,
#         seconds

#     from {{ ref('gen') }}

# {% endsnapshot %}
# """

snapshots_select_noconfig__snapshot_sql = """
{% snapshot snapshot_actual %}

    {{
        config(
            target_database=var('target_database', database),
            target_schema=var('target_schema', schema),
        )
    }}
    select * from {{target.database}}.{{target.schema}}.seed

{% endsnapshot %}

# {% snapshot snapshot_castillo %}

#     {{
#         config(
#             target_database=var('target_database', database),
#             updated_at='"1-updated_at"',
#         )
#     }}
#     select id,first_name,last_name,email,gender,ip_address,updated_at as "1-updated_at" from {{target.database}}.{{schema}}.seed where last_name = 'Castillo'

# {% endsnapshot %}

# {% snapshot snapshot_alvarez %}

#     {{
#         config(
#             target_database=var('target_database', database),
#         )
#     }}
#     select * from {{target.database}}.{{schema}}.seed where last_name = 'Alvarez'

# {% endsnapshot %}


# {% snapshot snapshot_kelly %}
#     {# This has no target_database set, which is allowed! #}
#     select * from {{target.database}}.{{schema}}.seed where last_name = 'Kelly'

# {% endsnapshot %}
# """

# snapshots_pg_custom_invalid__snapshot_sql = """
# {% snapshot snapshot_actual %}
#     {# this custom strategy does not exist  in the 'dbt' package #}
#     {{
#         config(
#             target_database=var('target_database', database),
#             target_schema=var('target_schema', schema),
#             unique_key='id || ' ~ "'-'" ~ ' || first_name',
#             strategy='dbt.custom',
#             updated_at='updated_at',
#         )
#     }}
#     select * from {{target.database}}.{{target.schema}}.seed

# {% endsnapshot %}
# """

seeds__seed_newcol_csv = """
id,first_name,last_name
1,Judith,Kennedy
2,Arthur,Kelly
3,Rachel,Moreno
"""

seeds__seed_csv = """
id,first_name
1,Judith
2,Arthur
3,Rachel
"""

# snapshots_changing_strategy__snapshot_sql = """

# {#
#     REPRO:
#         1. Run with check strategy
#         2. Add a new ts column and run with check strategy
#         3. Run with timestamp strategy on new ts column

#         Expect: new entry is added for changed rows in (3)
# #}


# {% snapshot my_snapshot %}

#     {#--------------- Configuration ------------ #}

#     {{ config(
#         target_schema=schema,
#         unique_key='id'
#     ) }}

#     {% if var('strategy') == 'timestamp' %}
#         {{ config(strategy='timestamp', updated_at='updated_at') }}
#     {% else %}
#         {{ config(strategy='check', check_cols=['color']) }}
#     {% endif %}

#     {#--------------- Test setup ------------ #}

#     {% if var('step') == 1 %}

#         select 1 as id, 'blue' as color
#         union all
#         select 2 as id, 'red' as color

#     {% elif var('step') == 2 %}

#         -- change id=1 color from blue to green
#         -- id=2 is unchanged when using the check strategy
#         select 1 as id, 'green' as color, '2020-01-01'::date as updated_at
#         union all
#         select 2 as id, 'red' as color, '2020-01-01'::date as updated_at

#     {% elif var('step') == 3 %}

#         -- bump timestamp for both records. Expect that after this runs
#         -- using the timestamp strategy, both ids should have the color
#         -- 'pink' in the database. This should be in the future b/c we're
#         -- going to compare to the check timestamp, which will be _now_
#         select 1 as id, 'pink' as color, (now() + interval '1 day')::date as updated_at
#         union all
#         select 2 as id, 'pink' as color, (now() + interval '1 day')::date as updated_at

#     {% endif %}

# {% endsnapshot %}
# """

# snapshots_checkall__snapshot_sql = """
# {% snapshot my_snapshot %}
#     {{ config(check_cols='all', unique_key='id', strategy='check', target_database=database, target_schema=schema) }}
#     select * from {{ ref(var('seed_name', 'seed')) }}
# {% endsnapshot %}
# """

snapshots_pg_custom_namespaced__snapshot_sql = """
{% snapshot snapshot_actual %}

    {{
        config(
            target_database=var('target_database', database),
            target_schema=var('target_schema', schema),
            unique_key='id || ' ~ "'-'" ~ ' || first_name',
            strategy='test.custom',
            updated_at='updated_at',
        )
    }}
    select * from {{target.database}}.{{target.schema}}.seed

{% endsnapshot %}
"""

snapshots_pg__snapshot_sql = """
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


# models_slow__gen_sql = """

# {{ config(materialized='ephemeral') }}


# /*
#     Generates 50 rows that "appear" to update every
#     second to a query-er.

#     1	2020-04-21 20:44:00-04	0
#     2	2020-04-21 20:43:59-04	59
#     3	2020-04-21 20:43:58-04	58
#     4	2020-04-21 20:43:57-04	57

#     .... 1 second later ....

#     1	2020-04-21 20:44:01-04	1
#     2	2020-04-21 20:44:00-04	0
#     3	2020-04-21 20:43:59-04	59
#     4	2020-04-21 20:43:58-04	58

#     This view uses pg_sleep(2) to make queries against
#     the view take a non-trivial amount of time

#     Use statement_timestamp() as it changes during a transactions.
#     If we used now() or current_time or similar, then the timestamp
#     of the start of the transaction would be returned instead.
# */

# with gen as (

#     select
#         id,
#         date_trunc('second', statement_timestamp()) - (interval '1 second' * id) as updated_at

#     from generate_series(1, 10) id

# )

# select
#     id,
#     updated_at,
#     extract(seconds from updated_at)::int as seconds

# from gen, pg_sleep(2)
# """

# snapshots_longtext__snapshot_sql = """
# {% snapshot snapshot_actual %}
#     {{
#         config(
#             target_database=var('target_database', database),
#             target_schema=schema,
#             unique_key='id',
#             strategy='timestamp',
#             updated_at='updated_at',
#         )
#     }}
#     select * from {{target.database}}.{{schema}}.super_long
# {% endsnapshot %}
# """

# snapshots_check_col_noconfig__snapshot_sql = """
# {% snapshot snapshot_actual %}
#     select * from {{target.database}}.{{schema}}.seed
# {% endsnapshot %}

# {# This should be exactly the same #}
# {% snapshot snapshot_checkall %}
# 	{{ config(check_cols='all') }}
#     select * from {{target.database}}.{{schema}}.seed
# {% endsnapshot %}
# """


@pytest.fixture
def snapshots_select():
    return {
        "snapshot.sql": snapshots_pg__snapshot_sql,
        "snapshot_select.sql": snapshots_select__snapshot_sql,
    }


@pytest.fixture
def snapshots_pg_custom():
    return {"snapshot.sql": snapshots_pg_custom__snapshot_sql}


# @pytest.fixture
# def models_collision():
#     return {"snapshot_actual.sql": models_collision__snapshot_actual_sql}


@pytest.fixture
def macros_custom_snapshot():
    return {
        "test_no_overlaps.sql": macros__test_no_overlaps_sql,
        "custom.sql": macros_custom_snapshot__custom_sql,
    }


# @pytest.fixture
# def test_snapshots_slow():
#     return {"test_timestamps.sql": test_snapshots_slow__test_timestamps_sql}


# @pytest.fixture
# def snapshots_check_col():
#     return {"snapshot.sql": snapshots_check_col__snapshot_sql}


# @pytest.fixture
# def snapshots_invalid():
#     return {"snapshot.sql": snapshots_invalid__snapshot_sql}


@pytest.fixture
def models():
    return {
        "schema.yml": models__schema_yml,
        "ref_snapshot.sql": models__ref_snapshot_sql,
    }


@pytest.fixture
def macros():
    return {"test_no_overlaps.sql": macros__test_no_overlaps_sql}


# @pytest.fixture
# def test_snapshots_changing_strategy():
#     return {"test_snapshot.sql": test_snapshots_changing_strategy__test_snapshot_sql}


# @pytest.fixture
# def snapshots_slow():
#     return {"snapshot.sql": snapshots_slow__snapshot_sql}


@pytest.fixture
def snapshots_select_noconfig():
    return {"snapshot.sql": snapshots_select_noconfig__snapshot_sql}


# @pytest.fixture
# def snapshots_pg_custom_invalid():
#     return {"snapshot.sql": snapshots_pg_custom_invalid__snapshot_sql}


@pytest.fixture
def seeds():
    return {"seed_newcol.csv": seeds__seed_newcol_csv, "seed.csv": seeds__seed_csv}


# @pytest.fixture
# def snapshots_changing_strategy():
#     return {"snapshot.sql": snapshots_changing_strategy__snapshot_sql}


# @pytest.fixture
# def snapshots_checkall():
#     return {"snapshot.sql": snapshots_checkall__snapshot_sql}


@pytest.fixture
def snapshots_pg_custom_namespaced():
    return {"snapshot.sql": snapshots_pg_custom_namespaced__snapshot_sql}


@pytest.fixture
def snapshots_pg():
    return {"snapshot.sql": snapshots_pg__snapshot_sql}


# @pytest.fixture
# def models_slow():
#     return {"gen.sql": models_slow__gen_sql}


# @pytest.fixture
# def snapshots_longtext():
#     return {"snapshot.sql": snapshots_longtext__snapshot_sql}


# @pytest.fixture
# def snapshots_check_col_noconfig():
#     return {"snapshot.sql": snapshots_check_col_noconfig__snapshot_sql}


@pytest.fixture
def project_files(
    project_root,
    snapshots_select,
    snapshots_pg_custom,
    #     models_collision,
    macros_custom_snapshot,
    #     test_snapshots_slow,
    #     snapshots_check_col,
    #     snapshots_invalid,
    models,
    macros,
    #     test_snapshots_changing_strategy,
    #     snapshots_slow,
    snapshots_select_noconfig,
    #     snapshots_pg_custom_invalid,
    seeds,
    #     snapshots_changing_strategy,
    #     snapshots_checkall,
    snapshots_pg_custom_namespaced,
    snapshots_pg,
    #     models_slow,
    #     snapshots_longtext,
    #     snapshots_check_col_noconfig,
):
    write_project_files(project_root, "snapshots-select", snapshots_select)
    write_project_files(project_root, "snapshots-pg-custom", snapshots_pg_custom)
    #     write_project_files(project_root, "models-collision", models_collision)
    write_project_files(project_root, "macros-custom-snapshot", macros_custom_snapshot)
    #     write_project_files(project_root, "test-snapshots-slow", test_snapshots_slow)
    #     write_project_files(project_root, "snapshots-check-col", snapshots_check_col)
    #     write_project_files(project_root, "snapshots-invalid", snapshots_invalid)
    write_project_files(project_root, "models", models)
    write_project_files(project_root, "macros", macros)
    #     write_project_files(
    #         project_root,
    #         "test-snapshots-changing-strategy",
    #         test_snapshots_changing_strategy,
    #     )
    #     write_project_files(project_root, "snapshots-slow", snapshots_slow)
    write_project_files(project_root, "snapshots-select-noconfig", snapshots_select_noconfig)
    #     write_project_files(
    #         project_root, "snapshots-pg-custom-invalid", snapshots_pg_custom_invalid
    #     )
    write_project_files(project_root, "seeds", seeds)
    #     write_project_files(
    #         project_root, "snapshots-changing-strategy", snapshots_changing_strategy
    #     )
    #     write_project_files(project_root, "snapshots-checkall", snapshots_checkall)
    write_project_files(
        project_root, "snapshots-pg-custom-namespaced", snapshots_pg_custom_namespaced
    )
    write_project_files(project_root, "snapshots-pg", snapshots_pg)


#     write_project_files(project_root, "models-slow", models_slow)
#     write_project_files(project_root, "snapshots-longtext", snapshots_longtext)
#     write_project_files(
#         project_root, "snapshots-check-col-noconfig", snapshots_check_col_noconfig
#     )
