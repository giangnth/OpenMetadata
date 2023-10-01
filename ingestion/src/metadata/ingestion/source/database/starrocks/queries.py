import textwrap

CLICKHOUSE_VIEW_DEFINITIONS = textwrap.dedent(
    """
select TABLE_SCHEMA, TABLE_NAME from information_schema.views;
"""
)


CLICKHOUSE_TABLE_DEFINITIONS = textwrap.dedent(
    """
select TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, ENGINE from information_schema.tables where ENGINE in ['StarRocks'];
"""
)

MSSQL_GET_DATABASE = """
SELECT SCHEMA_NAME FROM information_schema.schemata order by SCHEMA_NAME;
"""

STARROCKS_SQL_STATEMENT_TEST = """
        select * from information_schema.tables limit 10;
"""
