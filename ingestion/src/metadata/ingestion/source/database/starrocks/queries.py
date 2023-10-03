STARROCKS_SQL_GET_SCHEMA = """
        select SCHEMA_NAME from information_schema.schemata where SCHEMA_NAME not in ('sys', '_statistics_', 'information_schema');
"""

STARROCKS_SQL_GET_TABLES = """
select TABLE_NAME from information_schema.tables where TABLE_SCHEMA not in ('sys', '_statistics_', 'information_schema') and TABLE_TYPE='BASE TABLE';
"""

STARROCKS_SQL_STATEMENT_TEST = """
        select * from information_schema.tables limit 10;
"""

STARROCKS_SQL_GET_VIEW = """
select VIEW_DEFINITION view_def, DEFINER as view_name,  TABLE_SCHEMA as 'schema' from information_schema.views;
"""