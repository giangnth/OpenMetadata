from starrocks.sqlalchemy.dialect import StarRocksDialect
from sqlalchemy.engine import Connection
from sqlalchemy import exc
import logging
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql.type_api import TypeEngine
# from metadata.ingestion.source.database.starrocks.datatype import (
#     _type_map,
#     ARRAY,
#     MAP,
#     STRUCT,
# )
from metadata.ingestion.source.database.starrocks import trino_datatype


logger = logging.getLogger(__name__)


class DialectImp(StarRocksDialect):
    # def parse_sqltype(self, type_str: str) -> TypeEngine:
    #     type_str = type_str.strip().lower()
    #     match = re.match(r"^(?P<type>\w+)\s*(?:\((?P<options>.*)\))?", type_str)
    #     if not match:
    #         logger.warning(f"Could not parse type name '{type_str}'")
    #         return sqltypes.NULLTYPE
    #     type_name = match.group("type")
    #
    #     if type_name not in _type_map:
    #         logger.warning(f"Did not recognize type '{type_name}'")
    #         return sqltypes.NULLTYPE
    #     lst_complex_data_type = [
    #         ARRAY.__visit_name__.lower(),
    #         # MAP.__visit_name__.lower(),
    #         # STRUCT.__visit_name__.lower(),
    #     ]
    #     type_class = _type_map[type_name]
    #     if type_name in lst_complex_data_type:
    #         new_type_str = str(type_str).replace(type_name + "<", "", 1)[:-1]
    #         return type_class(self.parse_sqltype(new_type_str))
    #     else:
    #         return type_class()

    def get_type_name_and_opts(self, type_str: str) -> Tuple[str, Optional[str]]:
        match = re.match(r"^(?P<type>\w+)\s*(?:\((?P<options>.*)\))?", type_str)
        if not match:
            logger.warning(f"Could not parse type name '{type_str}'")
            return sqltypes.NULLTYPE
        type_name = match.group("type")
        type_opts = match.group("options")
        return type_name, type_opts

    def parse_array_data_type(self, type_str: str) -> str:
        """
        This mehtod is used to convert the complex array datatype to the format that is supported by OpenMetadata
        For Example:
        If we have a row type as array(row(col1 bigint, col2 string))
        this method will return type as -> array<struct<col1:bigint,col2:string>>
        """
        type_name, type_opts = self.get_type_name_and_opts(type_str)
        final = type_name + "<"
        new_type_str = str(type_str).replace(type_name + "<", "", 1)[:-1]
        if new_type_str:
            if new_type_str.startswith("struct"):
                final += self.parse_row_data_type(new_type_str)
            elif new_type_str.startswith("array"):
                final += self.parse_array_data_type(new_type_str)
            else:
                if type_opts:
                    final += type_opts
                else:
                    final += new_type_str
        return final + ">"

    def parse_row_data_type(self, type_str: str) -> str:
        """
        This mehtod is used to convert the complex row datatype to the format that is supported by OpenMetadata
        For Example:
        If we have a row type as row(col1 bigint, col2 bigint, col3 row(col4 string, col5 bigint))
        this method will return type as -> struct<col1:bigint,col2:bigint,col3:struct<col4:string,col5:bigint>>
        """
        type_name, type_opts = self.get_type_name_and_opts(type_str)
        final = type_name + "<"
        if type_opts:
            for data_type in trino_datatype.aware_split(type_opts) or []:
                attr_name, attr_type_str = trino_datatype.aware_split(
                    data_type.strip(), delimiter=" ", maxsplit=1
                )
                if attr_type_str.startswith("struct"):
                    final += attr_name + ":" + self.parse_row_data_type(attr_type_str) + ","
                elif attr_type_str.startswith("array"):
                    final += attr_name + ":" + self.parse_array_data_type(attr_type_str) + ","
                else:
                    final += attr_name + ":" + attr_type_str + ","
        return final[:-1] + ">"

    def get_columns(
        self, connection: Connection, table_name: str, schema: str = None, **kw
    ) -> List[Dict[str, Any]]:
        if not self.has_table(connection, table_name, schema):
            raise exc.NoSuchTableError(f"schema={schema}, table={table_name}")
        schema = schema or self._get_default_schema_name(connection)

        quote = self.identifier_preparer.quote_identifier
        full_name = quote(table_name)
        if schema:
            full_name = "{}.{}".format(quote(schema), full_name)

        res = connection.execute(f"SHOW COLUMNS FROM {full_name}")
        columns = []
        for record in res:
            try:
                datatype = trino_datatype.parse_sqltype(record.Type)
            except Exception as ex:
                logger.error(f"get_columns: {ex}")
                datatype = sqltypes.NULLTYPE
            # column = dict(
            #     name=record.Field,
            #     type=datatype,
            #     nullable=record.Null == "YES",
            #     default=record.Default,
            # )
            column = {
                "name": record.Field,
                "type": datatype,
                "nullable": record.Null == "YES",
                # "comment": record.Comment,
                "system_data_type": record.Type,
                "default": record.Default,
            }
            type_str = record.Type.strip().lower()

            if type_str.startswith("struct"):
                # column["system_data_type"] = self.parse_row_data_type(type_str)
                column["system_data_type"] = type_str.replace(", ", ",").replace(" ", ":")
                column["is_complex"] = True
            elif type_str.startswith("array"):
                # column["system_data_type"] = self.parse_array_data_type(type_str)
                column["system_data_type"] = type_str
                column["is_complex"] = True
            columns.append(column)
        return columns
