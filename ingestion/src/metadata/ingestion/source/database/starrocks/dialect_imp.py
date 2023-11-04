from starrocks.sqlalchemy.dialect import StarRocksDialect
from sqlalchemy.engine import Connection
from sqlalchemy import exc
import logging
import re
from typing import Any, Dict, List, Optional, Tuple
from sqlalchemy.sql import sqltypes
from metadata.ingestion.source.database.starrocks import datatype

logger = logging.getLogger(__name__)


class DialectImp(StarRocksDialect):

    def get_type_name_and_opts(self, type_str: str) -> Tuple[str, Optional[str]]:
        match = re.match(r"^(?P<type>\w+)\s*(?:(?:\(|<)(?P<options>.*)(?:\)|>))?", type_str)
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

        if type_opts:
            if type_opts.startswith(datatype.STRUCT.__visit_name__.lower()):
                final += self.parse_row_data_type(type_opts)
            elif type_opts.startswith(sqltypes.ARRAY.__visit_name__.lower()):
                final += self.parse_array_data_type(type_opts)
            else:
                final += type_opts
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
            for data_type in datatype.aware_split(type_opts) or []:
                attr_name, attr_type_str = datatype.aware_split(
                    data_type.strip(), delimiter=" ", maxsplit=1
                )
                if attr_type_str.startswith(datatype.STRUCT.__visit_name__.lower()):
                    final += attr_name + ":" + self.parse_row_data_type(attr_type_str) + ","
                elif attr_type_str.startswith(sqltypes.ARRAY.__visit_name__.lower()):
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
                dt = datatype.parse_sqltype(record.Type)
            except Exception as ex:
                logger.error(f"get_columns: {ex}")
                dt = sqltypes.NULLTYPE
            column = {
                "name": record.Field,
                "type": dt,
                "nullable": record.Null == "YES",
                # "comment": record.Comment,
                "system_data_type": record.Type,
                "default": record.Default,
            }
            type_str = record.Type.strip().lower()

            if type_str.startswith(datatype.STRUCT.__visit_name__.lower()):
                column["system_data_type"] = type_str.replace(", ", ",").replace(" ", ":")
                column["is_complex"] = True
            elif type_str.startswith(sqltypes.ARRAY.__visit_name__.lower()):
                column["system_data_type"] = type_str
                column["is_complex"] = True
            columns.append(column)
        return columns
