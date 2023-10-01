import traceback
from typing import Optional

from sqlalchemy.engine import Inspector
from starrocks.sqlalchemy.dialect import StarRocksDialect

from metadata.generated.schema.entity.data.table import TableType
from metadata.generated.schema.entity.services.connections.database.starrocksConnection import (
    StarrocksConnection,
)
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    OpenMetadataConnection,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.ingestion.api.steps import InvalidSourceException
from metadata.ingestion.source.database.common_db_source import CommonDbSourceService
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()


StarRocksDialect


class StarrocksSource(CommonDbSourceService):
    """
    Implements the necessary methods to extract
    Database metadata from StarRocks Source
    """

    @classmethod
    def create(cls, config_dict, metadata_config: OpenMetadataConnection):
        config: WorkflowSource = WorkflowSource.parse_obj(config_dict)
        connection: StarrocksConnection = config.serviceConnection.__root__.config
        if not isinstance(connection, StarrocksConnection):
            raise InvalidSourceException(
                f"Expected StarrocksConnection, but got {connection}"
            )
        return cls(config, metadata_config)

    def get_view_definition(
        self, table_type: str, table_name: str, schema_name: str, inspector: Inspector
    ) -> Optional[str]:
        if table_type in {TableType.View, TableType.MaterializedView}:
            definition_fn = inspector.get_view_definition
            try:
                view_definition = definition_fn(table_name, schema_name)
                view_definition = (
                    "" if view_definition is None else str(view_definition)
                )
                return view_definition

            except NotImplementedError:
                logger.warning("View definition not implemented")

            except Exception as exc:
                logger.debug(traceback.format_exc())
                logger.warning(
                    f"Failed to fetch view definition for {table_name}: {exc}"
                )
            return None
        return None
