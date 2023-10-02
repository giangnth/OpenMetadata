from starrocks.sqlalchemy.dialect import StarRocksDialect
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
                f"Expected DatabricksConnection, but got {connection}"
            )
        return cls(config, metadata_config)

    # def prepare(self):
    #     StarRocksDialect.get_table_names = get_table_names
    #     StarRocksDialect.get_view_names = get_view_names
    #     StarRocksDialect.get_table_comment = get_table_comment
    #     StarRocksDialect.get_columns = get_columns
    #     StarRocksDialect.get_view_definition = get_view_definition
