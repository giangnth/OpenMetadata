# StarRocks

In this section, we provide guides and references to use the StarRocks connector.

## Requirements
To extract metadata the user used in the connection needs to have access to the `INFORMATION_SCHEMA`. By default, a user can see only the rows in the `INFORMATION_SCHEMA` that correspond to objects for which the user has the proper access privileges.

```SQL
-- Create user. If <hostName> is ommited, defaults to '%'
-- More details https://dev.mysql.com/doc/refman/8.0/en/create-user.html
CREATE USER '<username>'[@'<hostName>'] IDENTIFIED BY '<password>';

-- Grant select on a database
GRANT SELECT ON world.* TO '<username>';

-- Grant select on a database
GRANT SELECT ON world.* TO '<username>';

-- Grant select on a specific object
GRANT SELECT ON world.hello TO '<username>';
```

### Profiler & Data Quality

Executing the profiler Workflow or data quality tests, will require the user to have `SELECT` permission on the tables/schemas where the profiler/tests will be executed. The user should also be allowed to view information in `tables` for all objects in the database. More information on the profiler workflow setup can be found [here](https://docs.open-metadata.org/connectors/ingestion/workflows/profiler) and data quality tests [here](https://docs.open-metadata.org/connectors/ingestion/workflows/data-quality).

### Usage & Lineage

For the Usage and Lineage workflows, the user will need `SELECT` privilege. You can find more information on the usage workflow [here](https://docs.open-metadata.org/connectors/ingestion/workflows/usage) and the lineage workflow [here](https://docs.open-metadata.org/connectors/ingestion/workflows/lineage).

You can find further information on the StarRocks connector in the [docs](https://docs.open-metadata.org/connectors/database/starrocks).

## Connection Details

$$section
### Username $(id="username")

Username to connect to StarRocks/Mysql. This user should have privileges to read all the metadata in StarRocks.
$$

$$section
### Password $(id="password")

Password to connect to StarRocks/Mysql.
$$

$$section
### Host Port $(id="hostPort")

This parameter specifies the host and port of the StarRocks instance. This should be specified as a string in the format `hostname:port`. For example, you might set the hostPort parameter to `localhost:9030`.

If you are running the OpenMetadata ingestion in a docker and your services are hosted on the `localhost`, then use `host.docker.internal:9030` as the value.
$$

$$section
### Database Schema $(id="databaseSchema")

Schema of the data source. This is an optional parameter, if you would like to restrict the metadata reading to a single schema. When left blank, OpenMetadata Ingestion attempts to scan all the schemas.
$$

$$section
### Connection Options $(id="connectionOptions")

Enter the details for any additional connection options that can be sent to StarRocks during the connection. These details must be added as Key-Value pairs.
$$

$$section
### Connection Arguments $(id="connectionArguments")

Enter the details for any additional connection arguments such as security or protocol configs that can be sent to StarRocks during the connection. These details must be added as Key-Value pairs.

In case you are using Single-Sign-On (SSO) for authentication, add the `authenticator` details in the Connection Arguments as a Key-Value pair as follows: `"authenticator" : "sso_login_url"`

$$
