package org.apache.pulsar.io.jdbc;

import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

@Connector(
    name = "jdbc-postgres",
    type = IOType.SINK,
    help = "A simple JDBC sink for PostgreSQL that writes pulsar messages to a database table",
    configClass = JdbcSinkConfig.class
)
public class PostgresqlJdbcAutoSchemaSink extends BaseJdbcAutoSchemaSink {

}
