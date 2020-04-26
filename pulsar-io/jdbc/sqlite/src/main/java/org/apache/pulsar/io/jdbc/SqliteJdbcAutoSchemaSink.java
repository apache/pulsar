package org.apache.pulsar.io.jdbc;

import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

@Connector(
    name = "jdbc-sqlite",
    type = IOType.SINK,
    help = "A simple JDBC sink for SQLite that writes pulsar messages to a database table",
    configClass = JdbcSinkConfig.class
)
public class SqliteJdbcAutoSchemaSink extends BaseJdbcAutoSchemaSink {

}
