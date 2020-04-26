package org.apache.pulsar.io.jdbc;

import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

@Connector(
    name = "jdbc-mysql",
    type = IOType.SINK,
    help = "A simple JDBC sink for MySQL that writes pulsar messages to a database table",
    configClass = JdbcSinkConfig.class
)
public class MysqlJdbcAutoSchemaSink extends BaseJdbcAutoSchemaSink {

}
