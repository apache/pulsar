package org.apache.pulsar.io.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

/**
 * @Author hdx
 * @Date 2019/12/12
 * @Version 1.0
 */
public class MariadbJdbcAutoSchemaSink extends BaseJdbcAutoSchemaSink {

    @Override
    public Connection getConnection(JdbcSinkConfig jdbcSinkConfig, Properties properties) throws Exception {
        Class.forName("org.mariadb.jdbc.Driver");
        return DriverManager.getConnection(jdbcSinkConfig.getJdbcUrl(), properties);
    }
}
