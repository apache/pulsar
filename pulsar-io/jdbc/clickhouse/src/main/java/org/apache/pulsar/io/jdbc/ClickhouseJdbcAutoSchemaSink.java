package org.apache.pulsar.io.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

/**
 * @Author hdx
 * @Date 2019/12/12
 * @Version 1.0
 */
public class ClickhouseJdbcAutoSchemaSink extends BaseJdbcAutoSchemaSink {

    @Override
    public Connection getConnection(JdbcSinkConfig jdbcSinkConfig, Properties properties) throws Exception {
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        return DriverManager.getConnection(jdbcSinkConfig.getJdbcUrl(), properties);
    }
}
