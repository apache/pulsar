/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pulsar.io.jdbc;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.Lists;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.stream.IntStream;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * Jdbc Utils
 */
@Slf4j
public class JdbcUtils {

    @Data(staticConstructor = "of")
    @Setter
    @Getter
    @EqualsAndHashCode
    @ToString
    public static class TableId {
        private final String catalogName;
        private final String schemaName;
        private final String tableName;
    }

    @Data(staticConstructor = "of")
    @Setter
    @Getter
    @EqualsAndHashCode
    @ToString
    public static class ColumnId {
        private final TableId tableId;
        private final String name;
        // SQL type from java.sql.Types
        private final int type;
        private final String typeName;
        // column position in table
        private final int position;
    }

    @Data(staticConstructor = "of")
    @Setter
    @Getter
    @EqualsAndHashCode
    @ToString
    public static class TableDefinition {
        private final TableId tableId;
        private final List<ColumnId> columns;
    }

    /**
     * Given a driver type(such as mysql), return its jdbc driver class name.
     * TODO: test and support more types, also add Driver in pom file.
     */
    public static String getDriverClassName(String driver) throws Exception {
        if (driver.equals("mysql")) {
            return "com.mysql.jdbc.Driver";
        } if (driver.equals("sqlite")) {
            return "org.sqlite.JDBC";
        } else {
            throw new Exception("Not tested jdbc driver type: " + driver);
        }
    }

    /**
     * Get the {@link Connection} for the given jdbcUrl.
     */
    public static Connection getConnection(String jdbcUrl, Properties properties) throws Exception {
        String driver = jdbcUrl.split(":")[1];
        String driverClassName = getDriverClassName(driver);
        Class.forName(driverClassName);

        return DriverManager.getConnection(jdbcUrl, properties);
    }

    /**
     * Get the {@link TableId} for the given tableName.
     */
    public static TableId getTableId(Connection connection, String tableName) throws Exception {
        DatabaseMetaData metadata = connection.getMetaData();
        try (ResultSet rs = metadata.getTables(null, null, tableName, new String[]{"TABLE"})) {
            if (rs.next()) {
                String catalogName = rs.getString(1);
                String schemaName = rs.getString(2);
                String gotTableName = rs.getString(3);
                checkState(tableName.equals(gotTableName),
                    "TableName not match: " + tableName + " Got: " + gotTableName);
                if (log.isDebugEnabled()) {
                    log.debug("Get Table: {}, {}, {}", catalogName, schemaName, tableName);
                }
                return TableId.of(catalogName, schemaName, tableName);
            } else {
                throw new Exception("Not able to find table: " + tableName);
            }
        }
    }

    /**
     * Get the {@link TableDefinition} for the given table.
     */
    public static TableDefinition getTableDefinition(Connection connection, TableId tableId) throws Exception {
        TableDefinition table = TableDefinition.of(tableId, Lists.newArrayList());

        try (ResultSet rs = connection.getMetaData().getColumns(
            tableId.getCatalogName(),
            tableId.getSchemaName(),
            tableId.getTableName(),
            null
        )) {
            while (rs.next()) {
                final String columnName = rs.getString(4);

                final int sqlDataType = rs.getInt(5);
                final String typeName = rs.getString(6);
                final int position = rs.getInt(17);

                table.columns.add(ColumnId.of(tableId, columnName, sqlDataType, typeName, position));
                if (log.isDebugEnabled()) {
                    log.debug("Get column. name: {}, data type: {}, position: {}", columnName, typeName, position);
                }
            }
            return table;
        }
    }

    public static String buildInsertSql(TableDefinition table) {
        StringBuilder builder = new StringBuilder();
        builder.append("INSERT INTO ");
        builder.append(table.tableId.getTableName());
        builder.append("(");

        table.columns.forEach(columnId -> builder.append(columnId.getName()).append(","));
        builder.deleteCharAt(builder.length() - 1);

        builder.append(") VALUES(");
        IntStream.range(0, table.columns.size() - 1).forEach(i -> builder.append("?,"));
        builder.append("?)");

        return builder.toString();
    }

    public static PreparedStatement buildInsertStatement(Connection connection, String insertSQL) throws SQLException {
        return connection.prepareStatement(insertSQL);
    }

}
