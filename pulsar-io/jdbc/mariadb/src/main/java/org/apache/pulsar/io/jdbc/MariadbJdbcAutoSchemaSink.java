/*
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

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.sql.Timestamp;
import java.sql.Date;
import java.sql.Time;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

@Connector(
        name = "jdbc-mariadb",
        type = IOType.SINK,
        help = "A simple JDBC sink for MariaDB that writes pulsar messages to a database table",
        configClass = JdbcSinkConfig.class
)
public class MariadbJdbcAutoSchemaSink extends BaseJdbcAutoSchemaSink {

    @Override
    public String generateUpsertQueryStatement() {
        return JdbcUtils.buildInsertSql(tableDefinition)
                + "ON DUPLICATE KEY UPDATE " + JdbcUtils.buildUpdateSqlSetPart(tableDefinition);
    }

    @Override
    public List<JdbcUtils.ColumnId> getColumnsForUpsert() {
        final List<JdbcUtils.ColumnId> columns = new ArrayList<>();
        columns.addAll(tableDefinition.getColumns());
        columns.addAll(tableDefinition.getNonKeyColumns());
        return columns;
    }

    /**
     * MariaDB does not support native array types in the same way as PostgreSQL.
     * <p>
     * While MariaDB supports JSON data types that can store arrays, it does not have
     * native array types with the same semantics as PostgreSQL. This implementation
     * does not provide automatic array conversion to avoid confusion and maintain
     * data type consistency.
     * </p>
     * <p>
     * <strong>Alternatives:</strong>
     * <ul>
     * <li>Use PostgreSQL JDBC sink for native array support</li>
     * <li>Serialize arrays to JSON and use MariaDB's JSON column type</li>
     * <li>Use separate tables with foreign keys for one-to-many relationships</li>
     * <li>Store arrays as delimited strings (with appropriate parsing logic)</li>
     * </ul>
     * </p>
     *
     * @param statement     the PreparedStatement (not used)
     * @param index         the parameter index (not used)
     * @param arrayValue    the array value (not used)
     * @param targetSqlType the target SQL type (not used)
     * @throws UnsupportedOperationException always thrown as MariaDB array support is not implemented
     */
    @Override
    protected void handleArrayValue(PreparedStatement statement, int index, Object arrayValue, String targetSqlType)
            throws Exception {
        throw new UnsupportedOperationException("Array types are not supported by MariaDB JDBC sink. "
                + "Consider using PostgreSQL JDBC sink for array support.");
    }

    /**
     * MariaDB supports Date, Timestamp, and Time types.
     * <p>
     * Inputs are converted to the appropriate datetime type.
     * </p>
     * <p>
     * <strong>Implementation Guidelines:</strong>
     * <ul>
     * <li>If the value is a Timestamp, it is converted to a Timestamp</li>
     * <li>If the value is a Date, it is converted to a Date</li>
     * <li>If the value is a Time, it is converted to a Time</li>
     * </ul>
     * </p>
     *
     * @param statement     the PreparedStatement (not used)
     * @param index         the parameter index (not used)
     * @param value         the value (not used)
     * @param targetSqlType the target SQL type (not used)
     * @return false as SQLite doesn't support datetime
     * @throws Exception if conversion or binding fails
     */
    @Override
    protected boolean handleDateTime(PreparedStatement statement, int index, Object value, String targetSqlType)
            throws Exception {
        switch (targetSqlType) {
            case "Timestamp":
                statement.setTimestamp(index, (Timestamp) value);
                return true;
            case "Date":
                statement.setDate(index, (Date) value);
                return true;
            case "Time":
                statement.setTime(index, (Time) value);
                return true;
            default:
                return false;
        }
    }
}
