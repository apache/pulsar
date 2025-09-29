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

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

@Connector(
        name = "jdbc-clickhouse",
        type = IOType.SINK,
        help = "A simple JDBC sink for ClickHouse that writes pulsar messages to a database table",
        configClass = JdbcSinkConfig.class
)
public class ClickHouseJdbcAutoSchemaSink extends BaseJdbcAutoSchemaSink {

    /**
     * ClickHouse array support is not currently implemented.
     * <p>
     * While ClickHouse has native support for array types (e.g., Array(Int32), Array(String)),
     * the automatic conversion from Avro arrays to ClickHouse arrays is not yet implemented
     * in this JDBC sink. ClickHouse arrays have specific syntax and behavior that would
     * require dedicated implementation.
     * </p>
     * <p>
     * <strong>Alternatives:</strong>
     * <ul>
     * <li>Use PostgreSQL JDBC sink for native array support</li>
     * <li>Implement custom ClickHouse array conversion logic</li>
     * <li>Serialize arrays to JSON strings for storage in String columns</li>
     * <li>Use separate tables for one-to-many relationships</li>
     * </ul>
     * </p>
     * <p>
     * <strong>Future Enhancement:</strong>
     * This method could be enhanced to support ClickHouse-specific array conversion
     * using the ClickHouse JDBC driver's array handling capabilities.
     * </p>
     *
     * @param statement     the PreparedStatement (not used)
     * @param index         the parameter index (not used)
     * @param arrayValue    the array value (not used)
     * @param targetSqlType the target SQL type (not used)
     * @throws UnsupportedOperationException always thrown as ClickHouse array support is not implemented
     */
    @Override
    protected void handleArrayValue(PreparedStatement statement, int index, Object arrayValue, String targetSqlType)
            throws Exception {
        throw new UnsupportedOperationException("Array types are not supported by ClickHouse JDBC sink. "
                + "Consider using PostgreSQL JDBC sink for array support.");
    }

    /**
     * ClickHouse supports various Date types.
     * <p>
     * Inputs are converted to the appropriate Date type.
     * </p>
     * <p>
     * <strong>Implementation Guidelines:</strong>
     * <ul>
     * <li>If the value is a Timestamp, it is converted to a Timestamp</li>
     * <li>If the value is a Date, it is converted to a Date</li>
     * <li>If the value is a Time, it not supported</li>
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
        if (targetSqlType == null) {
            return false;
        }
        switch (targetSqlType) {
            case "Timestamp":
                statement.setTimestamp(index, (Timestamp) value);
                return true;
            case "Date":
                statement.setDate(index, (Date) value);
                return true;
            case "Time":
                return false;
            default:
                return false;
        }
    }
}
