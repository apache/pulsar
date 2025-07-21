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
import java.util.stream.Collectors;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

@Connector(
        name = "jdbc-sqlite",
        type = IOType.SINK,
        help = "A simple JDBC sink for SQLite that writes pulsar messages to a database table",
        configClass = JdbcSinkConfig.class
)
public class SqliteJdbcAutoSchemaSink extends BaseJdbcAutoSchemaSink {

    @Override
    public String generateUpsertQueryStatement() {
        final List<JdbcUtils.ColumnId> keyColumns = tableDefinition.getKeyColumns();
        if (keyColumns.isEmpty()) {
            throw new IllegalStateException("UPSERT is not supported if 'key' config is not set.");
        }
        final String keys = keyColumns.stream().map(JdbcUtils.ColumnId::getName)
                .collect(Collectors.joining(","));
        return JdbcUtils.buildInsertSql(tableDefinition)
                + " ON CONFLICT(" + keys + ") "
                + "DO UPDATE SET " + JdbcUtils.buildUpdateSqlSetPart(tableDefinition);
    }

    @Override
    public List<JdbcUtils.ColumnId> getColumnsForUpsert() {
        final List<JdbcUtils.ColumnId> columns = new ArrayList<>();
        columns.addAll(tableDefinition.getColumns());
        columns.addAll(tableDefinition.getNonKeyColumns());
        return columns;
    }

    /**
     * SQLite does not support native array types.
     * <p>
     * SQLite does not have native array data types like PostgreSQL. While it's possible
     * to store arrays as JSON or delimited strings, this implementation does not provide
     * automatic array conversion to maintain data type safety and consistency.
     * </p>
     * <p>
     * <strong>Alternatives:</strong>
     * <ul>
     * <li>Use PostgreSQL JDBC sink for native array support</li>
     * <li>Serialize arrays to JSON strings in your Pulsar producer</li>
     * <li>Use separate tables with foreign keys for one-to-many relationships</li>
     * </ul>
     * </p>
     *
     * @param statement     the PreparedStatement (not used)
     * @param index         the parameter index (not used)
     * @param arrayValue    the array value (not used)
     * @param targetSqlType the target SQL type (not used)
     * @throws UnsupportedOperationException always thrown as SQLite doesn't support arrays
     */
    @Override
    protected void handleArrayValue(PreparedStatement statement, int index, Object arrayValue, String targetSqlType)
            throws Exception {
        throw new UnsupportedOperationException("Array types are not supported by SQLite JDBC sink. "
                + "Consider using PostgreSQL JDBC sink for array support.");
    }
}
