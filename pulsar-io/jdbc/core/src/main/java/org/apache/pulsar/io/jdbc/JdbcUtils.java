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

import static com.google.common.base.Preconditions.checkState;
import com.google.common.collect.Lists;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.IntStream;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcUtils {

    @Data(staticConstructor = "of")
    public static class TableId {
        private final String catalogName;
        private final String schemaName;
        private final String tableName;
    }

    @Data(staticConstructor = "of")
    public static class ColumnId {
        private final TableId tableId;
        private final String name;
        // SQL type from java.sql.Types
        private final int type;
        private final String typeName;
        // column position in table
        private final int position;
    }

    @Setter
    @Getter
    @EqualsAndHashCode
    @ToString
    public static class TableDefinition {
        private final TableId tableId;
        private final List<ColumnId> columns;
        private final List<ColumnId> nonKeyColumns;
        private final List<ColumnId> keyColumns;

        private TableDefinition(TableId tableId, List<ColumnId> columns) {
            this(tableId, columns, null, null);
        }

        private TableDefinition(
                TableId tableId,
                List<ColumnId> columns,
                List<ColumnId> nonKeyColumns,
                List<ColumnId> keyColumns
        ) {
            this.tableId = tableId;
            this.columns = columns;
            this.nonKeyColumns = nonKeyColumns;
            this.keyColumns = keyColumns;
        }

        public static TableDefinition of(TableId tableId, List<ColumnId> columns) {
            return new TableDefinition(tableId, columns);
        }

        public static TableDefinition of(TableId tableId, List<ColumnId> columns,
                                         List<ColumnId> nonKeyColumns, List<ColumnId> keyColumns) {
            return new TableDefinition(tableId, columns, nonKeyColumns, keyColumns);
        }

    }

    /**
     * Get the {@link TableId} for the given tableName.
     */
    public static TableId getTableId(Connection connection, String tableName) throws Exception {
        DatabaseMetaData metadata = connection.getMetaData();
        try (ResultSet rs = metadata.getTables(null, null, tableName, new String[]{"TABLE", "PARTITIONED TABLE"})) {
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
    public static TableDefinition getTableDefinition(
            Connection connection,
            TableId tableId,
            List<String> keyList,
            List<String> nonKeyList,
            boolean excludeNonDeclaredFields
    ) throws Exception {
        TableDefinition table = TableDefinition.of(
                tableId, Lists.newArrayList(), Lists.newArrayList(), Lists.newArrayList());

        keyList = keyList == null ? Collections.emptyList() : keyList;
        nonKeyList = nonKeyList == null ? Collections.emptyList() : nonKeyList;

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

                if (log.isDebugEnabled()) {
                    log.debug("Get column. name: {}, data type: {}, position: {}", columnName, typeName, position);
                }

                ColumnId columnId = ColumnId.of(tableId, columnName, sqlDataType, typeName, position);

                if (keyList.contains(columnName)) {
                    table.keyColumns.add(columnId);
                    table.columns.add(columnId);
                } else if (nonKeyList.contains(columnName)) {
                    table.nonKeyColumns.add(columnId);
                    table.columns.add(columnId);
                } else if (!excludeNonDeclaredFields) {
                    table.columns.add(columnId);
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

    public static String combationWhere(List<ColumnId> columnIds) {
        StringBuilder builder = new StringBuilder();
        if (!columnIds.isEmpty()) {
            builder.append(" WHERE ");
            StringJoiner whereJoiner = new StringJoiner(" AND ");
            columnIds.forEach((columnId -> {
                StringJoiner equals = new StringJoiner("=");
                equals.add(columnId.getName()).add("?");
                whereJoiner.add(equals.toString());
            }));
            builder.append(whereJoiner.toString());
            return builder.toString();
        }
        return "";
    }

    public static String buildUpdateSql(TableDefinition table) {
        StringBuilder builder = new StringBuilder();
        builder.append("UPDATE ");
        builder.append(table.tableId.getTableName());
        builder.append(" SET ");
        StringJoiner setJoiner = buildUpdateSqlSetPart(table);
        builder.append(setJoiner);
        builder.append(combationWhere(table.keyColumns));
        return builder.toString();
    }

    public static StringJoiner buildUpdateSqlSetPart(TableDefinition table) {
        if (table.nonKeyColumns.isEmpty()) {
            throw new IllegalStateException("UPDATE operations are not supported if 'nonKey' config is not set.");
        }
        StringJoiner setJoiner = new StringJoiner(",");

        table.nonKeyColumns.forEach((columnId) -> {
            StringJoiner equals = new StringJoiner("=");
            equals.add(columnId.getName()).add("? ");
            setJoiner.add(equals.toString());
        });
        return setJoiner;
    }

    public static String buildDeleteSql(TableDefinition table) {
        return "DELETE FROM " + table.tableId.getTableName() + combationWhere(table.keyColumns);
    }
}
