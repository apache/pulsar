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
package org.apache.pulsar.io.cassandra.util;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;
import com.google.common.collect.Lists;
import java.util.List;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

public class TableMetadataProvider {

    @Data(staticConstructor = "of")
    public static class TableId {
        private final String keyspaceName;
        private final String columnFamily;
    }

    @Data(staticConstructor = "of")
    public static class ColumnId {
        private final TableId tableId;
        private final String name;
        private final DataType type;
    }

    @Setter
    @Getter
    @EqualsAndHashCode
    @ToString
    public static class TableDefinition {
        private final TableId tableId;
        private final List<ColumnId> columns;
        private final List<ColumnId> partitionKeyColumns;
        private final List<ColumnId> primaryKeyColumns;

        private TableDefinition(TableId tableId, List<ColumnId> columns) {
            this(tableId, columns, null, null);
        }
        private TableDefinition(TableId tableId, List<ColumnId> columns,
                                List<ColumnId> partitionKeyColumns, List<ColumnId> primaryKeyColumns) {
            this.tableId = tableId;
            this.columns = columns;
            this.partitionKeyColumns = partitionKeyColumns;
            this.primaryKeyColumns = primaryKeyColumns;
        }

        public static TableDefinition of(TableId tableId, List<ColumnId> columns) {
            return new TableDefinition(tableId, columns);
        }

        public static TableDefinition of(TableId tableId, List<ColumnId> columns,
                                         List<ColumnId> nonKeyColumns, List<ColumnId> keyColumns) {
            return new TableDefinition(tableId, columns, nonKeyColumns, keyColumns);
        }

    }

    public static TableDefinition getTableDefinition(Metadata clusterMetadata, String keyspace, String columnFamily) {

        TableId tableId = TableId.of(keyspace, columnFamily);
        TableDefinition table = TableDefinition.of(tableId,
                Lists.newArrayList(), Lists.newArrayList(), Lists.newArrayList());

        TableMetadata meta = clusterMetadata
                .getKeyspace(keyspace)
                .getTable(columnFamily);

        for (ColumnMetadata col : meta.getPrimaryKey()) {
            ColumnId columnId = ColumnId.of(tableId, col.getName(), col.getType());
            table.getPrimaryKeyColumns().add(columnId);
        }

        for (ColumnMetadata col : meta.getPartitionKey()) {
            ColumnId columnId = ColumnId.of(tableId, col.getName(), col.getType());
            table.getPartitionKeyColumns().add(columnId);
        }

        for (ColumnMetadata col : meta.getColumns()) {
            ColumnId columnId = ColumnId.of(tableId, col.getName(), col.getType());
            table.getColumns().add(columnId);
        }

        return table;
    }
}
