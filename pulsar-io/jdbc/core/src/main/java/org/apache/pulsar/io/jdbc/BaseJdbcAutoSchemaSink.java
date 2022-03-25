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

import com.google.common.collect.Lists;
import java.sql.PreparedStatement;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.jdbc.JdbcUtils.ColumnId;

/**
 * An abstract Jdbc sink, which interprets input Record in generic record.
 */
@Slf4j
public abstract class BaseJdbcAutoSchemaSink extends JdbcAbstractSink<GenericRecord> {

    @Override
    public void bindValue(PreparedStatement statement,
                          Record<GenericRecord> message, String action) throws Exception {

        GenericRecord record = message.getValue();
        List<ColumnId> columns = Lists.newArrayList();
        if (action == null || action.equals(INSERT)) {
            columns = tableDefinition.getColumns();
        } else if (action.equals(DELETE)){
            columns.addAll(tableDefinition.getKeyColumns());
        } else if (action.equals(UPDATE)){
            columns.addAll(tableDefinition.getNonKeyColumns());
            columns.addAll(tableDefinition.getKeyColumns());
        }

        int index = 1;
        for (ColumnId columnId : columns) {
            String colName = columnId.getName();
            int colType = columnId.getType();
            if (log.isDebugEnabled()) {
                log.debug("colName: {} colType: {}", colName, colType);
            }
            try {
                Object obj = record.getField(colName);
                if (obj != null) {
                    setColumnValue(statement, index++, obj);
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("Column {} is null", colName);
                    }
                    setColumnNull(statement, index++, colType);
                }
            } catch (NullPointerException e) {
                // With JSON schema field is omitted, so get NPE
                // In this case we want to set column to Null
                if (log.isDebugEnabled()) {
                    log.debug("Column {} is null", colName);
                }
                setColumnNull(statement, index++, colType);
            }

        }
    }

    private static void setColumnNull(PreparedStatement statement, int index, int type) throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("Setting column value to null, statement: {}, index: {}", statement.toString(), index);
        }
        statement.setNull(index, type);

    }

    private static void setColumnValue(PreparedStatement statement, int index, Object value) throws Exception {

        log.debug("Setting column value, statement: {}, index: {}, value: {}", statement, index, value);

        if (value instanceof Integer) {
            statement.setInt(index, (Integer) value);
        } else if (value instanceof Long) {
            statement.setLong(index, (Long) value);
        } else if (value instanceof Double) {
            statement.setDouble(index, (Double) value);
        } else if (value instanceof Float) {
            statement.setFloat(index, (Float) value);
        } else if (value instanceof Boolean) {
            statement.setBoolean(index, (Boolean) value);
        } else if (value instanceof String) {
            statement.setString(index, (String) value);
        } else if (value instanceof Short) {
            statement.setShort(index, (Short) value);
        } else {
            throw new Exception("Not support value type, need to add it. " + value.getClass());
        }
    }
}

