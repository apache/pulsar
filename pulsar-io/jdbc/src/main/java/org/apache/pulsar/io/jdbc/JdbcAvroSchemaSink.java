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

import java.sql.PreparedStatement;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.jdbc.JdbcUtils.ColumnId;

/**
 * A Simple Jdbc sink, which assume input Record as AvroSchema format
 */
@Slf4j
public class JdbcAvroSchemaSink extends JdbcAbstractSink<byte[]> {

    private Schema avroSchema = null;
    private DatumReader<GenericRecord> reader = null;


    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        super.open(config, sinkContext);
        // get reader, and read value out as GenericRecord
        if (avroSchema == null || reader == null) {
            avroSchema = Schema.parse(schema);
            reader = new GenericDatumReader<>(avroSchema);
        }
        log.info("open JdbcAvroSchemaSink with schema: {}, and tableDefinition: {}", schema, tableDefinition.toString());
    }


    public void bindValue(PreparedStatement statement,
                          Record<byte[]> message) throws Exception {

        byte[] value = message.getValue();
        GenericRecord record = reader.read(null, DecoderFactory.get().binaryDecoder(value, null));

        int index = 1;
        for (ColumnId columnId : tableDefinition.getColumns()) {
            String colName = columnId.getName();
            Object obj = record.get(colName);
            setColumnValue(statement, index++, obj);
            log.info("set column value: {}", obj.toString());
        }
    }

    private static void setColumnValue(PreparedStatement statement, int index, Object value) throws Exception {
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
        } else if (value instanceof Utf8) {
            statement.setString(index, ((Utf8)value).toString());
        } else if (value instanceof Short) {
            statement.setShort(index, (Short) value);
        } else {
            throw new Exception("Not support value type, need to add it. " + value.getClass());
        }
    }
}

