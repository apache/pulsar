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
package org.apache.pulsar.io.hbase.sink;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

import java.util.ArrayList;
import java.util.List;

/**
 * A Simple hbase sink, which interprets input Record in generic record.
 */
@Connector(
    name = "hbase",
    type = IOType.SINK,
    help = "The HbaseGenericRecordSink is used for moving messages from Pulsar to Hbase.",
    configClass = HbaseSinkConfig.class
)
@Slf4j
public class HbaseGenericRecordSink extends HbaseAbstractSink<GenericRecord> {
    @Override
    public List<Put> bindValue(Record<GenericRecord> message) throws Exception {
        List<Put> result = new ArrayList<>();
        GenericRecord record = message.getValue();
        if (null == record){
            return null;
        }

        String rowKeyName = tableDefinition.getRowKeyName();
        Object rowKeyValue = record.getField(rowKeyName);

        // set familyName value from HbaseSinkConfig
        String familyName = tableDefinition.getFamilyName();
        byte[] familyValueBytes = getBytes(familyName);

        List<String> qualifierNames = tableDefinition.getQualifierNames();
        for (String qualifierName : qualifierNames) {
            Object qualifierValue = record.getField(qualifierName);
            if (null != qualifierValue) {
                Put put = new Put(getBytes(rowKeyValue));
                put.addColumn(familyValueBytes, getBytes(qualifierName), getBytes(qualifierValue));
                result.add(put);
            }
        }

        return result;
    }

    private byte[] getBytes(Object value) throws Exception{
        if (value instanceof Integer) {
            return Bytes.toBytes((Integer) value);
        } else if (value instanceof Long) {
            return Bytes.toBytes((Long) value);
        } else if (value instanceof Double) {
            return Bytes.toBytes((Double) value);
        } else if (value instanceof Float) {
            return Bytes.toBytes((Float) value);
        } else if (value instanceof Boolean) {
            return Bytes.toBytes((Boolean) value);
        } else if (value instanceof String) {
            return Bytes.toBytes((String) value);
        } else if (value instanceof Short) {
            return Bytes.toBytes((Short) value);
        } else {
            throw new Exception("Not support value type, need to add it. " + value.getClass());
        }
    }
}
