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
package org.apache.pulsar.functions.instance;

import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.client.impl.schema.KeyValueSchemaImpl;
import org.apache.pulsar.functions.api.KVRecord;
import org.apache.pulsar.functions.api.Record;

@Slf4j
@Data
@AllArgsConstructor
public class SinkRecord<T> implements Record<T> {

    private final Record<T> sourceRecord;
    private final T value;

    public Record<T> getSourceRecord() {
        return sourceRecord;
    }

    @Override
    public Optional<String> getTopicName() {
        return sourceRecord.getTopicName();
    }

    @Override
    public Optional<String> getKey() {
        return sourceRecord.getKey();
    }

    @Override
    public T getValue() {
        return value;
    }

    @Override
    public Optional<String> getPartitionId() {
        return sourceRecord.getPartitionId();
    }

    @Override
    public Optional<Integer> getPartitionIndex() {
        return sourceRecord.getPartitionIndex();
    }

    @Override
    public Optional<Long> getRecordSequence() {
        return sourceRecord.getRecordSequence();
    }

     @Override
    public Map<String, String> getProperties() {
        return sourceRecord.getProperties();
    }

    @Override
    public void ack() {
        sourceRecord.ack();
    }

    @Override
    public void fail() {
        sourceRecord.fail();
    }

    @Override
    public Optional<String> getDestinationTopic() {
        return sourceRecord.getDestinationTopic();
    }

    @Override
    public Schema<T> getSchema() {
        if (sourceRecord == null) {
            return null;
        }

        if (sourceRecord.getSchema() != null) {
            // unwrap actual schema
            Schema<T> schema =  sourceRecord.getSchema();
            // AutoConsumeSchema is a special schema, that comes into play
            // when the Sink is going to handle any Schema
            // usually you see Sink<GenericObject> or Sink<GenericRecord> in this case
            if (schema instanceof AutoConsumeSchema) {
                // extract the Schema from the message, this is the most accurate schema we have
                // see PIP-85
                if (sourceRecord.getMessage().isPresent()
                        && sourceRecord.getMessage().get().getReaderSchema().isPresent()) {
                    schema = (Schema<T>) sourceRecord.getMessage().get().getReaderSchema().get();
                } else {
                    schema = (Schema<T>) ((AutoConsumeSchema) schema).getInternalSchema();
                }
            }
            return schema;
        }

        if (sourceRecord instanceof KVRecord) {
            KVRecord kvRecord = (KVRecord) sourceRecord;
            return KeyValueSchemaImpl.of(kvRecord.getKeySchema(), kvRecord.getValueSchema(),
                    kvRecord.getKeyValueEncodingType());
        }

        return null;
    }

    @Override
    public Optional<Long> getEventTime() {
        return sourceRecord.getEventTime();
    }

    @Override
    public Optional<Message<T>> getMessage() {
        return sourceRecord.getMessage();
    }
}
