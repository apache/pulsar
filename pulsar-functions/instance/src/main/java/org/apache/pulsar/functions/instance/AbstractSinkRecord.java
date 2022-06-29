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

import java.util.Optional;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.client.impl.schema.KeyValueSchemaImpl;
import org.apache.pulsar.functions.api.KVRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.source.PulsarRecord;

@EqualsAndHashCode
@ToString
public abstract class AbstractSinkRecord<T> implements Record<T> {

    private final Record<?> sourceRecord;

    protected AbstractSinkRecord(Record<?> sourceRecord) {
        this.sourceRecord = sourceRecord;
    }

    public abstract boolean shouldAlwaysSetMessageProperties();

    public Record<?> getSourceRecord() {
        return sourceRecord;
    }

    @Override
    public Optional<String> getTopicName() {
        return sourceRecord.getTopicName();
    }

    @Override
    public void ack() {
        sourceRecord.ack();
    }

    /**
     * Some sink sometimes wants to control the ack type.
     */
    public void cumulativeAck() {
        if (sourceRecord instanceof PulsarRecord) {
            PulsarRecord pulsarRecord = (PulsarRecord) sourceRecord;
            pulsarRecord.cumulativeAck();
        } else {
            throw new RuntimeException("SourceRecord class type must be PulsarRecord");
        }
    }

    /**
     * Some sink sometimes wants to control the ack type.
     */
    public void individualAck() {
        if (sourceRecord instanceof PulsarRecord) {
            PulsarRecord pulsarRecord = (PulsarRecord) sourceRecord;
            pulsarRecord.individualAck();
        } else {
            throw new RuntimeException("SourceRecord class type must be PulsarRecord");
        }
    }

    @Override
    public void fail() {
        sourceRecord.fail();
    }

    protected static <T> Schema<T> getRecordSchema(Record<T> record) {
        if (record == null) {
            return null;
        }

        if (record.getSchema() != null) {
            // unwrap actual schema
            Schema<T> schema = record.getSchema();
            // AutoConsumeSchema is a special schema, that comes into play
            // when the Sink is going to handle any Schema
            // usually you see Sink<GenericObject> or Sink<GenericRecord> in this case
            if (schema instanceof AutoConsumeSchema) {
                // extract the Schema from the message, this is the most accurate schema we have
                // see PIP-85
                if (record.getMessage().isPresent()
                        && record.getMessage().get().getReaderSchema().isPresent()) {
                    schema = (Schema<T>) record.getMessage().get().getReaderSchema().get();
                } else {
                    schema = (Schema<T>) ((AutoConsumeSchema) schema).getInternalSchema();
                }
            }
            return schema;
        }

        if (record instanceof KVRecord) {
            KVRecord kvRecord = (KVRecord) record;
            return KeyValueSchemaImpl.of(kvRecord.getKeySchema(), kvRecord.getValueSchema(),
                    kvRecord.getKeyValueEncodingType());
        }

        return null;
    }
}
