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

package org.apache.pulsar.io.kafka;

import lombok.EqualsAndHashCode;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.connect.data.Schema;

@EqualsAndHashCode(callSuper=true)
public class ProducerRecordWithSchema<K, V> extends ProducerRecord<K, V> {

    final Schema keySchema;
    final Schema valueSchema;

    public ProducerRecordWithSchema(String topic, Integer partition, Long timestamp, K key, V value,
                                    Iterable<Header> headers, Schema keySchema, Schema valueSchema) {
        super(topic, partition, timestamp, key, value, headers);
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
    }

    public ProducerRecordWithSchema(String topic, Integer partition, Long timestamp,
                                    K key, V value, Schema keySchema, Schema valueSchema) {
        super(topic, partition, timestamp, key, value);
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
    }

    public ProducerRecordWithSchema(String topic, Integer partition, K key, V value,
                                    Iterable<Header> headers, Schema keySchema, Schema valueSchema) {
        super(topic, partition, key, value, headers);
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
    }

    public ProducerRecordWithSchema(String topic, Integer partition,
                                    K key, V value, Schema keySchema, Schema valueSchema) {
        super(topic, partition, key, value);
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
    }

    public ProducerRecordWithSchema(String topic, K key, V value, Schema keySchema, Schema valueSchema) {
        super(topic, key, value);
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
    }

    public ProducerRecordWithSchema(String topic, V value, Schema valueSchema) {
        super(topic, value);
        this.keySchema = Schema.STRING_SCHEMA;
        this.valueSchema = valueSchema;
    }

    public Schema getKeySchema() {
        return keySchema;
    }

    public Schema getValueSchema() {
        return valueSchema;
    }
}
