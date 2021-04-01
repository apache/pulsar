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

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.KeyValueSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.functions.api.KVRecord;
import org.apache.pulsar.functions.api.Record;

public class SinkKVRecord<K,V> extends SinkRecord<KeyValue<K,V>> implements KVRecord<K,V> {

    public SinkKVRecord(Record<KeyValue<K, V>> sourceRecord, KeyValue<K, V> value) {
        super(sourceRecord, value);
    }

    @Override
    public Schema<K> getKeySchema() {
        return ((KeyValueSchema) this.getSchema()).getKeySchema();
    }

    @Override
    public Schema<V> getValueSchema() {
        return ((KeyValueSchema) this.getSchema()).getValueSchema();
    }

    @Override
    public KeyValueEncodingType getKeyValueEncodingType() {
        return ((KeyValueSchema) this.getSchema()).getKeyValueEncodingType();
    }
}
