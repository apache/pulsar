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
package org.apache.pulsar.tests.integration.io;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.KVRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.Source;
import org.apache.pulsar.io.core.SourceContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class TestKeyValueSink implements Sink<KeyValue<String, Integer>> {

    @Override
    public void open(Map<String, Object> config, SinkContext sourceContext) throws Exception {
    }

    public void write(Record<KeyValue<String, Integer>> record) {
        log.info("write {} {} {}", record, record.getClass(), record.getSchema());
        if (!(record instanceof KVRecord)) {
            throw new RuntimeException("Expected a KVRecord, but got a "+record.getClass());
        }
        KVRecord<String, Integer> kvRecord = (KVRecord<String, Integer>) record;
        if (kvRecord.getKeySchema().getSchemaInfo().getType() != SchemaType.STRING) {
            throw new RuntimeException("Expected a String key schema but it was a "+kvRecord.getKeySchema());
        }

        if (kvRecord.getValueSchema().getSchemaInfo().getType() != SchemaType.INT32) {
            throw new RuntimeException("Expected a Integer key schema but it was a "+kvRecord.getValueSchema());
        }
    }
    @Override
    public void close() throws Exception {

    }
}
