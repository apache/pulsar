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
import org.apache.pulsar.client.api.schema.GenericObject;
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
public class TestGenericObjectSink implements Sink<GenericObject> {

    @Override
    public void open(Map<String, Object> config, SinkContext sourceContext) throws Exception {
    }

    public void write(Record<GenericObject> record) {
        log.info("received record {} {}", record, record.getClass());
        log.info("schema {}", record.getSchema());
        log.info("native schema {}", record.getSchema().getNativeSchema().orElse(null));

        log.info("value {}", record.getValue());
        log.info("value schema type {}", record.getValue().getSchemaType());
        log.info("value native object {}", record.getValue().getNativeObject());
    }

    @Override
    public void close() throws Exception {

    }
}
