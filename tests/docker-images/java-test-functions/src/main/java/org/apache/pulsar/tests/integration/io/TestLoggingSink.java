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
package org.apache.pulsar.tests.integration.io;

import java.util.Map;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.slf4j.Logger;

public class TestLoggingSink implements Sink<GenericObject> {

    private Logger logger;
    private Producer<String> producer;

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        logger = sinkContext.getLogger();
        String topic = (String) config.getOrDefault("log-topic", "log-topic");
        producer = sinkContext.getPulsarClient().newProducer(Schema.STRING)
                .topic(topic)
                .create();
    }

    @Override
    public void write(Record<GenericObject> record) throws Exception {
        Object nativeObject = record.getValue().getNativeObject();
        logger.info("Got message: " + nativeObject + " with schema" + record.getSchema());
        String payload = nativeObject.toString();
        if (nativeObject instanceof KeyValue) {
            KeyValue kv = (KeyValue) nativeObject;
            String key = kv.getKey().toString();
            String value = kv.getValue().toString();

            if (kv.getKey() instanceof GenericObject) {
                key = ((GenericObject) kv.getKey()).getNativeObject().toString();
            }
            if (kv.getValue() instanceof GenericObject) {
                value = ((GenericObject) kv.getValue()).getNativeObject().toString();
            }
            payload = "(key = " + key + ", value = " + value + ")";
        }
        producer.newMessage()
            .properties(record.getProperties())
            .value(record.getSchema().getSchemaInfo().getType().name() + " - " + payload)
            .send();
        record.ack();
    }

    @Override
    public void close() {

    }
}
