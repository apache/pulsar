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
package org.apache.pulsar.broker.service;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.client.impl.schema.KeyValueSchemaImpl;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * Null value message produce and consume test.
 */
@Slf4j
@Test(groups = "broker")
public class KeyValueTest extends BrokerTestBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void keyValueAutoConsumeTest()  throws Exception {
        String topic = "persistent://prop/ns-abc/kv-record";
        admin.topics().createNonPartitionedTopic(topic);

        RecordSchemaBuilder builder = SchemaBuilder
                .record("test");
                builder.field("test").type(SchemaType.STRING);
        GenericSchema<GenericRecord> schema = GenericAvroSchema.of(builder.build(SchemaType.AVRO));

        GenericRecord key = schema.newRecordBuilder().set("test", "foo").build();
        GenericRecord value = schema.newRecordBuilder().set("test", "bar").build();

        @Cleanup
        Producer<KeyValue<GenericRecord, GenericRecord>> producer = pulsarClient
                .newProducer(KeyValueSchemaImpl.of(schema, schema))
                .topic(topic)
                .create();

        producer.newMessage().value(new KeyValue<>(key, value)).send();

        @Cleanup
        Consumer<KeyValue<GenericRecord, GenericRecord>> consumer = pulsarClient
                .newConsumer(KeyValueSchemaImpl.of(Schema.AUTO_CONSUME(), Schema.AUTO_CONSUME()))
                .topic(topic)
                .subscriptionName("test")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();


        Message<KeyValue<GenericRecord, GenericRecord>> message = consumer.receive();
        assertEquals(key.getField("test"), message.getValue().getKey().getField("test"));
        assertEquals(value.getField("test"), message.getValue().getValue().getField("test"));

    }

}
