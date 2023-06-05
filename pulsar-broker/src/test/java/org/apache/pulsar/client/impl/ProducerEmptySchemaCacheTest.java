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
package org.apache.pulsar.client.impl;

import lombok.Cleanup;
import org.apache.pulsar.client.api.MockBrokerService;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.testng.Assert.assertEquals;

@Test(groups = "broker-impl")
public class ProducerEmptySchemaCacheTest {

    MockBrokerService mockBrokerService;

    @BeforeClass(alwaysRun = true)
    public void setup() {
        mockBrokerService = new MockBrokerService();
        mockBrokerService.start();
    }

    @AfterClass(alwaysRun = true)
    public void teardown() {
        mockBrokerService.stop();
    }

    @Test
    public void testProducerShouldCacheEmptySchema() throws Exception {
        @Cleanup
        PulsarClientImpl client = (PulsarClientImpl) PulsarClient.builder()
                .serviceUrl(mockBrokerService.getBrokerAddress())
                .build();

        AtomicLong counter = new AtomicLong(0);

        mockBrokerService.setHandleGetOrCreateSchema((ctx, commandGetOrCreateSchema) -> {
            counter.incrementAndGet();
            ctx.writeAndFlush(
                    Commands.newGetOrCreateSchemaResponse(commandGetOrCreateSchema.getRequestId(),
                            SchemaVersion.Empty));
        });

        // this schema mode is used in consumer retry and dlq Producer
        // when the origin consumer has Schema.BYTES schema
        // and when retry message or dlq message is send
        // will use typed message builder set Schema.Bytes to send message.

        Schema<byte[]> schema = Schema.BYTES;
        Schema<byte[]> readerSchema = Schema.BYTES;

        @Cleanup
        Producer<byte[]> dlqProducer = client.newProducer(Schema.AUTO_PRODUCE_BYTES(schema))
                .topic("testAutoProduceBytesSchemaShouldCache")
                .sendTimeout(5, TimeUnit.SECONDS)
                .maxPendingMessages(0)
                .enableBatching(false)
                .create();

        for (int i = 10; i > 0; i--) {
            TypedMessageBuilder<byte[]> typedMessageBuilderNew =
                    dlqProducer.newMessage(Schema.AUTO_PRODUCE_BYTES(readerSchema))
                            .value("hello".getBytes());

            typedMessageBuilderNew.send();
        }

        // schema should only be requested once.
        // and if the schemaVersion is empty (e.g. Schema.BYTES)
        // it should be cached by the client
        // to avoid continuously send `CommandGetOrCreateSchema` rpc
        assertEquals(counter.get(), 1);
    }
}
