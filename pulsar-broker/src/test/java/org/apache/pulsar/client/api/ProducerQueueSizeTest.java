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
package org.apache.pulsar.client.api;

import lombok.Cleanup;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class ProducerQueueSizeTest extends ProducerConsumerBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider(name = "matrix")
    public Object[][] matrix() {
        return new Object[][]{
                {Boolean.FALSE, Boolean.FALSE},
                {Boolean.FALSE, Boolean.TRUE},
                {Boolean.TRUE, Boolean.FALSE},
                {Boolean.TRUE, Boolean.TRUE},
        };
    }

    @Test(dataProvider = "matrix")
    public void testRemoveMaxQueueLimit(boolean blockIfQueueFull, boolean partitioned) throws Exception {
        String topic = newTopicName();

        if (partitioned) {
            admin.topics().createPartitionedTopic(topic, 10);
        }

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(brokerUrl.toString())
                .memoryLimit(10, SizeUnit.KILO_BYTES)
                .build();

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(topic)
                .blockIfQueueFull(blockIfQueueFull)
                .maxPendingMessages(0)
                .maxPendingMessagesAcrossPartitions(0)
                .create();

        List<CompletableFuture<?>> futures = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            futures.add(producer.sendAsync("hello"));
        }

        producer.flush();

        for (CompletableFuture<?>f : futures) {
            f.get();
        }
    }
}
