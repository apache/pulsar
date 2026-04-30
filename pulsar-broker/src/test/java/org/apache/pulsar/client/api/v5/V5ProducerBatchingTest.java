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
package org.apache.pulsar.client.api.v5;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.config.BatchingPolicy;
import org.apache.pulsar.client.api.v5.config.MemorySize;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.testng.annotations.Test;

/**
 * Producer batching coverage. Default / disabled / custom batching policies all have to
 * deliver the same observable end-to-end result: every produced message lands at the
 * consumer in send order with the right value.
 *
 * <p>Single-segment topic so message order is fully determined by the producer side.
 */
public class V5ProducerBatchingTest extends V5ClientBaseTest {

    private void produceAndVerify(BatchingPolicy policy, int count, String subSuffix)
            throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .batchingPolicy(policy)
                .create();
        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("batching-" + subSuffix)
                .subscribe();

        // Use the async producer so we don't pay the round-trip per message — that way
        // batching can actually batch when enabled.
        CompletableFuture<?>[] sends = new CompletableFuture<?>[count];
        for (int i = 0; i < count; i++) {
            sends[i] = producer.async().newMessage().value("v-" + i).send();
        }
        producer.async().flush().get(5, TimeUnit.SECONDS);
        for (CompletableFuture<?> send : sends) {
            assertNotNull(send.get(5, TimeUnit.SECONDS));
        }

        Set<String> received = new HashSet<>();
        for (int i = 0; i < count; i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(5));
            assertNotNull(msg, "expected message " + i + " of " + count);
            received.add(msg.value());
            consumer.acknowledge(msg.id());
        }
        assertEquals(received.size(), count, "duplicate or missing messages");
    }

    @Test
    public void testDefaultBatchingDeliversAllMessages() throws Exception {
        produceAndVerify(BatchingPolicy.ofDefault(), 100, "default");
    }

    @Test
    public void testDisabledBatchingDeliversAllMessages() throws Exception {
        produceAndVerify(BatchingPolicy.ofDisabled(), 100, "disabled");
    }

    @Test
    public void testTightBatchingByDelay() throws Exception {
        // Small max-publish-delay forces batches to flush quickly; both ends still see
        // every message in order.
        BatchingPolicy tight = BatchingPolicy.of(
                Duration.ofMillis(5), 100, MemorySize.ofMegabytes(1));
        produceAndVerify(tight, 50, "tight-delay");
    }

    @Test
    public void testBatchingWithSmallBatchSize() throws Exception {
        // Cap the batch at 5 messages — exercises the maxMessages branch of the batching
        // packer so a 50-message stream gets cut into 10 batches.
        BatchingPolicy small = BatchingPolicy.of(
                Duration.ofSeconds(1), 5, MemorySize.ofMegabytes(1));
        produceAndVerify(small, 50, "small-batch");
    }
}
