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
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.testng.annotations.Test;

/**
 * Coverage for {@link QueueConsumerBuilder#receiverQueueSize(int)}: the V5 builder
 * must wire the user-supplied prefetch depth down to every per-segment v4
 * {@code ConsumerImpl}. Without this wiring, the V5 setting would be silently
 * ignored — the v4 layer would default to its own receiver queue size and the
 * caller would have no way of knowing.
 *
 * <p>Verified end-to-end by reaching into the V5 internals via reflection (the
 * V5 {@link QueueConsumer} interface itself doesn't expose the v4 consumers,
 * since end users never need them).
 */
public class V5ConsumerReceiverQueueSizeTest extends V5ClientBaseTest {

    @Test
    public void testReceiverQueueSizePropagatesToV4Consumer() throws Exception {
        String topic = newScalableTopic(1);
        int requested = 17;

        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("rq-size-sub")
                .receiverQueueSize(requested)
                .subscribe();

        int actual = readV4ReceiverQueueSize(consumer);
        assertEquals(actual, requested,
                "V5 receiverQueueSize must propagate to the per-segment v4 ConsumerImpl");
    }

    /**
     * Multi-segment topic: every per-segment v4 consumer must inherit the same
     * V5-configured prefetch depth, otherwise individual segments would buffer
     * more than the user asked for.
     */
    @Test
    public void testReceiverQueueSizeAppliesToEverySegment() throws Exception {
        String topic = newScalableTopic(3);
        int requested = 9;

        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("rq-size-multi-sub")
                .receiverQueueSize(requested)
                .subscribe();

        Map<Long, ?> segmentConsumers = readSegmentConsumers(consumer);
        assertEquals(segmentConsumers.size(), 3, "expected one v4 consumer per segment");
        for (CompletableFuture<?> future : asConsumerFutures(segmentConsumers)) {
            Object v4Consumer = future.get();
            int actual = invokeGetCurrentReceiverQueueSize(v4Consumer);
            assertEquals(actual, requested,
                    "every segment's v4 ConsumerImpl must carry the same receiverQueueSize");
        }
    }

    // --- Helpers ---

    private static int readV4ReceiverQueueSize(QueueConsumer<?> consumer) throws Exception {
        Map<Long, ?> segmentConsumers = readSegmentConsumers(consumer);
        assertEquals(segmentConsumers.size(), 1, "expected a single segment for this test");
        CompletableFuture<?> future = asConsumerFutures(segmentConsumers).iterator().next();
        Object v4Consumer = future.get();
        return invokeGetCurrentReceiverQueueSize(v4Consumer);
    }

    private static Map<Long, ?> readSegmentConsumers(QueueConsumer<?> consumer) throws Exception {
        Field field = consumer.getClass().getDeclaredField("segmentConsumers");
        field.setAccessible(true);
        Object map = field.get(consumer);
        assertNotNull(map, "expected segmentConsumers map on V5 queue consumer");
        return (Map<Long, ?>) map;
    }

    @SuppressWarnings("unchecked")
    private static Iterable<CompletableFuture<?>> asConsumerFutures(Map<Long, ?> segmentConsumers) {
        return (Iterable<CompletableFuture<?>>) (Iterable<?>) segmentConsumers.values();
    }

    private static int invokeGetCurrentReceiverQueueSize(Object v4Consumer) throws Exception {
        // Defined on org.apache.pulsar.client.impl.ConsumerBase — use reflection so the
        // test doesn't have to import a non-API class.
        var method = v4Consumer.getClass().getMethod("getCurrentReceiverQueueSize");
        return (int) method.invoke(v4Consumer);
    }
}
