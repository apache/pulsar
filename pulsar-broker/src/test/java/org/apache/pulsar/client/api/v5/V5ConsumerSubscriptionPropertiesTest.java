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
 * Coverage for {@link QueueConsumerBuilder#subscriptionProperties(Map)}: the V5
 * setter must propagate to every per-segment v4 {@code Consumer}'s
 * {@link org.apache.pulsar.client.impl.conf.ConsumerConfigurationData}, which
 * is what the v4 wire layer ships to the broker on subscribe.
 */
public class V5ConsumerSubscriptionPropertiesTest extends V5ClientBaseTest {

    @Test
    public void testSubscriptionPropertiesPropagateToV4Consumer() throws Exception {
        String topic = newScalableTopic(1);
        Map<String, String> props = Map.of("env", "prod", "team", "data-platform");

        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("sub-props-test")
                .subscriptionProperties(props)
                .subscribe();

        Map<String, String> actual = readSubscriptionPropertiesFromV4(consumer);
        assertEquals(actual, props,
                "V5 subscriptionProperties must propagate to the v4 consumer config");
    }

    @Test
    public void testSubscriptionPropertiesAppliesToEverySegment() throws Exception {
        String topic = newScalableTopic(3);
        Map<String, String> props = Map.of("region", "us-east-1");

        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("sub-props-multi-test")
                .subscriptionProperties(props)
                .subscribe();

        Map<Long, ?> segmentConsumers = readSegmentConsumers(consumer);
        assertEquals(segmentConsumers.size(), 3, "expected one v4 consumer per segment");
        for (CompletableFuture<?> future : asConsumerFutures(segmentConsumers)) {
            Object v4Consumer = future.get();
            Map<String, String> v4Props = readConfSubscriptionProperties(v4Consumer);
            assertEquals(v4Props, props,
                    "every segment's v4 Consumer must carry the same subscriptionProperties");
        }
    }

    // --- Helpers ---

    private static Map<String, String> readSubscriptionPropertiesFromV4(QueueConsumer<?> consumer)
            throws Exception {
        Map<Long, ?> segmentConsumers = readSegmentConsumers(consumer);
        assertEquals(segmentConsumers.size(), 1, "expected a single segment for this test");
        CompletableFuture<?> future = asConsumerFutures(segmentConsumers).iterator().next();
        Object v4Consumer = future.get();
        return readConfSubscriptionProperties(v4Consumer);
    }

    private static Map<Long, ?> readSegmentConsumers(QueueConsumer<?> consumer) throws Exception {
        Field f = consumer.getClass().getDeclaredField("segmentConsumers");
        f.setAccessible(true);
        Object map = f.get(consumer);
        assertNotNull(map, "expected segmentConsumers map on V5 queue consumer");
        return (Map<Long, ?>) map;
    }

    @SuppressWarnings("unchecked")
    private static Iterable<CompletableFuture<?>> asConsumerFutures(Map<Long, ?> segmentConsumers) {
        return (Iterable<CompletableFuture<?>>) (Iterable<?>) segmentConsumers.values();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> readConfSubscriptionProperties(Object v4Consumer) throws Exception {
        // ConsumerBase#conf is protected; walk the class hierarchy.
        Class<?> c = v4Consumer.getClass();
        while (c != null) {
            try {
                Field f = c.getDeclaredField("conf");
                f.setAccessible(true);
                Object conf = f.get(v4Consumer);
                var getter = conf.getClass().getMethod("getSubscriptionProperties");
                return (Map<String, String>) getter.invoke(conf);
            } catch (NoSuchFieldException e) {
                c = c.getSuperclass();
            }
        }
        throw new NoSuchFieldException("conf not found on " + v4Consumer.getClass());
    }
}
