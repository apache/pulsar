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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.testng.annotations.Test;

/**
 * Coverage for {@link ProducerBuilder#sendTimeout(Duration)} and
 * {@link ProducerBuilder#blockIfQueueFull(boolean)}: the V5 builder must wire
 * each user-supplied flow-control knob down to every per-segment v4
 * {@code ProducerImpl}. Without this wiring, the V5 setting would be silently
 * ignored — the v4 layer would default and the caller would have no way of
 * knowing.
 *
 * <p>Behavioural verification of the actual timeout-firing and block-on-full
 * paths lives in the v4 test suite (e.g. {@code SimpleProducerConsumerTest
 * .testSendTimeout}); those tests stop the broker mid-send to force the
 * pending-queue overflow / timeout, which the in-process shared cluster used
 * here cannot do. The plumbing test suffices as a regression guard for the V5
 * → v4 mapping.
 */
public class V5ProducerFlowControlTest extends V5ClientBaseTest {

    @Test
    public void testSendTimeoutPropagatesToV4Producer() throws Exception {
        String topic = newScalableTopic(1);
        Duration requested = Duration.ofSeconds(7);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .sendTimeout(requested)
                .create();
        // V5 segment producers are created lazily on first send — produce a
        // message so the per-segment v4 ProducerImpl exists to inspect.
        producer.newMessage().value("warm-up").send();

        ProducerConfigurationData conf = readV4ProducerConf(producer);
        assertEquals(conf.getSendTimeoutMs(), requested.toMillis(),
                "V5 sendTimeout must propagate to the per-segment v4 ProducerImpl");
    }

    @Test
    public void testBlockIfQueueFullTrue() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .blockIfQueueFull(true)
                .create();
        producer.newMessage().value("warm-up").send();

        ProducerConfigurationData conf = readV4ProducerConf(producer);
        assertTrue(conf.isBlockIfQueueFull(),
                "blockIfQueueFull(true) must propagate to the v4 ProducerImpl");
    }

    @Test
    public void testBlockIfQueueFullFalse() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .blockIfQueueFull(false)
                .create();
        producer.newMessage().value("warm-up").send();

        ProducerConfigurationData conf = readV4ProducerConf(producer);
        assertFalse(conf.isBlockIfQueueFull(),
                "blockIfQueueFull(false) must propagate to the v4 ProducerImpl");
    }

    /**
     * Multi-segment topic: every per-segment v4 producer must inherit the same
     * V5-configured sendTimeout, otherwise individual segments would honor
     * different deadlines than the user asked for.
     */
    @Test
    public void testSendTimeoutAppliesToEverySegment() throws Exception {
        String topic = newScalableTopic(3);
        Duration requested = Duration.ofMillis(2_500);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .sendTimeout(requested)
                .create();
        // Send across keys so every segment lazily materializes its v4 producer.
        for (int i = 0; i < 30; i++) {
            producer.newMessage().key("k-" + i).value("warm-" + i).send();
        }

        Map<Long, ?> segmentProducers = readSegmentProducers(producer);
        assertEquals(segmentProducers.size(), 3, "expected one v4 producer per segment");
        for (CompletableFuture<?> future : asProducerFutures(segmentProducers)) {
            Object v4Producer = future.get();
            ProducerConfigurationData conf = readConfField(v4Producer);
            assertEquals(conf.getSendTimeoutMs(), requested.toMillis(),
                    "every segment's v4 ProducerImpl must carry the same sendTimeout");
        }
    }

    // --- Helpers ---

    private static ProducerConfigurationData readV4ProducerConf(Producer<?> producer) throws Exception {
        Map<Long, ?> segmentProducers = readSegmentProducers(producer);
        assertEquals(segmentProducers.size(), 1, "expected a single segment for this test");
        CompletableFuture<?> future = asProducerFutures(segmentProducers).iterator().next();
        Object v4Producer = future.get();
        return readConfField(v4Producer);
    }

    private static Map<Long, ?> readSegmentProducers(Producer<?> producer) throws Exception {
        Field field = producer.getClass().getDeclaredField("segmentProducers");
        field.setAccessible(true);
        Object map = field.get(producer);
        assertNotNull(map, "expected segmentProducers map on V5 producer");
        return (Map<Long, ?>) map;
    }

    @SuppressWarnings("unchecked")
    private static Iterable<CompletableFuture<?>> asProducerFutures(Map<Long, ?> segmentProducers) {
        return (Iterable<CompletableFuture<?>>) (Iterable<?>) segmentProducers.values();
    }

    private static ProducerConfigurationData readConfField(Object v4Producer) throws Exception {
        // ProducerBase#conf is protected; walk the class hierarchy to find it.
        Class<?> c = v4Producer.getClass();
        while (c != null) {
            try {
                Field f = c.getDeclaredField("conf");
                f.setAccessible(true);
                return (ProducerConfigurationData) f.get(v4Producer);
            } catch (NoSuchFieldException e) {
                c = c.getSuperclass();
            }
        }
        throw new NoSuchFieldException("conf not found on " + v4Producer.getClass());
    }
}
