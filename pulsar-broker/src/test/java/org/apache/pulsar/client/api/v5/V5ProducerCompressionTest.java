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
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.config.CompressionPolicy;
import org.apache.pulsar.client.api.v5.config.CompressionType;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.testng.annotations.Test;

/**
 * Producer-side compression coverage. For each {@link CompressionType} (plus the
 * disabled / default cases), produce a payload that's redundant enough to actually
 * compress, then read it back through a V5 consumer and confirm the decompression
 * yields the original value.
 */
public class V5ProducerCompressionTest extends V5ClientBaseTest {

    private static final String PAYLOAD = "x".repeat(4096);

    private void roundtripWithPolicy(CompressionPolicy policy, String subSuffix) throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .compressionPolicy(policy)
                .create();
        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("compression-" + subSuffix)
                .subscribe();

        producer.newMessage().value(PAYLOAD).send();

        Message<String> msg = consumer.receive(Duration.ofSeconds(5));
        assertNotNull(msg, "compressed message did not arrive within 5s");
        assertEquals(msg.value(), PAYLOAD,
                "decompressed value does not match what was sent");
        consumer.acknowledge(msg.id());
    }

    @Test
    public void testCompressionDisabled() throws Exception {
        roundtripWithPolicy(CompressionPolicy.disabled(), "disabled");
    }

    @Test
    public void testCompressionLz4() throws Exception {
        roundtripWithPolicy(CompressionPolicy.of(CompressionType.LZ4), "lz4");
    }

    @Test
    public void testCompressionZlib() throws Exception {
        roundtripWithPolicy(CompressionPolicy.of(CompressionType.ZLIB), "zlib");
    }

    @Test
    public void testCompressionZstd() throws Exception {
        roundtripWithPolicy(CompressionPolicy.of(CompressionType.ZSTD), "zstd");
    }

    @Test
    public void testCompressionSnappy() throws Exception {
        roundtripWithPolicy(CompressionPolicy.of(CompressionType.SNAPPY), "snappy");
    }

    @Test
    public void testNoCompressionPolicySetUsesDefault() throws Exception {
        // Skip the helper — exercise the "didn't call compressionPolicy()" path.
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("compression-default-sub")
                .subscribe();

        producer.newMessage().value(PAYLOAD).send();
        Message<String> msg = consumer.receive(Duration.ofSeconds(5));
        assertNotNull(msg);
        assertEquals(msg.value(), PAYLOAD);
        consumer.acknowledge(msg.id());
    }
}
