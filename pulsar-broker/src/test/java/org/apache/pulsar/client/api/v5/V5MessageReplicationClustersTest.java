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
import java.time.Duration;
import java.util.List;
import lombok.Cleanup;
import org.apache.pulsar.broker.service.SharedPulsarCluster;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.impl.MessageImpl;
import org.testng.annotations.Test;

/**
 * Coverage for {@link MessageBuilder#replicationClusters(java.util.List)}: the
 * V5 setter must propagate to the v4 {@code TypedMessageBuilder} that V5 uses
 * internally, so the cluster-restriction list lands in the message metadata
 * the broker stores.
 *
 * <p>Verified by sending a V5 message with an explicit cluster restriction and
 * reflecting into the V5 {@code MessageV5} wrapper to inspect the underlying
 * v4 {@code MessageImpl.getReplicateTo()} — that's where the message metadata
 * becomes observable.
 */
public class V5MessageReplicationClustersTest extends V5ClientBaseTest {

    @Test
    public void testReplicationClustersPropagatesToMessageMetadata() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("repl-clusters-watcher")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();

        // Restrict to the local cluster only — same effective behaviour as the
        // namespace default, but exercises the explicit-cluster code path.
        List<String> clusters = List.of(SharedPulsarCluster.CLUSTER_NAME);

        producer.newMessage()
                .value("hello")
                .replicationClusters(clusters)
                .send();

        Message<String> msg = consumer.receive(Duration.ofSeconds(5));
        assertNotNull(msg, "consumer should receive the produced message");

        // Reach into the V5 MessageV5 wrapper to inspect the underlying v4 metadata.
        MessageImpl<?> v4Impl = readUnderlyingV4Message(msg);
        assertEquals(v4Impl.getReplicateTo(), clusters,
                "replicationClusters from V5 must land in the message metadata");
    }

    private static MessageImpl<?> readUnderlyingV4Message(Message<?> v5Msg) throws Exception {
        Field f = v5Msg.getClass().getDeclaredField("v4Message");
        f.setAccessible(true);
        Object v4Msg = f.get(v5Msg);
        assertNotNull(v4Msg, "expected v4Message inside V5 MessageV5 wrapper");
        if (!(v4Msg instanceof MessageImpl)) {
            throw new AssertionError("expected MessageImpl, got: " + v4Msg.getClass());
        }
        return (MessageImpl<?>) v4Msg;
    }
}
