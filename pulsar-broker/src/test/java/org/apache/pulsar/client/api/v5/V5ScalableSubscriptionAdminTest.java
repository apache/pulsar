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
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.policies.data.ScalableSubscriptionType;
import org.testng.annotations.Test;

/**
 * Coverage for {@code admin.scalableTopics().createSubscription(...)}: the admin
 * API must materialize a cursor on every active segment so a consumer that
 * subscribes <em>after</em> messages are produced still receives those
 * messages — the whole point of pre-creating the subscription.
 *
 * <p>The behavioural assertion (a late consumer sees pre-subscription messages)
 * is the user-facing guarantee, and any regression in
 * {@code ScalableTopicController.createSubscriptionOnSegment} — which converts
 * each {@code SegmentInfo} to the underlying {@code persistent://} topic and
 * pre-creates the cursor through the standard topic admin API — would surface
 * here as messages going missing.
 */
public class V5ScalableSubscriptionAdminTest extends V5ClientBaseTest {

    @Test
    public void testPreCreatedSubscriptionRetainsPreProductionMessages() throws Exception {
        String topic = newScalableTopic(3);
        String subscription = "pre-created-sub";

        // Pre-create the subscription on the scalable topic. This must materialize a
        // cursor on every active segment so that subsequent produces are retained
        // until the consumer drains them.
        admin.scalableTopics().createSubscription(topic, subscription,
                ScalableSubscriptionType.QUEUE);

        // Produce *before* any consumer subscribes.
        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        int n = 30;
        Set<String> sent = new HashSet<>();
        for (int i = 0; i < n; i++) {
            String v = "msg-" + i;
            producer.newMessage().key("k-" + i).value(v).send();
            sent.add(v);
        }

        // Subscribe with the SAME subscription name. If createSubscription truly
        // pre-created cursors on every segment, the consumer must receive every
        // message produced above. If it didn't, the consumer attaches at "latest"
        // by default and drops the entire backlog.
        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName(subscription)
                .subscribe();

        Set<String> received = new HashSet<>();
        long deadline = System.currentTimeMillis() + 30_000L;
        while (received.size() < n && System.currentTimeMillis() < deadline) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(1));
            if (msg != null) {
                received.add(msg.value());
                consumer.acknowledge(msg.id());
            }
        }
        assertEquals(received, sent,
                "pre-created subscription must retain every message produced before consumer subscribed");
    }
}
