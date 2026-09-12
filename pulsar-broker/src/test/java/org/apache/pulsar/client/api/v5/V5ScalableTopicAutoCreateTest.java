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
import static org.testng.Assert.assertThrows;
import java.util.UUID;
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.apache.pulsar.common.policies.data.ScalableTopicMetadata;
import org.testng.annotations.Test;

/**
 * A V5 client looking up a {@code topic://...} scalable topic that doesn't exist yet should
 * have it auto-created with a single initial segment — gated by the same broker/namespace
 * auto-topic-creation policy as regular topics (PIP-468 / PIP-483). When the policy disallows
 * it, the lookup fails as before.
 */
public class V5ScalableTopicAutoCreateTest extends V5ClientBaseTest {

    private String freshTopic() {
        return "topic://" + getNamespace() + "/autocreate-"
                + UUID.randomUUID().toString().substring(0, 8);
    }

    @Test
    public void testProducerAutoCreatesScalableTopicWithOneSegment() throws Exception {
        String topic = freshTopic();

        // No admin.createScalableTopic — the producer's lookup must create it.
        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        assertNotNull(producer.newMessage().value("hello").send());

        ScalableTopicMetadata md = admin.scalableTopics().getMetadata(topic);
        assertNotNull(md, "scalable topic must have been auto-created");
        assertEquals(md.getSegments().size(), 1, "auto-create uses a single initial segment");
    }

    @Test
    public void testConsumerAutoCreatesScalableTopic() throws Exception {
        String topic = freshTopic();

        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("sub")
                .subscribe();

        ScalableTopicMetadata md = admin.scalableTopics().getMetadata(topic);
        assertNotNull(md);
        assertEquals(md.getSegments().size(), 1);
    }

    @Test
    public void testNoAutoCreateWhenNamespaceDisallows() throws Exception {
        // Turn auto-topic-creation off for this namespace; a lookup of a non-existent
        // scalable topic must then fail instead of creating one.
        admin.namespaces().setAutoTopicCreation(getNamespace(),
                AutoTopicCreationOverride.builder().allowAutoTopicCreation(false).build());
        try {
            String topic = freshTopic();
            // The lookup must surface a typed not-found, exactly as a non-existent regular
            // topic does — not a generic PulsarClientException.
            assertThrows(PulsarClientException.NotFoundException.class, () ->
                    v5Client.newProducer(Schema.string()).topic(topic).create());
        } finally {
            admin.namespaces().removeAutoTopicCreation(getNamespace());
        }
    }
}
