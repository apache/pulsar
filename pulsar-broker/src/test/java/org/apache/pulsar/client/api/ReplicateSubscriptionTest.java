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
package org.apache.pulsar.client.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.client.impl.ConsumerBuilderImpl;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ReplicateSubscriptionTest extends ProducerConsumerBase {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
    }

    @DataProvider
    public Object[] replicateSubscriptionState() {
        return new Object[]{
                Boolean.TRUE,
                Boolean.FALSE,
                null
        };
    }

    @Test(dataProvider = "replicateSubscriptionState")
    public void testReplicateSubscriptionState(Boolean replicateSubscriptionState)
            throws Exception {
        String topic = "persistent://my-property/my-ns/" + System.nanoTime();
        String subName = "sub-" + System.nanoTime();
        ConsumerBuilder<String> consumerBuilder = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName(subName);
        if (replicateSubscriptionState != null) {
            consumerBuilder.replicateSubscriptionState(replicateSubscriptionState);
        }
        ConsumerBuilderImpl consumerBuilderImpl = (ConsumerBuilderImpl) consumerBuilder;
        assertEquals(consumerBuilderImpl.getConf().getReplicateSubscriptionState(), replicateSubscriptionState);
        @Cleanup
        Consumer<String> ignored = consumerBuilder.subscribe();
        CompletableFuture<Optional<Topic>> topicIfExists = pulsar.getBrokerService().getTopicIfExists(topic);
        assertThat(topicIfExists)
                .succeedsWithin(3, TimeUnit.SECONDS)
                .matches(optionalTopic -> {
                    assertTrue(optionalTopic.isPresent());
                    Topic topicRef = optionalTopic.get();
                    Subscription subscription = topicRef.getSubscription(subName);
                    assertNotNull(subscription);
                    assertTrue(subscription instanceof PersistentSubscription);
                    PersistentSubscription persistentSubscription = (PersistentSubscription) subscription;
                    assertEquals(persistentSubscription.getReplicatedControlled(), replicateSubscriptionState);
                    return true;
                });
    }
}
