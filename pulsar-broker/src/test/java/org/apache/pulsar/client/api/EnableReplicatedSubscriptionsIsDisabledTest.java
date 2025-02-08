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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
public class EnableReplicatedSubscriptionsIsDisabledTest extends ProducerConsumerBase {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        conf.setEnableReplicatedSubscriptions(false);
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testReplicateSubscriptionStateIsEnabled() throws Exception {
        String topicName = TopicName.get("my-property/my-ns/testReplicateSubscriptionStateIsEnabled").toString();
        String subName = "my-subscription";
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer(Schema.BYTES)
                .topic(topicName)
                .subscriptionName(subName)
                .replicateSubscriptionState(true)
                .subscribe();
        CompletableFuture<Optional<Topic>> topicIfExists = pulsar.getBrokerService().getTopicIfExists(topicName);
        assertThat(topicIfExists)
                .succeedsWithin(3, TimeUnit.SECONDS)
                .matches(optionalTopic -> {
                    assertTrue(optionalTopic.isPresent());
                    Topic topicRef = optionalTopic.get();
                    Subscription subscription = topicRef.getSubscription(subName);
                    assertNotNull(subscription);
                    assertFalse(subscription.isReplicated());
                    return true;
                });
    }
}
