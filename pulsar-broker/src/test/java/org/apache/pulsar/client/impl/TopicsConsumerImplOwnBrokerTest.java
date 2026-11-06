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
package org.apache.pulsar.client.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Multi-topics consumer tests that use topic names without a namespace, which resolve to {@code public/default}, so
 * they run on a broker of their own. {@link TopicsConsumerImplTest} has the tests that share one.
 */
@Test(groups = "broker-impl")
public class TopicsConsumerImplOwnBrokerTest extends ProducerConsumerBase {

    @BeforeClass
    @Override
    public void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testResubscribeSameTopic() throws Exception {
        final String localTopicName = "TopicsConsumerResubscribeSameTopicTest";
        final String localPartitionName = localTopicName + "-partition-0";
        final String topicNameWithNamespace = "public/default/" + localTopicName;
        final String topicNameWithDomain = "persistent://" + topicNameWithNamespace;

        admin.topics().createPartitionedTopic(localTopicName, 2);

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(localTopicName)
                .subscriptionName("SubscriptionName")
                .subscribe();

        assertTrue(consumer instanceof MultiTopicsConsumerImpl);
        MultiTopicsConsumerImpl<byte[]> multiTopicsConsumer = (MultiTopicsConsumerImpl<byte[]>) consumer;

        multiTopicsConsumer.subscribeAsync(topicNameWithNamespace, false).handle((res, exception) -> {
            assertTrue(exception instanceof PulsarClientException.AlreadyClosedException);
            assertEquals(exception.getMessage(), "Already subscribed to " + topicNameWithNamespace);
            return null;
        }).get();
        multiTopicsConsumer.subscribeAsync(topicNameWithDomain, false).handle((res, exception) -> {
            assertTrue(exception instanceof PulsarClientException.AlreadyClosedException);
            assertEquals(exception.getMessage(), "Already subscribed to " + topicNameWithDomain);
            return null;
        }).get();
        multiTopicsConsumer.subscribeAsync(localPartitionName, false).handle((res, exception) -> {
            assertTrue(exception instanceof PulsarClientException.AlreadyClosedException);
            assertEquals(exception.getMessage(), "Already subscribed to " + localPartitionName);
            return null;
        }).get();

        consumer.unsubscribe();
        consumer.close();
    }
}
