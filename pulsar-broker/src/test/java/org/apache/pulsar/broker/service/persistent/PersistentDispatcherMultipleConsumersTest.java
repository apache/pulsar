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
package org.apache.pulsar.broker.service.persistent;

import com.carrotsearch.hppc.ObjectSet;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.awaitility.reflect.WhiteboxImpl;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-api")
public class PersistentDispatcherMultipleConsumersTest extends ProducerConsumerBase {

    @BeforeClass(alwaysRun = true)
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

    @Test(timeOut = 30 * 1000)
    public void testTopicDeleteIfConsumerSetMismatchConsumerList() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        final String subscription = "s1";
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().createSubscription(topicName, subscription, MessageId.earliest);

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(topicName).subscriptionName(subscription)
                .subscriptionType(SubscriptionType.Shared).subscribe();
        // Make an error that "consumerSet" is mismatch with "consumerList".
        Dispatcher dispatcher = pulsar.getBrokerService()
                .getTopic(topicName, false).join().get()
                .getSubscription(subscription).getDispatcher();
        ObjectSet<org.apache.pulsar.broker.service.Consumer> consumerSet =
                WhiteboxImpl.getInternalState(dispatcher, "consumerSet");
        List<org.apache.pulsar.broker.service.Consumer> consumerList =
                WhiteboxImpl.getInternalState(dispatcher, "consumerList");

        org.apache.pulsar.broker.service.Consumer serviceConsumer = consumerList.get(0);
        consumerSet.add(serviceConsumer);
        consumerList.add(serviceConsumer);

        // Verify: the topic can be deleted successfully.
        consumer.close();
        admin.topics().delete(topicName, false);
    }

    @Test(timeOut = 30 * 1000)
    public void testTopicDeleteIfConsumerSetMismatchConsumerList2() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        final String subscription = "s1";
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().createSubscription(topicName, subscription, MessageId.earliest);

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(topicName).subscriptionName(subscription)
                .subscriptionType(SubscriptionType.Shared).subscribe();
        // Make an error that "consumerSet" is mismatch with "consumerList".
        Dispatcher dispatcher = pulsar.getBrokerService()
                .getTopic(topicName, false).join().get()
                .getSubscription(subscription).getDispatcher();
        ObjectSet<org.apache.pulsar.broker.service.Consumer> consumerSet =
                WhiteboxImpl.getInternalState(dispatcher, "consumerSet");
        consumerSet.clear();

        // Verify: the topic can be deleted successfully.
        consumer.close();
        admin.topics().delete(topicName, false);
    }
}
