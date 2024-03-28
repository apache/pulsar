/**
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

import com.carrotsearch.hppc.ObjectSet;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.common.naming.TopicName;
import org.awaitility.Awaitility;
import org.awaitility.reflect.WhiteboxImpl;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-api")
public class SimpleProducerConsumerMLInitializeDelayTest extends ProducerConsumerBase {

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

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        conf.setTopicLoadTimeoutSeconds(60 * 5);
    }

    @Test(timeOut = 30 * 1000)
    public void testConsumerListMatchesConsumerSet() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        final String subName = "sub";
        final int clientOperationTimeout = 3;
        final int loadMLDelayMillis = clientOperationTimeout * 3 * 1000;
        final int clientMaxBackoffSeconds = clientOperationTimeout * 2;
        admin.topics().createNonPartitionedTopic(topicName);
        // Create a client with a low operation timeout.
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(lookupUrl.toString())
                .operationTimeout(clientOperationTimeout, TimeUnit.SECONDS)
                .maxBackoffInterval(clientMaxBackoffSeconds, TimeUnit.SECONDS)
                .build();
        Consumer consumer = client.newConsumer()
                .topic(topicName)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();
        // Inject a delay for the initialization of ML, to make the consumer to register twice.
        // Consumer register twice: the first will be timeout, and try again.
        AtomicInteger delayTimes = new AtomicInteger();
        mockZooKeeper.delay(loadMLDelayMillis, (op, s) -> {
            if (op.toString().equals("GET") && s.contains(TopicName.get(topicName).getPersistenceNamingEncoding())) {
                return delayTimes.incrementAndGet() == 1;
            }
            return false;
        });
        admin.topics().unload(topicName);
        // Verify: at last, "dispatcher.consumers.size" equals "dispatcher.consumerList.size".
        Awaitility.await().atMost(Duration.ofSeconds(loadMLDelayMillis * 3))
                .ignoreExceptions().untilAsserted(() -> {
            Dispatcher dispatcher = pulsar.getBrokerService()
                            .getTopic(topicName, false).join().get()
                            .getSubscription(subName).getDispatcher();
            ObjectSet consumerSet = WhiteboxImpl.getInternalState(dispatcher, "consumerSet");
            List consumerList = WhiteboxImpl.getInternalState(dispatcher, "consumerList");
            log.info("consumerSet_size: {}, consumerList_size: {}", consumerSet.size(), consumerList.size());
            Assert.assertEquals(consumerList.size(), 1);
            Assert.assertEquals(consumerSet.size(), 1);
        });

        // Verify: the topic can be deleted.
        consumer.close();
        admin.topics().delete(topicName);
        // cleanup.
        client.close();
    }
}
