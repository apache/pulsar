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
package org.apache.pulsar.tests.integration.messaging;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.tests.integration.suites.PulsarTestSuite;
import org.testng.annotations.BeforeMethod;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class MessagingBase extends PulsarTestSuite {

    protected String methodName;

    @BeforeMethod(alwaysRun = true)
    public void beforeMethod(Method m) throws Exception {
        methodName = m.getName();
    }

    protected String getNonPartitionedTopic(String topicPrefix, boolean isPersistent) throws Exception {
        String nsName = generateNamespaceName();
        pulsarCluster.createNamespace(nsName);

        return generateTopicName(nsName, topicPrefix, true);
    }

    protected String getPartitionedTopic(String topicPrefix, boolean isPersistent, int partitions) throws Exception {
        assertTrue(partitions > 0, "partitions must greater than 1");
        String nsName = generateNamespaceName();
        pulsarCluster.createNamespace(nsName);

        String topicName = generateTopicName(nsName, topicPrefix, true);
        pulsarCluster.createPartitionedTopic(topicName, partitions);
        return topicName;
    }

    protected <T extends Comparable<T>> void receiveMessagesCheckOrderAndDuplicate
            (List<Consumer<T>> consumerList, int messagesToReceive) throws PulsarClientException {
        Set<T> messagesReceived = Sets.newHashSet();
        for (Consumer<T> consumer : consumerList) {
            Message<T> currentReceived;
            Map<String, Message<T>> lastReceivedMap = new HashMap<>();
            while (true) {
                try {
                    currentReceived = consumer.receive(3, TimeUnit.SECONDS);
                } catch (PulsarClientException e) {
                    log.info("no more messages to receive for consumer {}", consumer.getConsumerName());
                    break;
                }
                // Make sure that messages are received in order
                if (currentReceived != null) {
                    consumer.acknowledge(currentReceived);
                    if (lastReceivedMap.containsKey(currentReceived.getTopicName())) {
                        assertTrue(currentReceived.getMessageId().compareTo(
                                lastReceivedMap.get(currentReceived.getTopicName()).getMessageId()) > 0,
                                "Received messages are not in order.");
                    }
                } else {
                    break;
                }
                lastReceivedMap.put(currentReceived.getTopicName(), currentReceived);
                // Make sure that there are no duplicates
                assertTrue(messagesReceived.add(currentReceived.getValue()),
                        "Received duplicate message " + currentReceived.getValue());
            }
        }
        assertEquals(messagesReceived.size(), messagesToReceive);
    }

    protected <T> void receiveMessagesCheckDuplicate
            (List<Consumer<T>> consumerList, int messagesToReceive) throws PulsarClientException {
        Set<T> messagesReceived = Sets.newHashSet();
        for (Consumer<T> consumer : consumerList) {
            Message<T> currentReceived = null;
            while (true) {
                try {
                    currentReceived = consumer.receive(3, TimeUnit.SECONDS);
                } catch (PulsarClientException e) {
                    log.info("no more messages to receive for consumer {}", consumer.getConsumerName());
                    break;
                }
                if (currentReceived != null) {
                    consumer.acknowledge(currentReceived);
                    // Make sure that there are no duplicates
                    assertTrue(messagesReceived.add(currentReceived.getValue()),
                            "Received duplicate message " + currentReceived.getValue());
                } else {
                    break;
                }
            }
        }
        assertEquals(messagesReceived.size(), messagesToReceive);
    }

    protected <T> void receiveMessagesCheckStickyKeyAndDuplicate
            (List<Consumer<T>> consumerList, int messagesToReceive) throws PulsarClientException {
        Map<String, Set<String>> consumerKeys = Maps.newHashMap();
        Set<T> messagesReceived = Sets.newHashSet();
        for (Consumer<T> consumer : consumerList) {
            Message<T> currentReceived;
            while (true) {
                try {
                    currentReceived = consumer.receive(3, TimeUnit.SECONDS);
                } catch (PulsarClientException e) {
                    log.info("no more messages to receive for consumer {}", consumer.getConsumerName());
                    break;
                }
                if (currentReceived != null) {
                    consumer.acknowledge(currentReceived);
                    assertNotNull(currentReceived.getKey());
                    consumerKeys.putIfAbsent(consumer.getConsumerName(), Sets.newHashSet());
                    consumerKeys.get(consumer.getConsumerName()).add(currentReceived.getKey());
                    // Make sure that there are no duplicates
                    assertTrue(messagesReceived.add(currentReceived.getValue()),
                            "Received duplicate message " + currentReceived.getValue());
                } else {
                    break;
                }
            }
        }
        // Make sure key will not be distributed to multiple consumers
        Set<String> allKeys = Sets.newHashSet();
        consumerKeys.forEach((k, v) -> v.forEach(key -> {
            assertTrue(allKeys.add(key),
                    "Key "+ key +  "is distributed to multiple consumers" );
        }));
        assertEquals(messagesReceived.size(), messagesToReceive);
    }

    protected <T> void closeConsumers(List<Consumer<T>> consumerList) throws PulsarClientException {
        Iterator<Consumer<T>> iterator = consumerList.iterator();
        while (iterator.hasNext()) {
            iterator.next().close();
            iterator.remove();
        }
    }
}
