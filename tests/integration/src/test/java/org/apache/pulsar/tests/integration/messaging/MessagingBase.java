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
package org.apache.pulsar.tests.integration.messaging;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertEquals;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.tests.integration.suites.PulsarTestSuite;
import org.testng.annotations.BeforeMethod;

public abstract class MessagingBase extends PulsarTestSuite {

    private static final long RECEIVE_TIMEOUT_SECONDS = 30;

    protected String methodName;

    @BeforeMethod(alwaysRun = true)
    public void beforeMethod(Method m) throws Exception {
        methodName = m.getName();
    }

    protected String getNonPartitionedTopic(String topicPrefix, boolean isPersistent) throws Exception {
        String nsName = generateNamespaceName();
        createNamespace(nsName);

        return generateTopicName(nsName, topicPrefix, isPersistent);
    }

    protected String getPartitionedTopic(String topicPrefix, boolean isPersistent, int partitions) throws Exception {
        assertTrue(partitions > 0, "partitions must greater than 1");
        String nsName = generateNamespaceName();
        createNamespace(nsName);

        String topicName = generateTopicName(nsName, topicPrefix, isPersistent);
        pulsarAdmin.topics().createPartitionedTopic(topicName, partitions);
        return topicName;
    }

    // With the admin client: pulsar-admin in a broker container would start a JVM for every command
    private void createNamespace(String nsName) throws PulsarAdminException {
        pulsarAdmin.namespaces().createNamespace("public/" + nsName, Set.of(pulsarCluster.getClusterName()));
    }

    protected <T extends Comparable<T>> void receiveMessagesCheckOrderAndDuplicate
            (List<Consumer<T>> consumerList, int messagesToReceive)
            throws PulsarClientException, PulsarAdminException {
        Set<T> messagesReceived = Sets.newHashSet();
        Map<Consumer<T>, Map<String, Message<T>>> lastReceivedMaps = new HashMap<>();
        receiveMessages(consumerList, messagesToReceive, (consumer, currentReceived) -> {
            // Make sure that messages are received in the order they were sent. By sequence id: a non-persistent topic
            // gives all its messages the same message id
            Map<String, Message<T>> lastReceivedMap =
                    lastReceivedMaps.computeIfAbsent(consumer, k -> new HashMap<>());
            if (lastReceivedMap.containsKey(currentReceived.getTopicName())) {
                assertTrue(currentReceived.getSequenceId()
                                > lastReceivedMap.get(currentReceived.getTopicName()).getSequenceId(),
                        "Received messages are not in order.");
            }
            lastReceivedMap.put(currentReceived.getTopicName(), currentReceived);
            // Make sure that there are no duplicates
            assertTrue(messagesReceived.add(currentReceived.getValue()),
                    "Received duplicate message " + currentReceived.getValue());
        });
        assertEquals(messagesToReceive, messagesReceived.size());
    }

    protected <T> void receiveMessagesCheckDuplicate
            (List<Consumer<T>> consumerList, int messagesToReceive)
            throws PulsarClientException, PulsarAdminException {
        Set<T> messagesReceived = Sets.newHashSet();
        receiveMessages(consumerList, messagesToReceive, (consumer, currentReceived) -> {
            // Make sure that there are no duplicates
            assertTrue(messagesReceived.add(currentReceived.getValue()),
                    "Received duplicate message " + currentReceived.getValue());
        });
        assertEquals(messagesReceived.size(), messagesToReceive);
    }

    protected <T> void receiveMessagesCheckStickyKeyAndDuplicate
            (List<Consumer<T>> consumerList, int messagesToReceive)
            throws PulsarClientException, PulsarAdminException {
        Map<String, Set<String>> consumerKeys = Maps.newHashMap();
        Set<T> messagesReceived = Sets.newHashSet();
        receiveMessages(consumerList, messagesToReceive, (consumer, currentReceived) -> {
            assertNotNull(currentReceived.getKey());
            consumerKeys.putIfAbsent(consumer.getConsumerName(), Sets.newHashSet());
            consumerKeys.get(consumer.getConsumerName()).add(currentReceived.getKey());
            // Make sure that there are no duplicates
            assertTrue(messagesReceived.add(currentReceived.getValue()),
                    "Received duplicate message " + currentReceived.getValue());
        });
        // Make sure key will not be distributed to multiple consumers (except null key)
        Set<String> allKeys = Sets.newHashSet();
        consumerKeys.forEach((k, v) -> v.stream().filter(Objects::nonNull).forEach(key -> {
            assertTrue(allKeys.add(key),
                    "Key " + key + " is distributed to multiple consumers");
        }));
        assertEquals(messagesReceived.size(), messagesToReceive);
    }

    /**
     * Receives from the consumers in turn, acknowledging each message and passing it to {@code check}, until the
     * expected number of messages has arrived and the broker has none left for them: their subscriptions have no
     * backlog, so the broker has processed the acknowledgments and any later delivery would be a duplicate. A last
     * round that receives nothing makes sure that none was already on its way.
     */
    private <T> void receiveMessages(List<Consumer<T>> consumerList, int messagesToReceive,
                                     BiConsumer<Consumer<T>, Message<T>> check)
            throws PulsarClientException, PulsarAdminException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(RECEIVE_TIMEOUT_SECONDS);
        int received = 0;
        boolean noBacklog = false;
        while (true) {
            boolean receivedAny = false;
            for (Consumer<T> consumer : consumerList) {
                Message<T> message;
                while ((message = consumer.receive(100, TimeUnit.MILLISECONDS)) != null) {
                    consumer.acknowledge(message);
                    check.accept(consumer, message);
                    received++;
                    receivedAny = true;
                }
            }
            if (noBacklog && !receivedAny) {
                return;
            }
            if (System.nanoTime() - deadline > 0) {
                fail(received < messagesToReceive
                        ? "Received " + received + " of " + messagesToReceive + " messages"
                        : "The subscriptions still have a backlog after receiving " + received + " messages");
            }
            noBacklog = received >= messagesToReceive && hasNoBacklog(consumerList);
        }
    }

    private boolean hasNoBacklog(List<? extends Consumer<?>> consumerList) throws PulsarAdminException {
        for (Consumer<?> consumer : consumerList) {
            String topic = consumer.getTopic();
            TopicStats stats = pulsarAdmin.topics().getPartitionedTopicMetadata(topic).partitions > 0
                    ? pulsarAdmin.topics().getPartitionedStats(topic, false)
                    : pulsarAdmin.topics().getStats(topic);
            if (stats.getSubscriptions().get(consumer.getSubscription()).getMsgBacklog() > 0) {
                return false;
            }
        }
        return true;
    }

    protected <T> void closeConsumers(List<Consumer<T>> consumerList) throws PulsarClientException {
        Iterator<Consumer<T>> iterator = consumerList.iterator();
        while (iterator.hasNext()) {
            iterator.next().close();
            iterator.remove();
        }
    }
}
