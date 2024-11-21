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

import static org.apache.pulsar.broker.BrokerTestUtil.receiveMessagesInThreads;
import com.google.common.collect.Sets;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.BiFunction;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;

public abstract class ProducerConsumerBase extends MockedPulsarServiceBaseTest {

    protected String methodName;

    @BeforeMethod(alwaysRun = true)
    public void beforeMethod(Method m) throws Exception {
        methodName = m.getName();
    }

    protected void producerBaseSetup() throws Exception {
        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        admin.tenants().createTenant("my-property",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("my-property/my-ns");
        admin.namespaces().setNamespaceReplicationClusters("my-property/my-ns", Sets.newHashSet("test"));

        // so that clients can test short names
        admin.tenants().createTenant("public",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("public/default");
        admin.namespaces().setNamespaceReplicationClusters("public/default", Sets.newHashSet("test"));
    }

    protected <T> void testMessageOrderAndDuplicates(Set<T> messagesReceived, T receivedMessage,
            T expectedMessage) {
        // Make sure that messages are received in order
        Assert.assertEquals(receivedMessage, expectedMessage,
                "Received message " + receivedMessage + " did not match the expected message " + expectedMessage);

        // Make sure that there are no duplicates
        Assert.assertTrue(messagesReceived.add(receivedMessage), "Received duplicate message " + receivedMessage);
    }

    private static final Random random = new Random();

    protected String newTopicName() {
        return "my-property/my-ns/topic-" + Long.toHexString(random.nextLong());
    }

    protected <T> ReceivedMessages<T> receiveAndAckMessages(
            BiFunction<MessageId, T, Boolean> ackPredicate,
            Consumer<T>...consumers) throws Exception {
        ReceivedMessages receivedMessages = new ReceivedMessages();
        receiveMessagesInThreads((consumer, msg) -> {
            T v = msg.getValue();
            MessageId messageId = msg.getMessageId();
            receivedMessages.messagesReceived.add(Pair.of(msg.getMessageId(), v));
            if (ackPredicate.apply(messageId, v)) {
                consumer.acknowledgeAsync(msg);
                receivedMessages.messagesAcked.add(Pair.of(msg.getMessageId(), v));
            }
            return true;
        }, Duration.ofSeconds(2), consumers);
        return receivedMessages;
    }

    protected <T> ReceivedMessages<T> ackAllMessages(Consumer<T>...consumers) throws Exception {
        return receiveAndAckMessages((msgId, msgV) -> true, consumers);
    }

    protected static class ReceivedMessages<T> {

        List<Pair<MessageId, T>> messagesReceived = Collections.synchronizedList(new ArrayList<>());

        List<Pair<MessageId, T>> messagesAcked = Collections.synchronizedList(new ArrayList<>());

        public boolean hasReceivedMessage(T v) {
            for (Pair<MessageId, T> pair : messagesReceived) {
                if (pair.getRight().equals(v)) {
                    return true;
                }
            }
            return false;
        }

        public boolean hasAckedMessage(T v) {
            for (Pair<MessageId, T> pair : messagesAcked) {
                if (pair.getRight().equals(v)) {
                    return true;
                }
            }
            return false;
        }
    }
}
