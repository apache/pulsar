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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ReaderMessageTTLTest extends SharedPulsarBaseTest {

    @DataProvider
    public Object[][] expiryPaths() {
        return new Object[][] {{true}, {false}};
    }

    @Test(dataProvider = "expiryPaths", timeOut = 60000)
    public void testReaderRetainsUnreadMessagesDuringExpiry(boolean sharedPosition) throws Exception {
        String topicName = newTopicName();
        admin.namespaces().setRetention(getNamespace(), new RetentionPolicies(-1, -1));
        admin.namespaces().setNamespaceMessageTTL(getNamespace(), 1);
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().createSubscription(topicName, "durable", MessageId.earliest);
        int messageCount = 20;
        try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName)
                .enableBatching(false).create()) {
            for (int i = 0; i < messageCount; i++) {
                producer.send("message-" + i);
            }
        }
        try (Reader<String> reader = pulsarClient.newReader(Schema.STRING).topic(topicName)
                .subscriptionName("reader").startMessageId(MessageId.earliest).receiverQueueSize(1).create()) {
            Message<String> first = reader.readNext(10, TimeUnit.SECONDS);
            assertThat(first).isNotNull();
            assertThat(first.getValue()).isEqualTo("message-0");
            PersistentTopic topic = (PersistentTopic) getTopicReference(topicName).orElseThrow();
            PersistentSubscription readerSubscription = topic.getSubscription("reader");
            assertThat(readerSubscription.getCursor().isDurable()).isFalse();
            Position readPosition = readerSubscription.getCursor().getReadPosition();
            PersistentSubscription durable = topic.getSubscription("durable");

            // Pause the reader until all published messages are old enough to expire.
            long expiryTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(2);
            Awaitility.await().until(() -> System.currentTimeMillis() > expiryTime);
            PersistentTopic expiryTopic = topic;
            if (!sharedPosition) {
                expiryTopic = spy(topic);
                // Exercise the fallback used by custom ManagedLedger implementations.
                doReturn(mock(ManagedLedger.class, delegatesTo(topic.getManagedLedger())))
                        .when(expiryTopic).getManagedLedger();
            }
            PersistentTopic topicToCheck = expiryTopic;
            Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
                topicToCheck.checkMessageExpiry();
                assertThat(durable.getCursor().getNumberOfEntriesInBacklog(false)).isZero();
            });
            assertThat(readerSubscription.getCursor().getMarkDeletedPosition())
                    .as("TTL must not acknowledge unread reader messages")
                    .isLessThan(readPosition);
            for (int i = 1; i < messageCount; i++) {
                Message<String> message = reader.readNext(10, TimeUnit.SECONDS);
                assertThat(message).as("retained message %s", i).isNotNull();
                assertThat(message.getValue()).isEqualTo("message-" + i);
            }
            assertThat(reader.hasMessageAvailable()).isFalse();
        }
    }
}
