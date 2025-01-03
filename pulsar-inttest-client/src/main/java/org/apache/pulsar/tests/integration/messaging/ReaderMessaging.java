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

import static org.apache.pulsar.tests.integration.utils.IntegTestUtils.getNonPartitionedTopic;
import static org.assertj.core.api.Assertions.assertThat;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.tests.integration.IntegTest;

@Slf4j
public class ReaderMessaging extends IntegTest {

    public ReaderMessaging(PulsarClient client, PulsarAdmin admin) {
        super(client, admin);
    }

    public void testReaderReconnectAndRead() throws Exception {
        log.info("-- Starting testReaderReconnectAndRead test --");
        final String topicName = getNonPartitionedTopic(admin, "test-reader-reconnect-read", false);
        @Cleanup final Reader<String> reader = client.newReader(Schema.STRING)
                .topic(topicName)
                .subscriptionName("test-sub")
                // Here we need to make sure that setting the startMessageId should not cause a change in the
                // behavior of the reader under non.
                .startMessageId(MessageId.earliest)
                .create();

        final int messagesToSend = 10;
        @Cleanup final Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(topicName)
                .enableBatching(false)
                .create();
        for (int i = 0; i < messagesToSend; i++) {
            MessageId messageId = producer.newMessage().value("message-" + i).send();
            assertThat(messageId).isNotNull();
        }

        for (int i = 0; i < messagesToSend; i++) {
            Message<String> msg = reader.readNext();
            assertThat(msg.getValue()).isEqualTo("message-" + i);
        }

        admin.topics().unload(topicName);

        for (int i = 0; i < messagesToSend; i++) {
            MessageId messageId = producer.newMessage().value("message-" + i).send();
            assertThat(messageId).isNotNull();
        }

        for (int i = 0; i < messagesToSend; i++) {
            Message<String> msg = reader.readNext();
            assertThat(msg.getValue()).isEqualTo("message-" + i);
        }

        log.info("-- Exiting testReaderReconnectAndRead test --");
    }

    public void testReaderReconnectAndReadBatchMessages()
            throws Exception {
        log.info("-- Starting testReaderReconnectAndReadBatchMessages test --");
        final String topicName = getNonPartitionedTopic(admin, "test-reader-reconnect-read-batch", false);
        @Cleanup final Reader<String> reader = client.newReader(Schema.STRING)
                .topic(topicName)
                .subscriptionName("test-sub")
                // Here we need to make sure that setting the startMessageId should not cause a change in the
                // behavior of the reader under non.
                .startMessageId(MessageId.earliest)
                .create();

        final int messagesToSend = 10;
        @Cleanup final Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(topicName)
                .enableBatching(true)
                .batchingMaxPublishDelay(5, TimeUnit.SECONDS)
                .batchingMaxMessages(5)
                .create();

        for (int i = 0; i < messagesToSend; i++) {
            MessageId messageId = producer.newMessage().value("message-" + i).send();
            assertThat(messageId).isNotNull();
        }

        for (int i = 0; i < messagesToSend; i++) {
            Message<String> msg = reader.readNext();
            assertThat(msg.getValue()).isEqualTo("message-" + i);
        }

        admin.topics().unload(topicName);

        for (int i = 0; i < messagesToSend; i++) {
            MessageId messageId = producer.newMessage().value("message-" + i).send();
            assertThat(messageId).isNotNull();
        }

        for (int i = 0; i < messagesToSend; i++) {
            Message<String> msg = reader.readNext();
            assertThat(msg.getValue()).isEqualTo("message-" + i);
        }

        log.info("-- Exiting testReaderReconnectAndReadBatchMessages test --");
    }
}
