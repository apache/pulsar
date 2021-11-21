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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.testng.annotations.Test;

@Slf4j
public class ReaderMessagingTest extends MessagingBase {

    @Test(dataProvider = "ServiceAndAdminUrls")
    public void testReaderReconnectAndRead(Supplier<String> serviceUrl, Supplier<String> adminUrl) throws Exception {
        log.info("-- Starting {} test --", methodName);
        final String topicName = getNonPartitionedTopic("test-reader-reconnect-read", false);
        @Cleanup final PulsarClient client = PulsarClient.builder()
                .serviceUrl(serviceUrl.get())
                .build();
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
            assertNotNull(messageId);
        }

        for (int i = 0; i < messagesToSend; i++) {
            Message<String> msg = reader.readNext();
            assertEquals(msg.getValue(), "message-" + i);
        }

        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder()
                .serviceHttpUrl(adminUrl.get())
                .build();

        admin.topics().unload(topicName);

        for (int i = 0; i < messagesToSend; i++) {
            MessageId messageId = producer.newMessage().value("message-" + i).send();
            assertNotNull(messageId);
        }

        for (int i = 0; i < messagesToSend; i++) {
            Message<String> msg = reader.readNext();
            assertEquals(msg.getValue(), "message-" + i);
        }

        log.info("-- Exiting {} test --", methodName);
    }

    @Test(dataProvider = "ServiceAndAdminUrls")
    public void testReaderReconnectAndReadBatchMessages(Supplier<String> serviceUrl, Supplier<String> adminUrl)
            throws Exception {
        log.info("-- Starting {} test --", methodName);
        final String topicName = getNonPartitionedTopic("test-reader-reconnect-read-batch", false);
        @Cleanup final PulsarClient client = PulsarClient.builder()
                .serviceUrl(serviceUrl.get())
                .build();
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
            assertNotNull(messageId);
        }

        for (int i = 0; i < messagesToSend; i++) {
            Message<String> msg = reader.readNext();
            assertEquals(msg.getValue(), "message-" + i);
        }

        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder()
                .serviceHttpUrl(adminUrl.get())
                .build();

        admin.topics().unload(topicName);

        for (int i = 0; i < messagesToSend; i++) {
            MessageId messageId = producer.newMessage().value("message-" + i).send();
            assertNotNull(messageId);
        }

        for (int i = 0; i < messagesToSend; i++) {
            Message<String> msg = reader.readNext();
            assertEquals(msg.getValue(), "message-" + i);
        }

        log.info("-- Exiting {} test --", methodName);
    }
}
