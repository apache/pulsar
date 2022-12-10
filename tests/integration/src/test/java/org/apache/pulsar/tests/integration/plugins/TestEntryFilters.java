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
package org.apache.pulsar.tests.integration.plugins;

import static org.testng.Assert.assertNotNull;
import lombok.Cleanup;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.tests.integration.messaging.TopicMessagingBase;
import org.testng.annotations.Test;
import java.util.Collections;
import java.util.function.Supplier;

public class TestEntryFilters extends TopicMessagingBase {

    private static final String PREFIX = "PULSAR_PREFIX_";

    @Override
    public void setupCluster() throws Exception {
        brokerEnvs.put(PREFIX + "entryFilterNames", "pattern_filter");
        brokerEnvs.put(PREFIX + "entryFiltersDirectory", "/pulsar/examples");
        super.setupCluster();
    }

    @Test(dataProvider = "ServiceUrls")
    public void test(Supplier<String> serviceUrlSupplier) throws Exception {
        String serviceUrl = serviceUrlSupplier.get();

        final String topicName = getNonPartitionedTopic("filtered-topic", true);
        @Cleanup
        final PulsarClient client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .build();

        String evenPattern = "^[a-z]+-\\d*[02468]$";

        @Cleanup
        final Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(topicName)
                .enableBatching(false)
                .producerName("producer")
                .create();
        int messagesToSend = 20;
        for (int i = 0; i < messagesToSend; i++) {
            String messageValue = producer.getProducerName() + "-" + i;
            MessageId messageId = producer.newMessage()
                    .value(messageValue)
                    .property("filter_property", messageValue)
                    .send();
            assertNotNull(messageId);
        }

        try (Consumer<String> consumer = createConsumer(client, topicName, evenPattern)) {
            receiveMessagesCheckOrderAndDuplicate(Collections.singletonList(consumer), messagesToSend / 2);

        }
        try(Consumer<String> consumer = createConsumer(client, topicName, null)) {
            receiveMessagesCheckOrderAndDuplicate(Collections.singletonList(consumer), messagesToSend);

        }
    }

    private Consumer<String> createConsumer(
            PulsarClient client, String topicName, String filterPattern) throws Exception {
        ConsumerBuilder<String> builder = client.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(randomName(8))
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest);
        if (filterPattern != null) {
                builder.subscriptionProperties(Collections.singletonMap("entry_filter_pattern", filterPattern));
        }
        return builder.subscribe();
    }

}
