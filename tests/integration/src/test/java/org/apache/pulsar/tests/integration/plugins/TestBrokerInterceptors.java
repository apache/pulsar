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
import static org.testng.Assert.assertTrue;
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
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.testng.annotations.Test;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;


public class TestBrokerInterceptors extends TopicMessagingBase {

    private static final String PREFIX = "PULSAR_PREFIX_";

    @Override
    public void setupCluster() throws Exception {
        brokerEnvs.put(PREFIX + "disableBrokerInterceptors", "false");
        brokerEnvs.put(PREFIX + "brokerInterceptorsDirectory", "/pulsar/examples");
        brokerEnvs.put(PREFIX + "brokerInterceptors", "loggingInterceptor");
        super.setupCluster();
    }

    @Override
    protected PulsarClusterSpec.PulsarClusterSpecBuilder beforeSetupCluster(String clusterName,
                                                                            PulsarClusterSpec.PulsarClusterSpecBuilder specBuilder) {
        specBuilder.numBrokers(1);
        return specBuilder;
    }

    @Test(dataProvider = "ServiceUrls")
    public void test(Supplier<String> serviceUrlSupplier) throws Exception {
        String serviceUrl = serviceUrlSupplier.get();

        final String topicName = getNonPartitionedTopic("interceptorTest-topic", true);
        @Cleanup
        final PulsarClient client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .build();

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
                    .send();
            assertNotNull(messageId);
        }

        try (Consumer<String> consumer = createConsumer(client, topicName)) {
            for (int i = 0; i < messagesToSend; ++i) {
                consumer.receive(3, TimeUnit.SECONDS);
            }
        }

        String log = pulsarCluster.getAnyBroker()
                .execCmd("cat", "/var/log/pulsar/broker.log").getStdout();

        for (String line : new String[]{
                "initialize: OK",
                "onConnectionCreated",
                "producerCreated",
                "consumerCreated",
                "messageProduced",
                "beforeSendMessage: OK",
        }) {
            assertTrue(log.contains("LoggingBrokerInterceptor - " + line), "Log did not contain line '" + line + "'");
        }

    }

    private Consumer<String> createConsumer(PulsarClient client, String topicName) throws Exception {
        ConsumerBuilder<String> builder = client.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(randomName(8))
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest);
        return builder.subscribe();
    }
}
