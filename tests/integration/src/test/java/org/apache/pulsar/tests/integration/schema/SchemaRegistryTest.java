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
package org.apache.pulsar.tests.integration.schema;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.tests.integration.messaging.TopicMessagingBase;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class SchemaRegistryTest extends TopicMessagingBase {
    protected String methodName;
    private String SCHEMA_REGISTRY="org.apache.pulsar.broker.service.schema.MockSchemaRegistry";
    private String SCHEMA_STORAGE="org.apache.pulsar.broker.service.schema.MockSchemaStorage";

    @Override
    public void beforeStartCluster() throws Exception {
        super.beforeStartCluster();
        pulsarCluster.getSpec().schemaRegistryStorageClassName(SCHEMA_STORAGE);
        pulsarCluster.getSpec().schemaRegistryClassName(SCHEMA_REGISTRY);
    }

    @BeforeMethod(alwaysRun = true)
    public void beforeMethod(Method m) throws Exception {
        methodName = m.getName();
    }

    @Test
    protected void verifySchemaRegistryClass() throws Exception {
        String schemaRegistryClass = pulsarCluster.getSpec().schemaRegistryClassName();
        String schemaStorageClass = pulsarCluster.getSpec().schemaRegistryStorageClassName();

        assertEquals(schemaRegistryClass, SCHEMA_REGISTRY);
        assertEquals(schemaStorageClass, SCHEMA_STORAGE);
    }

    @Test(dataProvider = "ServiceUrls")
    protected void useMockSchemaRegistry(Supplier<String> serviceUrl) throws Exception {
        log.info("-- Starting {} test --", methodName);
        final String topicName = getPartitionedTopic("test-custom-schema-registry", true, 3);
        @Cleanup
        final PulsarClient client = PulsarClient.builder()
                .serviceUrl(serviceUrl.get())
                .build();
        @Cleanup
        final Consumer<MockPojo> consumer = client.newConsumer(Schema.JSON(MockPojo.class))
                .topic(topicName)
                .subscriptionName("test-sub")
                .subscriptionType(SubscriptionType.Exclusive)
                .subscribe();
        try {
            client.newConsumer(Schema.JSON(MockPojo.class))
                    .topic(topicName)
                    .subscriptionName("test-sub")
                    .subscriptionType(SubscriptionType.Exclusive)
                    .subscribe();
            fail("should be failed");
        } catch (PulsarClientException ignore) {
        }
        final int messagesToSend = 10;
        final String producerName = "producerForExclusive";
        @Cleanup
        final Producer<MockPojo> producer = client.newProducer(Schema.JSON(MockPojo.class))
                .topic(topicName)
                .enableBatching(false)
                .producerName(producerName)
                .create();
        for (int i = 0; i < messagesToSend; i++) {
            MessageId messageId = producer.newMessage().value(new MockPojo(producer.getProducerName() + "-" + i, i)).send();
            assertNotNull(messageId);
        }
        log.info("public messages complete.");
        receiveMessagesCheckOrderAndDuplicate(Collections.singletonList(consumer), messagesToSend);
        log.info("-- Exiting {} test --", methodName);
    }
}
