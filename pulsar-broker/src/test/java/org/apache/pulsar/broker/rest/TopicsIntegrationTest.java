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
package org.apache.pulsar.broker.rest;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Sets;
import lombok.Cleanup;
import org.apache.pulsar.broker.MultiBrokerBaseTest;
import org.apache.pulsar.broker.rest.base.RestClientImpl;
import org.apache.pulsar.broker.rest.base.api.RestClient;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.schema.StringSchema;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.websocket.data.ProducerMessage;
import org.apache.pulsar.websocket.data.ProducerMessages;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.List;
import java.util.concurrent.TimeUnit;
import static org.mockito.Mockito.spy;

public class TopicsIntegrationTest extends MultiBrokerBaseTest {

    private Topics topics;
    private final String testLocalCluster = "test";
    private final String testTenant = "my-tenant";
    private final String testNamespace = "my-namespace";
    private final String testTopicName = "my-topic";

    private RestClient restClient;

    protected void pulsarResourcesSetup() throws PulsarAdminException {
        topics = spy(new Topics());
        topics.setPulsar(pulsar);
        admin.clusters().createCluster(testLocalCluster, new ClusterDataImpl());
        admin.tenants().createTenant(testTenant, new TenantInfoImpl(Sets.newHashSet("role1", "role2"),
                Sets.newHashSet(testLocalCluster)));
        admin.namespaces().createNamespace(testTenant + "/" + testNamespace,
                Sets.newHashSet(testLocalCluster));
        ClientConfigurationData clientConfigurationData = new ClientConfigurationData();
        clientConfigurationData.setServiceUrl(pulsar.getWebServiceAddress());
        try {
            restClient = new RestClientImpl(pulsar.getWebServiceAddress(), clientConfigurationData);
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testProduceToNonPartitionedTopic() throws Exception {
        String topicName = "persistent://" + testTenant + "/" + testNamespace + "/testProduceToNonPartitionedTopic";
        Schema<String> schema = StringSchema.utf8();
        ProducerMessages producerMessages = new ProducerMessages();
        producerMessages.setKeySchema(ObjectMapperFactory.getThreadLocal().
                writeValueAsString(schema.getSchemaInfo()));
        producerMessages.setValueSchema(ObjectMapperFactory.getThreadLocal().
                writeValueAsString(schema.getSchemaInfo()));
        String message = "[" +
                "{\"key\":\"my-key\",\"payload\":\"RestProducer:1\",\"eventTime\":1603045262772,\"sequenceId\":1}," +
                "{\"key\":\"my-key\",\"payload\":\"RestProducer:2\",\"eventTime\":1603045262772,\"sequenceId\":2}," +
                "{\"key\":\"my-key\",\"payload\":\"RestProducer:3\",\"eventTime\":1603045262772,\"sequenceId\":3}]";
        producerMessages.setMessages(ObjectMapperFactory.getThreadLocal().readValue(message,
                new TypeReference<List<ProducerMessage>>() {}));
        restClient.producer().send(topicName, producerMessages);
        Assert.assertEquals(admin.topics().getInternalStats(topicName).currentLedgerEntries, 3);

        @Cleanup
        Consumer consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("my-sub")
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        Assert.assertNotNull(consumer.receive(3, TimeUnit.SECONDS).getData());
        Assert.assertNotNull(consumer.receive(3, TimeUnit.SECONDS).getData());
        Assert.assertNotNull(consumer.receive(3, TimeUnit.SECONDS).getData());
    }
}
