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
package org.apache.pulsar.broker.resourcegroup;

import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
public class StallSubscribeTest extends ProducerConsumerBase {
    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        this.prepareData();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testProduceConsumeUsageOnRG() throws Exception {
        testProduceConsumeUsageOnRG(PRODUCE_CONSUME_PERSISTENT_TOPIC);
    }

    private void createTopic(String topic) {
        BrokerService bs = this.pulsar.getBrokerService();
        bs.getOrCreateTopic(topic);
    }

    private void testProduceConsumeUsageOnRG(String topicString) throws Exception {
        createTopic(topicString);
        Consumer<byte[]> consumer = null;
        try {
            consumer = pulsarClient.newConsumer()
                    .topic(topicString)
                    .subscriptionName("my-subscription")
                    .subscriptionType(SubscriptionType.Shared)
                    .subscribe();
        } catch (PulsarClientException p) {
            final String errMesg = String.format("Got exception while building consumer: ex={}", p.getMessage());
            Assert.assertTrue(false, errMesg);
        }

        log.info("Consumer created");
    }

    final String TenantName = "tenant";
    final String NsName = "namespace";
    final String TenantAndNsName = TenantName + "/" + NsName;
    final String TestProduceConsumeTopicName = "/prod-cons-topic";
    final String PRODUCE_CONSUME_PERSISTENT_TOPIC = "persistent://" + TenantAndNsName + TestProduceConsumeTopicName;

    private void prepareData() throws PulsarAdminException {
        this.conf.setAllowAutoTopicCreation(true);

        final String clusterName = "test";
        admin.clusters().createCluster(clusterName, ClusterData.builder().serviceUrl(brokerUrl.toString()).build());
        admin.tenants().createTenant(TenantName,
                    new TenantInfoImpl(Sets.newHashSet("fakeAdminRole"), Sets.newHashSet(clusterName)));
        admin.namespaces().createNamespace(TenantAndNsName);
        admin.namespaces().setNamespaceReplicationClusters(TenantAndNsName, Sets.newHashSet(clusterName));
    }
}
