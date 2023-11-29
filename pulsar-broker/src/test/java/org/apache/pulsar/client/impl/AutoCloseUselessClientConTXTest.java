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
package org.apache.pulsar.client.impl;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-impl")
public class AutoCloseUselessClientConTXTest extends AutoCloseUselessClientConSupports {

    private static final String topicName = UUID.randomUUID().toString().replaceAll("-","");
    private static final String topicFullName = "persistent://public/default/" + topicName;

    @BeforeMethod
    public void before() throws PulsarAdminException, MetadataStoreException {
        // Create Topics
        PulsarAdmin pulsarAdmin_0 = super.getAllAdmins().get(0);
        List<String> topicList_defaultNamespace = pulsarAdmin_0.topics().getList("public/default");
        if (!topicList_defaultNamespace.contains(topicName)
                && !topicList_defaultNamespace.contains(topicFullName + "-partition-0")
                && !topicList_defaultNamespace.contains(topicFullName)){
            pulsarAdmin_0.topics().createNonPartitionedTopic(topicFullName);
        }
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        updateConfig(conf);
    }

    @Override
    protected ServiceConfiguration createConfForAdditionalBroker(int additionalBrokerIndex) {
        ServiceConfiguration conf = super.createConfForAdditionalBroker(additionalBrokerIndex);
        updateConfig(conf);
        return conf;
    }

    /**
     * Override for make client enable transaction.
     */
    @Override
    protected PulsarClient createNewPulsarClient(ClientBuilder clientBuilder) throws PulsarClientException {
        try {
            if (!admin.clusters().getClusters().contains("test")){
                admin.clusters().createCluster("test", ClusterData.builder().build());
            }
            if (!admin.tenants().getTenants().contains("pulsar")){
                admin.tenants().createTenant("pulsar",
                        TenantInfo.builder().allowedClusters(Collections.singleton("test")).build());
            }
            if (!admin.namespaces().getNamespaces("pulsar").contains("pulsar/system")) {
                admin.namespaces().createNamespace("pulsar/system");
            }

            if (conf.isTransactionCoordinatorEnabled()) {
                if (!pulsar.getPulsarResources()
                        .getNamespaceResources()
                        .getPartitionedTopicResources()
                        .partitionedTopicExists(SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN)){
                    pulsar.getPulsarResources()
                            .getNamespaceResources()
                            .getPartitionedTopicResources()
                            .createPartitionedTopic(SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN,
                                    new PartitionedTopicMetadata(2));
                }
            }
        } catch (Exception e){
            log.warn("create namespace failure", e);
        }
        return clientBuilder.enableTransaction(true).build();
    }

    /**
     * Override for make broker enable transaction.
     */
    private void updateConfig(ServiceConfiguration conf) {
        this.conf.setTransactionCoordinatorEnabled(true);
        this.conf.setSystemTopicEnabled(true);
        this.conf.setTopicLevelPoliciesEnabled(true);
        conf.setTransactionCoordinatorEnabled(true);
        conf.setSystemTopicEnabled(true);
        conf.setTopicLevelPoliciesEnabled(true);
    }

    @Test
    public void testConnectionAutoReleaseUnPartitionedTopicWithTransaction() throws Exception {
        PulsarClientImpl pulsarClient = (PulsarClientImpl) super.getAllClients().get(0);
        // Init clients
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName("my-subscription-x")
                .subscribe();
        Producer<byte[]> producer = pulsarClient.newProducer()
                .sendTimeout(0, TimeUnit.SECONDS)
                .topic(topicName)
                .create();
        // Ensure transaction works
        ensureTransactionWorks(pulsarClient, producer, consumer);
        // Connection to every Broker
        connectionToEveryBroker(pulsarClient);
        Assert.assertTrue(pulsarClient.getCnxPool().getPoolSize() >= 5);
        // Assert "auto release works"
        trigReleaseConnection(pulsarClient);
        Awaitility.waitAtMost(Duration.ofSeconds(5)).until(()-> {
            // Wait for async task done, then assert auto release success
            return pulsarClient.getCnxPool().getPoolSize() <= 3;
        });
        // Ensure transaction works
        ensureTransactionWorks(pulsarClient, producer, consumer);
        // Verify that the number of connections did not increase after the work was completed
        Assert.assertTrue(pulsarClient.getCnxPool().getPoolSize() <= 3);
        consumer.close();
        producer.close();
    }
}
