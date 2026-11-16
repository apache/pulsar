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
package org.apache.pulsar.broker.transaction.coordinator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.transaction.TransactionTestBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.ServiceUrlProvider;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException.CoordinatorNotFoundException;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TransactionMetaStoreAssignmentTest extends TransactionTestBase {

    @Override
    @BeforeMethod(alwaysRun = true)
    protected void setup() throws Exception {
        setUpBase(3, 16, null, 0);
        pulsarClient.close();
    }

    @Test
    public void testTransactionMetaStoreAssignAndFailover() throws Exception {

        pulsarClient = buildClient();

        checkTransactionCoordinatorNum(16);

        pulsarClient.close();
        PulsarService crashedMetaStore = null;
        for (int i = pulsarServiceList.size() - 1; i >= 0; i--) {
            if (pulsarServiceList.get(i).getTransactionMetadataStoreService().getStores().size() > 0) {
                crashedMetaStore = pulsarServiceList.get(i);
                break;
            }
        }

        Assert.assertNotNull(crashedMetaStore);
        pulsarServiceList.remove(crashedMetaStore);
        crashedMetaStore.close();

        pulsarClient.close();
        pulsarClient = buildClient();
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    int transactionMetaStoreCount2 = pulsarServiceList.stream()
                            .mapToInt(pulsarService -> pulsarService.getTransactionMetadataStoreService()
                                    .getStores().size()).sum();
                    Assert.assertEquals(transactionMetaStoreCount2, 16);
                });
        pulsarClient.close();
    }

    @Test
    public void testTransactionMetaStoreUnload() throws Exception {

        pulsarClient = buildClient();
        checkTransactionCoordinatorNum(16);

        // close pulsar client will not init tc again
        pulsarClient.close();

        admin.topics().unload(SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN.toString());

        for (int i = 0; i < 16; i++) {
            final int f = i;
            pulsarServiceList.forEach((pulsarService) -> pulsarService
                    .getTransactionMetadataStoreService()
                    .removeTransactionMetadataStore(TransactionCoordinatorID.get(f)));
        }
        checkTransactionCoordinatorNum(0);
        pulsarClient = buildClient();
        checkTransactionCoordinatorNum(16);

        pulsarClient.close();

    }

    @Test
    public void testScalableHandshakeRequiresElectedLeader() throws Exception {
        PulsarService leader = awaitTcZeroLeader();
        PulsarService bundleOwner = pulsarServiceList.stream().filter(broker -> broker != leader).findFirst()
                .orElseThrow();
        var assignTopic = SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN.getPartition(0);
        if (!admin.lookups().lookupTopic(assignTopic.toString()).equals(bundleOwner.getBrokerServiceUrl())) {
            String bundle = admin.lookups().getBundleRange(assignTopic.toString());
            admin.namespaces().unloadNamespaceBundle(assignTopic.getNamespace(), bundle, bundleOwner.getBrokerId());
        }
        Awaitility.await().untilAsserted(() -> {
            assertThat(admin.lookups().lookupTopic(assignTopic.toString()))
                    .isEqualTo(bundleOwner.getBrokerServiceUrl());
            assertThat(bundleOwner.getNamespaceService().isServiceUnitOwned(assignTopic)).isTrue();
        });
        assertThat(bundleOwner.getTransactionCoordinatorV5().isLeaderFor(0)).isFalse();

        PulsarClientImpl client = (PulsarClientImpl) leader.getClient();
        connectToTcZero(client, leader, true);
        connectToTcZero(client, bundleOwner, false);
        assertThatThrownBy(() -> connectToTcZero(client, bundleOwner, true))
                .as("v5 handshake on the assign-topic bundle owner")
                .hasCauseInstanceOf(CoordinatorNotFoundException.class);

        leader.getTransactionCoordinatorV5().close();
        assertThatThrownBy(() -> connectToTcZero(client, leader, true))
                .hasCauseInstanceOf(CoordinatorNotFoundException.class);
        PulsarService nextLeader = awaitTcZeroLeader();
        assertThat(nextLeader).isNotSameAs(leader);
        connectToTcZero(client, nextLeader, true);
    }

    private PulsarService awaitTcZeroLeader() {
        return Awaitility.await().until(() -> pulsarServiceList.stream()
                .filter(broker -> broker.getTransactionCoordinatorV5().isLeaderFor(0))
                .findFirst().orElse(null), Objects::nonNull);
    }

    private static void connectToTcZero(PulsarClientImpl client, PulsarService broker, boolean scalable)
            throws Exception {
        var address = InetSocketAddress.createUnresolved(broker.getAdvertisedAddress(),
                broker.getBrokerListenPort().orElseThrow());
        var cnx = client.getCnxPool().getConnection(address).get(10, TimeUnit.SECONDS);
        long requestId = client.newRequestId();
        cnx.sendScalableSessionRequest(Commands.newTcClientConnectRequest(0, requestId, scalable), requestId)
                .get(10, TimeUnit.SECONDS);
    }

    private void checkTransactionCoordinatorNum(int number) {
        Awaitility.await()
                .untilAsserted(() -> {
                    int transactionMetaStoreCount = pulsarServiceList.stream()
                            .mapToInt(pulsarService -> pulsarService.getTransactionMetadataStoreService()
                                    .getStores().size()).sum();
                    Assert.assertEquals(transactionMetaStoreCount, number);
                });
    }

    @SuppressWarnings({"try", "deprecation"})
    private PulsarClient buildClient() throws Exception {
        return PulsarClient.builder()
                .serviceUrlProvider(new ServiceUrlProvider() {
                    final AtomicInteger atomicInteger = new AtomicInteger();
                    @Override
                    public void initialize(PulsarClient client) {

                    }

                    @Override
                    public String getServiceUrl() {
                        return pulsarServiceList.get(atomicInteger.getAndIncrement()
                                % pulsarServiceList.size()).getBrokerServiceUrl();
                    }
                })
                .statsInterval(0, TimeUnit.SECONDS)
                .enableTransaction(true)
                .build();
    }
    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }
}
