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
package org.apache.pulsar.broker.service;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.namespace.OwnedBundle;
import org.apache.pulsar.broker.namespace.OwnershipCache;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.apache.pulsar.metadata.impl.MetadataStoreSessionEventsForTest;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Fences the current ledger of one topic from a separate BookKeeper client, as the auto-recovery replication worker
 * does when it recovers an open ledger, and checks what happens to the other topics of the topic's bundle.
 */
@Test(groups = "broker")
public class FencedLedgerBundleOwnershipTest extends BkEnsemblesTestBase {

    public FencedLedgerBundleOwnershipTest() {
        super(2);
    }

    @Override
    protected void configurePulsar(ServiceConfiguration config) {
        // no rollover during the test, so that the topic's current ledger changes only when it gets fenced
        config.setManagedLedgerMaxEntriesPerLedger(50000);
        config.setManagedLedgerDefaultEnsembleSize(2);
        config.setManagedLedgerDefaultWriteQuorum(2);
        config.setManagedLedgerDefaultAckQuorum(2);
    }

    @Test(timeOut = 120_000)
    public void testFencedLedgerWhileTheMetadataSessionIsAliveAffectsOnlyItsTopic() throws Exception {
        String namespace = "prop/fenced-ledger-session-alive";
        admin.namespaces().createNamespace(namespace, 1);
        String fencedTopicName = "persistent://" + namespace + "/fenced";
        String otherTopicName = "persistent://" + namespace + "/other";
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(pulsar.getBrokerServiceUrl()).build();
        @Cleanup
        Producer<byte[]> fencedTopicProducer = client.newProducer().topic(fencedTopicName).create();
        @Cleanup
        Producer<byte[]> otherTopicProducer = client.newProducer().topic(otherTopicName).create();
        fencedTopicProducer.send("fenced-1".getBytes());
        otherTopicProducer.send("other-1".getBytes());

        PersistentTopic fencedTopic = getLoadedTopic(fencedTopicName);
        PersistentTopic otherTopic = getLoadedTopic(otherTopicName);
        NamespaceBundle bundle = getBundleOfBoth(fencedTopicName, otherTopicName);
        OwnershipCache ownershipCache = pulsar.getNamespaceService().getOwnershipCache();
        OwnedBundle ownedBundle = ownershipCache.getOwnedBundle(bundle);
        assertThat(ownedBundle).isNotNull();
        ClientCnx fencedTopicCnx = ((ProducerImpl<?>) fencedTopicProducer).getClientCnx();
        ClientCnx otherTopicCnx = ((ProducerImpl<?>) otherTopicProducer).getClientCnx();
        long fencedLedgerId = getCurrentLedgerId(fencedTopic);

        assertThat(pulsar.isMetadataSessionLost()).isFalse();
        fenceLedgerAsAutoRecovery(fencedLedgerId);

        // The fenced topic's next write finds the ledger fenced, checks it and switches to a new ledger
        fencedTopicProducer.send("fenced-2".getBytes());
        assertThat(getCurrentLedgerId(fencedTopic)).isNotEqualTo(fencedLedgerId);
        // without closing or reloading the topic, or disconnecting its producer
        assertThat(getLoadedTopic(fencedTopicName)).isSameAs(fencedTopic);
        assertThat(fencedTopic.getProducers()).hasSize(1);
        assertThat(((ProducerImpl<?>) fencedTopicProducer).getClientCnx()).isSameAs(fencedTopicCnx);

        // The bundle is still served, and its other topic is untouched
        assertThat(ownershipCache.getOwnedBundle(bundle)).isSameAs(ownedBundle);
        assertThat(ownedBundle.isActive()).isTrue();
        assertThat(getLoadedTopic(otherTopicName)).isSameAs(otherTopic);
        assertThat(otherTopic.getProducers()).hasSize(1);
        assertThat(((ProducerImpl<?>) otherTopicProducer).getClientCnx()).isSameAs(otherTopicCnx);
        otherTopicProducer.send("other-2".getBytes());
        assertThat(getLoadedTopic(otherTopicName)).isSameAs(otherTopic);
    }

    @Test(timeOut = 120_000)
    public void testFencedLedgerWhileTheMetadataSessionIsLostStopsServingTheBundle() throws Exception {
        String namespace = "prop/fenced-ledger-session-lost";
        admin.namespaces().createNamespace(namespace, 1);
        String fencedTopicName = "persistent://" + namespace + "/fenced";
        String otherTopicName = "persistent://" + namespace + "/other";
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(pulsar.getBrokerServiceUrl()).build();
        @Cleanup
        Producer<byte[]> fencedTopicProducer = client.newProducer().topic(fencedTopicName).create();
        @Cleanup
        Producer<byte[]> otherTopicProducer = client.newProducer().topic(otherTopicName).create();
        fencedTopicProducer.send("fenced-1".getBytes());
        otherTopicProducer.send("other-1".getBytes());

        PersistentTopic fencedTopic = getLoadedTopic(fencedTopicName);
        getLoadedTopic(otherTopicName);
        NamespaceBundle bundle = getBundleOfBoth(fencedTopicName, otherTopicName);
        OwnershipCache ownershipCache = pulsar.getNamespaceService().getOwnershipCache();
        assertThat(ownershipCache.getOwnedBundle(bundle)).isNotNull();
        long fencedLedgerId = getCurrentLedgerId(fencedTopic);

        MetadataStoreSessionEventsForTest.send(pulsar.getLocalMetadataStore(), SessionEvent.SessionLost);
        Awaitility.await().until(pulsar::isMetadataSessionLost);
        fenceLedgerAsAutoRecovery(fencedLedgerId);
        fencedTopicProducer.sendAsync("fenced-2".getBytes());

        // While the session is lost, the broker can't tell this fence from a takeover by another broker: it stops
        // serving the whole bundle and closes its other topic too
        Awaitility.await().untilAsserted(() -> {
            assertThat(ownershipCache.getOwnedBundle(bundle)).isNull();
            assertThat(pulsar.getBrokerService().getTopicReference(otherTopicName)).isEmpty();
        });

        // Once the session is reestablished, the next lookup acquires the bundle again
        MetadataStoreSessionEventsForTest.send(pulsar.getLocalMetadataStore(), SessionEvent.SessionReestablished);
        Awaitility.await().until(() -> !pulsar.isMetadataSessionLost());
        otherTopicProducer.send("other-2".getBytes());
        assertThat(ownershipCache.getOwnedBundle(bundle)).isNotNull();
    }

    private PersistentTopic getLoadedTopic(String topicName) {
        return (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).orElseThrow();
    }

    private NamespaceBundle getBundleOfBoth(String topicName, String otherTopicName) {
        NamespaceBundle bundle = pulsar.getNamespaceService().getBundle(TopicName.get(topicName));
        assertThat(pulsar.getNamespaceService().getBundle(TopicName.get(otherTopicName))).isEqualTo(bundle);
        return bundle;
    }

    private static long getCurrentLedgerId(PersistentTopic topic) {
        return ((ManagedLedgerImpl) topic.getManagedLedger()).getLastConfirmedEntry().getLedgerId();
    }

    /** Opens the ledger with recovery from a separate client, which fences it, as auto-recovery does. */
    private void fenceLedgerAsAutoRecovery(long ledgerId) throws Exception {
        ClientConfiguration conf = new ClientConfiguration()
                .setMetadataServiceUri("zk://127.0.0.1:" + bkEnsemble.getZookeeperPort() + "/ledgers");
        @Cleanup
        BookKeeper autoRecoveryClient = BookKeeper.forConfig(conf).build();
        @Cleanup
        ReadHandle recovered = autoRecoveryClient.newOpenLedgerOp().withLedgerId(ledgerId).withRecovery(true)
                .withDigestType(DigestType.CRC32C).withPassword(new byte[0]).execute().get(30, TimeUnit.SECONDS);
        assertThat(recovered.isClosed()).isTrue();
    }
}
