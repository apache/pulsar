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
package org.apache.pulsar.client.api;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BkEnsemblesTestBase;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Uses a private real BookKeeper ensemble: stopping every bookie must not affect the shared test cluster.
 */
@Test(groups = "broker-api")
public class ClientDeduplicationRecoveryTest extends BkEnsemblesTestBase {
    private static final int WINDOW_SIZE = 8;
    private static final int SEND_TIMEOUT_SECONDS = 60;

    @Override
    protected void configurePulsar(ServiceConfiguration config) {
        config.setManagedLedgerDefaultEnsembleSize(3);
        config.setManagedLedgerDefaultWriteQuorum(3);
        config.setManagedLedgerDefaultAckQuorum(2);
        // Keep the initial ledger open so the outage exercises an actual failed add, not only ledger creation.
        config.setManagedLedgerMaxEntriesPerLedger(1000);
        config.setBookkeeperClientTimeoutInSeconds(2);
        config.setManagedLedgerAddEntryTimeoutSeconds(2);
    }

    @Test(timeOut = 300000)
    public void testClientDeduplicationCorrectnessWithFailure() throws Exception {
        String namespace = "prop/" + BrokerTestUtil.newUniqueName("dedup-recovery");
        String topic = "persistent://" + namespace + "/topic";
        String producerName = "dedup-producer";
        admin.namespaces().createNamespace(namespace);
        admin.namespaces().setDeduplicationStatus(namespace, true);
        admin.namespaces().setRetention(namespace, new RetentionPolicies(-1, -1));

        try (PulsarClient client = PulsarClient.builder().serviceUrl(pulsar.getBrokerServiceUrl())
                .operationTimeout(30, TimeUnit.SECONDS).connectionTimeout(5, TimeUnit.SECONDS)
                .maxBackoffInterval(1, TimeUnit.SECONDS).build();
             Producer<String> producer = client.newProducer(Schema.STRING).topic(topic).producerName(producerName)
                .enableBatching(false).maxPendingMessages(WINDOW_SIZE).blockIfQueueFull(false)
                .sendTimeout(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS).create()) {
            awaitSends(sendWindow(producer, 1));

            PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService().getTopic(topic, false)
                    .get(30, TimeUnit.SECONDS).orElseThrow();
            ManagedLedger ledger = persistentTopic.getManagedLedger();
            long initialErrors = ledger.getStats().getAddEntryErrorsTotal();
            List<CompletableFuture<MessageId>> duringFailure;
            bkEnsemble.stopBK();
            try {
                duringFailure = sendWindow(producer, WINDOW_SIZE + 1);
                // Do not restart on a timer: prove that a publish actually encountered the storage outage.
                Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() ->
                        assertThat(ledger.getStats().getAddEntryErrorsTotal())
                                .as("BookKeeper add-entry failures during the outage").isGreaterThan(initialErrors));
                for (CompletableFuture<MessageId> send : duringFailure) {
                    assertThat(send.isDone()).as("publish must remain pending without a BookKeeper quorum").isFalse();
                }
            } finally {
                // Restore storage even if an assertion fails, before closing the producer and the fixture.
                bkEnsemble.startBK();
            }
            awaitSends(duringFailure);

            // Explicitly retry the recovered sequences. A reader must still see only one stored copy of each.
            awaitSends(sendWindow(producer, WINDOW_SIZE + 1));
            awaitSends(sendWindow(producer, 2 * WINDOW_SIZE + 1));

            long endSequence = 3L * WINDOW_SIZE + 1;
            CompletableFuture<MessageId> end = producer.newMessage().sequenceId(endSequence).value("end").sendAsync();
            awaitSends(List.of(end));

            try (Reader<String> reader = client.newReader(Schema.STRING).topic(topic)
                    .startMessageId(MessageId.earliest).receiverQueueSize(WINDOW_SIZE).create()) {
                for (long sequence = 1; sequence <= endSequence; sequence++) {
                    Message<String> message = reader.readNext(5, TimeUnit.SECONDS);
                    assertThat(message).as("persisted message for sequence %s, including completion marker", sequence)
                            .isNotNull();
                    assertThat(message.getSequenceId()).as("exact persisted sequence").isEqualTo(sequence);
                    assertThat(message.getProducerName()).isEqualTo(producerName);
                    assertThat(message.getValue()).isEqualTo(sequence == endSequence ? "end" : "foo-" + sequence);
                }
                assertThat(reader.readNext(1, TimeUnit.SECONDS)).as("no extra persisted copy after completion")
                        .isNull();
            }
        }
    }

    private static List<CompletableFuture<MessageId>> sendWindow(Producer<String> producer, long firstSequence) {
        List<CompletableFuture<MessageId>> sends = new ArrayList<>(WINDOW_SIZE);
        for (long sequence = firstSequence; sequence < firstSequence + WINDOW_SIZE; sequence++) {
            sends.add(producer.newMessage().sequenceId(sequence).value("foo-" + sequence).sendAsync());
        }
        return sends;
    }

    private static void awaitSends(List<CompletableFuture<MessageId>> sends) throws Exception {
        CompletableFuture.allOf(sends.toArray(new CompletableFuture<?>[0]))
                .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
}
