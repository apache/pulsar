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
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.BrokerServiceException.ServiceUnitNotReadyException;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.assertj.core.api.SoftAssertions;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * A topic delete that reaches a broker after the broker has given up the topic's namespace bundle must not delete
 * the topic that another broker serves.
 */
@CustomLog
@Test(groups = "broker")
public class BrokerServiceDeleteAfterUnloadTest extends SharedMultiBrokerPulsarBaseTest {

    @DataProvider
    public Object[][] forceDelete() {
        return new Object[][] {{false}, {true}};
    }

    // A delete can pass the admin API's ownership check and reach the broker only after a bundle unload closed the
    // topic and released the bundle. This is such a delete as it reaches the broker.
    @Test(dataProvider = "forceDelete", timeOut = 120_000)
    public void testDeleteReachingFormerOwnerAfterBundleUnload(boolean forceDelete) throws Exception {
        String topic = newTopicName();
        TopicName topicName = TopicName.get(topic);
        String schemaPath = "/schemas/" + topicName.getSchemaName();
        try (Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic).create()) {
            producer.send("before unload");
            PulsarService formerOwner = brokers.stream()
                    .filter(broker -> broker.getBrokerService().getTopicReference(topic).isPresent())
                    .findFirst().orElseThrow();
            PulsarService newOwner = brokers.stream()
                    .filter(broker -> broker != formerOwner).findFirst().orElseThrow();
            PulsarService leader = brokers.stream()
                    .filter(broker -> broker.getLeaderElectionService().isLeader()).findFirst().orElseThrow();
            NamespaceBundle bundle = formerOwner.getNamespaceService().getBundle(topicName);

            leader.getLoadManager().get().setNamespaceBundleAffinity(bundle.toString(), newOwner.getBrokerId());
            formerOwner.getNamespaceService().unloadNamespaceBundle(bundle).get(30, TimeUnit.SECONDS);
            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                assertThat(newOwner.getBrokerService().getTopicReference(topic))
                        .as("the new owner loaded the topic").isPresent();
                assertThat(newOwner.getBrokerService().getTopicReference(topic).orElseThrow().getProducers())
                        .as("the producer reconnected to the new owner").hasSize(1);
                assertThat(producer.isConnected()).as("the producer is connected").isTrue();
            });
            producer.sendAsync("on the new owner").get(10, TimeUnit.SECONDS);
            PersistentTopic newTopic =
                    (PersistentTopic) newOwner.getBrokerService().getTopicReference(topic).orElseThrow();
            long newOwnerLedgerId = ((ManagedLedgerImpl) newTopic.getManagedLedger()).getLedgersInfo().lastKey();
            assertThat(formerOwner.getBrokerService().getTopics())
                    .as("the topic cache of the former owner after the unload").doesNotContainKey(topic);

            Throwable deletionError = formerOwner.getBrokerService().deleteTopic(topic, forceDelete)
                    .handle((ignored, error) -> error).get(30, TimeUnit.SECONDS);
            boolean managedLedgerExists = newOwner.getPulsarResources().getTopicResources()
                    .persistentTopicExists(topicName).get(10, TimeUnit.SECONDS);
            boolean schemaExists = newOwner.getLocalMetadataStore().exists(schemaPath).get(10, TimeUnit.SECONDS);
            boolean newOwnerLedgerExists = ledgerExists(newOwner, newOwnerLedgerId);
            Throwable sendError = producer.sendAsync("after the delete on the former owner")
                    .handle((ignored, error) -> error).get(30, TimeUnit.SECONDS);
            log.info()
                    .attr("deletionError", deletionError)
                    .attr("managedLedgerExists", managedLedgerExists)
                    .attr("schemaExists", schemaExists)
                    .attr("newOwnerLedgerId", newOwnerLedgerId)
                    .attr("newOwnerLedgerExists", newOwnerLedgerExists)
                    .attr("sendError", sendError)
                    .log("State after a delete that reached the former owner after the unload");

            SoftAssertions softly = new SoftAssertions();
            softly.assertThat(FutureUtil.unwrapCompletionException(deletionError))
                    .as("the delete on %s, which no longer owns the bundle, must fail", formerOwner.getBrokerId())
                    .isInstanceOf(ServiceUnitNotReadyException.class);
            softly.assertThat(managedLedgerExists)
                    .as("the managed ledger of the topic that %s serves must still exist", newOwner.getBrokerId())
                    .isTrue();
            softly.assertThat(schemaExists)
                    .as("the schema of the topic that %s serves must still exist", newOwner.getBrokerId())
                    .isTrue();
            softly.assertThat(newOwnerLedgerExists)
                    .as("the ledger %s that %s writes to must still exist", newOwnerLedgerId, newOwner.getBrokerId())
                    .isTrue();
            softly.assertThat(sendError)
                    .as("a send to the topic on %s must still succeed", newOwner.getBrokerId())
                    .isNull();
            softly.assertThat(formerOwner.getBrokerService().getTopics())
                    .as("the topic cache of %s after the delete", formerOwner.getBrokerId())
                    .doesNotContainKey(topic);
            softly.assertAll();
        }
    }

    private static boolean ledgerExists(PulsarService broker, long ledgerId) throws Exception {
        return broker.getBookKeeperClient().getLedgerMetadata(ledgerId).handle((metadata, error) -> {
            if (error == null) {
                return true;
            }
            Throwable cause = FutureUtil.unwrapCompletionException(error);
            if (cause instanceof BKException.BKNoSuchLedgerExistsOnMetadataServerException
                    || cause instanceof BKException.BKNoSuchLedgerExistsException) {
                return false;
            }
            throw FutureUtil.wrapToCompletionException(cause);
        }).get(10, TimeUnit.SECONDS);
    }
}
