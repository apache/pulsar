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
package org.apache.pulsar.broker.service.persistent;

import static org.apache.bookkeeper.mledger.ManagedLedgerConfig.PROPERTY_SOURCE_TOPIC_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ShadowTopicsDisabledTest extends SharedPulsarBaseTest {

    @DataProvider
    public Object[][] partitionCounts() {
        return new Object[][] {{0}, {2}};
    }

    @Test(dataProvider = "partitionCounts")
    public void testCreationAndConfigurationDisabled(int partitions) throws Exception {
        assertThat(new ServiceConfiguration().isEnableShadowTopics()).isFalse();
        assertThat(getConfig().isEnableShadowTopics()).isFalse();
        String source = newTopicName();
        String shadow = source + "-shadow";
        if (partitions == 0) {
            admin.topics().createNonPartitionedTopic(source);
        } else {
            admin.topics().createPartitionedTopic(source, partitions);
        }
        assertThatThrownBy(() -> admin.topics().createShadowTopic(shadow, source))
                .isInstanceOf(PulsarAdminException.NotAllowedException.class)
                .hasMessageContaining("Shadow topics are disabled");
        assertThatThrownBy(() -> admin.topics().setShadowTopics(source, List.of(shadow)))
                .isInstanceOf(PulsarAdminException.NotAllowedException.class);
        assertThatThrownBy(() -> admin.topics().updateProperties(source,
                Map.of(PROPERTY_SOURCE_TOPIC_KEY, shadow)))
                .isInstanceOf(PulsarAdminException.NotAllowedException.class);
        assertThat(admin.topics().getShadowTopics(source)).isNull();
        admin.topics().removeShadowTopics(source);
        try (Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(source)
                     .subscriptionName("ordinary").subscribe();
             Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(source).create()) {
            producer.send("message");
            assertThat(consumer.receive(10, TimeUnit.SECONDS).getValue()).isEqualTo("message");
        }
    }

    @Test(dataProvider = "partitionCounts")
    public void testExistingShadowTopicCannotLoad(int partitions) throws Exception {
        String source = newTopicName();
        String shadow = source + "-shadow";
        String topicToLoad = shadow;
        if (partitions == 0) {
            admin.topics().createNonPartitionedTopic(shadow);
            PersistentTopic topic = (PersistentTopic) getTopicIfExists(shadow).get().orElseThrow();
            topic.getManagedLedger().setProperty(PROPERTY_SOURCE_TOPIC_KEY, source);
            admin.topics().unload(shadow);
        } else {
            getPulsar().getPulsarResources().getNamespaceResources().getPartitionedTopicResources()
                    .createPartitionedTopic(TopicName.get(shadow),
                            new PartitionedTopicMetadata(partitions, Map.of(PROPERTY_SOURCE_TOPIC_KEY, source)));
            topicToLoad = TopicName.get(shadow).getPartition(0).toString();
        }
        String finalTopicToLoad = topicToLoad;
        try {
            assertThatThrownBy(() -> getTopic(finalTopicToLoad, true).get(10, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(BrokerServiceException.NotAllowedException.class)
                    .hasStackTraceContaining("Shadow topics are disabled");
            assertThat(getTopicReference(finalTopicToLoad)).isEmpty();
        } finally {
            if (partitions == 0) {
                getPulsar().getDefaultManagedLedgerFactory().delete(
                        TopicName.get(shadow).getPersistenceNamingEncoding());
            } else {
                admin.topics().deletePartitionedTopic(shadow, true);
            }
        }
    }

    @Test
    public void testExistingShadowPolicyDoesNotStartReplication() throws Exception {
        String source = newTopicName();
        String shadow = source + "-shadow";
        admin.topics().createNonPartitionedTopic(source);
        getPulsar().getTopicPoliciesService().updateTopicPoliciesAsync(TopicName.get(source), false, false,
                policies -> policies.setShadowTopics(List.of(shadow))).get(10, TimeUnit.SECONDS);
        admin.topics().unload(source);
        PersistentTopic topic = (PersistentTopic) getTopicIfExists(source).get().orElseThrow();
        assertThat(admin.topics().getShadowTopics(source)).containsExactly(shadow);
        topic.checkReplication().get(10, TimeUnit.SECONDS);
        assertThat(topic.getShadowReplicators()).isEmpty();
        admin.topics().removeShadowTopics(source);
    }
}
