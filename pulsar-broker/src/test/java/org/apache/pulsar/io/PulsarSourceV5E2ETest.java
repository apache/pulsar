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
package org.apache.pulsar.io;

import static org.apache.pulsar.functions.worker.PulsarFunctionLocalRunTest.getPulsarIODataGeneratorNar;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.QueueConsumer;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.common.policies.data.PublisherStats;
import org.apache.pulsar.common.scalable.ScalableTopicConstants;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * End-to-end tests of Pulsar sources that publish with the V5 client.
 */
@Test(groups = "broker-io")
public class PulsarSourceV5E2ETest extends AbstractPulsarE2ETest {

    private static final String NAMESPACE = "io-v5";

    private void createNamespace() throws Exception {
        admin.namespaces().createNamespace(tenant + "/" + NAMESPACE, Set.of("use"));
    }

    private static SourceConfig sourceConfig(String tenant, String name, String topic) {
        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setTenant(tenant);
        sourceConfig.setNamespace(NAMESPACE);
        sourceConfig.setName(name);
        sourceConfig.setParallelism(1);
        sourceConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);
        sourceConfig.setTopicName(topic);
        return sourceConfig;
    }

    @Test(timeOut = 60000)
    public void testSourceToScalableTopic() throws Exception {
        createNamespace();
        String topic = "topic://" + tenant + "/" + NAMESPACE + "/scalable-output";
        admin.scalableTopics().createScalableTopic(topic, 2);

        try (PulsarClient client = newV5Client();
             QueueConsumer<byte[]> consumer = client.newQueueConsumer(Schema.bytes())
                     .topic(topic)
                     .subscriptionName("verify")
                     .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                     .subscribe()) {
            // the topic:// output selects the V5 client without setting clientApi
            admin.sources().createSourceWithUrl(sourceConfig(tenant, "v5-source", topic),
                    getPulsarIODataGeneratorNar().toURI().toString());

            Message<byte[]> message = consumer.receive(Duration.ofSeconds(30));
            assertThat(message).isNotNull();
            assertThat(message.value()).isNotEmpty();
            consumer.acknowledge(message.id());
        } finally {
            admin.sources().deleteSource(tenant, NAMESPACE, "v5-source");
        }
    }

    @Test(timeOut = 60000)
    public void testSourceWithClientApiV5ToPersistentTopic() throws Exception {
        createNamespace();
        String topic = "persistent://" + tenant + "/" + NAMESPACE + "/output";
        SourceConfig sourceConfig = sourceConfig(tenant, "v5-persistent-source", topic);
        sourceConfig.setClientApi(FunctionConfig.ClientApi.V5);
        admin.sources().createSourceWithUrl(sourceConfig, getPulsarIODataGeneratorNar().toURI().toString());
        try {
            // the V5 client marks its publishers so that PIP-475 migration does not count them as legacy clients
            Awaitility.await().atMost(Duration.ofSeconds(30)).ignoreExceptions().untilAsserted(() -> {
                List<? extends PublisherStats> publishers = admin.topics().getStats(topic).getPublishers();
                assertThat(publishers).hasSize(1);
                assertThat(publishers.get(0).getMetadata())
                        .containsEntry(ScalableTopicConstants.V5_MANAGED_METADATA_KEY,
                                ScalableTopicConstants.V5_MANAGED_METADATA_VALUE)
                        .containsEntry("id", tenant + "/" + NAMESPACE + "/v5-persistent-source");
                assertThat(admin.topics().getInternalStats(topic, false).numberOfEntries).isPositive();
            });
        } finally {
            admin.sources().deleteSource(tenant, NAMESPACE, "v5-persistent-source");
        }
    }

    @Test(timeOut = 60000)
    public void testRejectsClientApiV4WithScalableTopic() throws Exception {
        createNamespace();
        SourceConfig sourceConfig = sourceConfig(tenant, "v4-source",
                "topic://" + tenant + "/" + NAMESPACE + "/scalable-output");
        sourceConfig.setClientApi(FunctionConfig.ClientApi.V4);
        assertThatThrownBy(() -> admin.sources().createSourceWithUrl(sourceConfig,
                getPulsarIODataGeneratorNar().toURI().toString()))
                .isInstanceOf(PulsarAdminException.class)
                .hasMessageContaining("clientApi V4 cannot be used with topic");
    }
}
