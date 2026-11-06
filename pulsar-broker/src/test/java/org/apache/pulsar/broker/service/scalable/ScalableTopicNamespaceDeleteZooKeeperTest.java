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
package org.apache.pulsar.broker.service.scalable;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.Set;
import java.util.UUID;
import lombok.Cleanup;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.v5.Producer;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Namespace deletion with scalable topics on ZooKeeper, which refuses to delete a node that has children: what
 * the delete leaves under the namespace fails it after its retries.
 */
public class ScalableTopicNamespaceDeleteZooKeeperTest extends MockedPulsarServiceBaseTest {

    private PulsarClient v5Client;

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        conf.setForceDeleteNamespaceAllowed(true);
        internalSetup();
        setupDefaultTenantAndNamespace();
        v5Client = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .build();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        if (v5Client != null) {
            v5Client.close();
            v5Client = null;
        }
        internalCleanup();
    }

    @Test
    public void testForceDeleteNamespaceWithScalableTopic() throws Exception {
        String namespace = newNamespace();
        String topic = "topic://" + namespace + "/scalable";
        admin.scalableTopics().createScalableTopic(topic, 1);
        publishString(topic, "v");

        admin.namespaces().deleteNamespace(namespace, true);

        assertNamespaceDeleted(namespace, topic);
    }

    @Test
    public void testDeleteNamespaceAfterDeletingItsScalableTopic() throws Exception {
        String namespace = newNamespace();
        String topic = "topic://" + namespace + "/scalable";
        admin.scalableTopics().createScalableTopic(topic, 1);
        publishString(topic, "v");
        admin.scalableTopics().deleteScalableTopic(topic, true);

        admin.namespaces().deleteNamespace(namespace);

        assertNamespaceDeleted(namespace, topic);
    }

    private String newNamespace() throws Exception {
        String namespace = "public/ns-" + UUID.randomUUID().toString().substring(0, 8);
        admin.namespaces().createNamespace(namespace, Set.of(configClusterName));
        return namespace;
    }

    private void publishString(String topic, String value) throws Exception {
        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        producer.newMessage().key("k").value(value).send();
    }

    private void assertNamespaceDeleted(String namespace, String topic) throws Exception {
        assertThat(admin.namespaces().getNamespaces("public")).doesNotContain(namespace);
        MetadataStore store = pulsar.getLocalMetadataStore();
        assertThat(store.exists("/topics/" + namespace).get())
                .as("scalable topic records of the deleted namespace %s", namespace)
                .isFalse();
        assertThat(store.exists("/managed-ledgers/" + namespace).get())
                .as("managed ledgers of the deleted namespace %s", namespace)
                .isFalse();
        assertThat(pulsar.getSchemaRegistryService().getSchema(TopicName.get(topic).getSchemaName()).get())
                .as("schema of %s", topic)
                .isNull();
    }
}
