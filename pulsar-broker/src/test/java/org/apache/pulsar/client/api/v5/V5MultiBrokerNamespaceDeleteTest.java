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
package org.apache.pulsar.client.api.v5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import lombok.Cleanup;
import org.apache.pulsar.broker.service.SharedMultiBrokerPulsarCluster;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.assertj.core.api.SoftAssertions;
import org.testng.annotations.Test;

/**
 * Namespace and tenant deletion with scalable topics, on Oxia: unlike the in-memory store of the single-broker
 * tests, it refuses to delete a node that has children, so whatever the delete leaves under the namespace
 * fails it.
 */
public class V5MultiBrokerNamespaceDeleteTest extends V5MultiBrokerClientBaseTest {

    @Test
    public void testDeleteNamespaceWithScalableTopicIsRejected() throws Exception {
        String namespace = getNamespace();
        String topic = newScalableTopic(1);

        Throwable thrown = catchThrowable(() -> admin.namespaces().deleteNamespace(namespace));

        SoftAssertions softly = new SoftAssertions();
        softly.assertThat(thrown)
                .as("deleting, without force, a namespace that has a scalable topic")
                .isInstanceOf(PulsarAdminException.ConflictException.class);
        softly.assertThat(admin.namespaces().getPolicies(namespace).deleted)
                .as("the namespace is not marked deleted")
                .isFalse();
        softly.assertThat(admin.scalableTopics().listScalableTopics(namespace))
                .containsExactly(topic);
        softly.assertAll();
    }

    @Test
    public void testForceDeleteNamespaceWithScalableTopic() throws Exception {
        String namespace = getNamespace();
        String topic = newScalableTopic(2);
        publishString(topic, "v");

        admin.namespaces().deleteNamespace(namespace, true);

        assertNamespaceDeleted(namespace, topic);
    }

    @Test
    public void testDeleteNamespaceAfterDeletingItsScalableTopic() throws Exception {
        String namespace = getNamespace();
        String topic = newScalableTopic(1);
        publishString(topic, "v");
        admin.scalableTopics().deleteScalableTopic(topic, true);

        admin.namespaces().deleteNamespace(namespace);

        assertNamespaceDeleted(namespace, topic);
    }

    @Test
    public void testDeleteNamespaceDeletesSegmentTopicsLeftBehind() throws Exception {
        String namespace = getNamespace();
        String topic = newScalableTopic(2);
        publishString(topic, "v");
        // What a scalable-topic delete that failed to delete the segment topics leaves: only the record is gone.
        brokers.get(0).getPulsarResources().getScalableTopicResources()
                .deleteScalableTopicAsync(TopicName.get(topic)).get();

        admin.namespaces().deleteNamespace(namespace);

        assertThat(admin.namespaces().getNamespaces(SharedMultiBrokerPulsarCluster.TENANT_NAME))
                .doesNotContain(namespace);
        assertThat(store().exists("/managed-ledgers/" + namespace).get())
                .as("managed ledgers of the deleted namespace %s", namespace)
                .isFalse();
    }

    @Test
    public void testDeleteNamespaceWithNodeLeftUnderItIsConflict() throws Exception {
        String namespace = getNamespace();
        // Like a topic created while the namespace is deleted, but one that nothing deletes.
        String stray = "/managed-ledgers/" + namespace + "/stray";
        store().put(stray, new byte[0], Optional.empty()).get();

        assertThatThrownBy(() -> admin.namespaces().deleteNamespace(namespace))
                .as("deleting a namespace that keeps a node under it, after the retries")
                .isInstanceOf(PulsarAdminException.ConflictException.class)
                .hasMessageContaining("/managed-ledgers/" + namespace);

        store().delete(stray, Optional.empty()).get();
        admin.namespaces().deleteNamespace(namespace);
        assertThat(admin.namespaces().getNamespaces(SharedMultiBrokerPulsarCluster.TENANT_NAME))
                .doesNotContain(namespace);
    }

    @Test
    public void testForceDeleteTenantWithScalableTopic() throws Exception {
        String tenant = "scalable-tenant-" + UUID.randomUUID().toString().substring(0, 8);
        admin.tenants().createTenant(tenant, TenantInfo.builder()
                .allowedClusters(Set.of(SharedMultiBrokerPulsarCluster.CLUSTER_NAME))
                .build());
        String namespace = tenant + "/ns";
        admin.namespaces().createNamespace(namespace, Set.of(SharedMultiBrokerPulsarCluster.CLUSTER_NAME));
        String topic = "topic://" + namespace + "/scalable";
        admin.scalableTopics().createScalableTopic(topic, 1);
        publishString(topic, "v");

        admin.tenants().deleteTenant(tenant, true);

        assertThat(admin.tenants().getTenants()).doesNotContain(tenant);
        assertThat(store().exists("/topics/" + tenant).get())
                .as("scalable topic records of the deleted tenant")
                .isFalse();
        assertThat(store().exists("/managed-ledgers/" + tenant).get())
                .as("managed ledgers of the deleted tenant")
                .isFalse();
        assertSchemaDeleted(topic);
    }

    private void publishString(String topic, String value) throws Exception {
        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        producer.newMessage().key("k").value(value).send();
    }

    private MetadataStore store() {
        return brokers.get(0).getPulsarResources().getScalableTopicResources().getStore();
    }

    private void assertNamespaceDeleted(String namespace, String topic) throws Exception {
        assertThat(admin.namespaces().getNamespaces(SharedMultiBrokerPulsarCluster.TENANT_NAME))
                .doesNotContain(namespace);
        assertThat(store().exists("/topics/" + namespace).get())
                .as("scalable topic records of the deleted namespace %s", namespace)
                .isFalse();
        assertThat(store().exists("/managed-ledgers/" + namespace).get())
                .as("managed ledgers of the deleted namespace %s", namespace)
                .isFalse();
        assertSchemaDeleted(topic);
    }

    private void assertSchemaDeleted(String topic) throws Exception {
        assertThat(brokers.get(0).getSchemaRegistryService()
                .getSchema(TopicName.get(topic).getSchemaName()).get())
                .as("schema of %s", topic)
                .isNull();
    }
}
