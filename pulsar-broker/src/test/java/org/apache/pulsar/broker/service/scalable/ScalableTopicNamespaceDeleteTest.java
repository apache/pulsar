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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import lombok.Cleanup;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
import org.apache.pulsar.broker.service.SharedPulsarCluster;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.v5.Checkpoint;
import org.apache.pulsar.client.api.v5.CheckpointConsumer;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.Producer;
import org.apache.pulsar.client.api.v5.V5ClientBaseTest;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.assertj.core.api.SoftAssertions;
import org.testng.annotations.Test;

/**
 * Deleting a namespace, or a tenant, must handle the scalable topics in it like its regular topics: a delete
 * without force is refused while one exists, and a forced delete deletes them, with their metadata, the managed
 * ledgers of their segments and their schema, so a namespace re-created with the same name starts empty.
 */
public class ScalableTopicNamespaceDeleteTest extends V5ClientBaseTest {

    @Test
    public void testDeleteNamespaceWithScalableTopicIsRejected() throws Exception {
        String topic = newScalableTopic(1);

        assertThatThrownBy(() -> admin.namespaces().deleteNamespace(getNamespace()))
                .as("deleting, without force, a namespace that has a scalable topic")
                .isInstanceOf(PulsarAdminException.ConflictException.class);

        assertThat(admin.namespaces().getPolicies(getNamespace()).deleted)
                .as("the namespace is not marked deleted")
                .isFalse();
        assertThat(admin.scalableTopics().listScalableTopics(getNamespace()))
                .containsExactly(topic);
    }

    @Test
    public void testForceDeleteNamespaceDeletesScalableTopic() throws Exception {
        String namespace = getNamespace();
        String topic = newScalableTopic(2);
        publishString(topic, "v");

        admin.namespaces().deleteNamespace(namespace, true);

        assertScalableTopicDeleted(namespace, topic);
    }

    @Test
    public void testDeleteNamespaceDeletesSegmentTopicsLeftBehind() throws Exception {
        String namespace = getNamespace();
        String topic = newScalableTopic(2);
        publishString(topic, "v");
        // What a scalable-topic delete that failed to delete the segment topics leaves: only the record is gone.
        getPulsar().getPulsarResources().getScalableTopicResources()
                .deleteScalableTopicAsync(TopicName.get(topic)).get();

        admin.namespaces().deleteNamespace(namespace);

        assertThat(store().getChildren("/managed-ledgers/" + namespace + "/segment").get())
                .as("managed ledgers of the segment topics left behind")
                .isEmpty();
    }

    @Test
    public void testRecreatedNamespaceDoesNotResurrectScalableTopic() throws Exception {
        String namespace = getNamespace();
        String topic = newScalableTopic(1);
        publishString(topic, "old");

        admin.namespaces().deleteNamespace(namespace, true);
        admin.namespaces().createNamespace(namespace, Set.of(SharedPulsarCluster.CLUSTER_NAME));

        assertThat(admin.scalableTopics().listScalableTopics(namespace))
                .as("scalable topics of the re-created namespace")
                .isEmpty();
        // A new topic with the name of the deleted one, and another schema, starts empty.
        admin.scalableTopics().createScalableTopic(topic, 1);
        @Cleanup
        Producer<Integer> producer = v5Client.newProducer(Schema.int32())
                .topic(topic)
                .create();
        producer.newMessage().key("k").value(1).send();
        @Cleanup
        CheckpointConsumer<Integer> consumer = v5Client.newCheckpointConsumer(Schema.int32())
                .topic(topic)
                .startPosition(Checkpoint.earliest())
                .create();
        Message<Integer> msg = consumer.receive(Duration.ofSeconds(10));
        assertThat(msg).as("first message of the new topic").isNotNull();
        assertThat(msg.value()).isEqualTo(1);
    }

    @Test
    public void testForceDeleteTenantDeletesScalableTopic() throws Exception {
        String tenant = "scalable-tenant-" + UUID.randomUUID().toString().substring(0, 8);
        admin.tenants().createTenant(tenant, TenantInfo.builder()
                .allowedClusters(Set.of(SharedPulsarCluster.CLUSTER_NAME))
                .build());
        String namespace = tenant + "/ns";
        admin.namespaces().createNamespace(namespace, Set.of(SharedPulsarCluster.CLUSTER_NAME));
        String topic = "topic://" + namespace + "/scalable";
        admin.scalableTopics().createScalableTopic(topic, 1);
        publishString(topic, "v");

        admin.tenants().deleteTenant(tenant, true);

        assertScalableTopicDeleted(namespace, topic);
        assertThat(store().getChildren("/topics/" + tenant).get())
                .as("scalable topic records of the deleted tenant")
                .isEmpty();
    }

    @Test
    public void testForceDeleteNamespaceDeletesMigratedTopic() throws Exception {
        String namespace = getNamespace();
        String base = namespace + "/migrated-" + UUID.randomUUID().toString().substring(0, 8);
        admin.topics().createNonPartitionedTopic("persistent://" + base);
        publishString("persistent://" + base, "pre");
        admin.scalableTopics().migrateToScalable(base, false);
        publishString("topic://" + base, "post");

        admin.namespaces().deleteNamespace(namespace, true);

        assertScalableTopicDeleted(namespace, "topic://" + base);
        assertThat(getPulsar().getPulsarResources().getTopicResources()
                .persistentTopicExists(TopicName.get("persistent://" + base)).get())
                .as("the persistent topic the scalable topic was migrated from")
                .isFalse();
    }

    @Test
    public void testForceDeleteNamespaceWithSameNamedNonPersistentTopic() throws Exception {
        String namespace = getNamespace();
        String topic = newScalableTopic(1);
        publishString(topic, "v");
        // Connected, so the namespace delete deletes the non-persistent topic as an active one.
        @Cleanup
        var nonPersistentProducer = pulsarClient.newProducer()
                .topic("non-persistent://" + TopicName.get(topic).getNamespace() + "/"
                        + TopicName.get(topic).getLocalName())
                .create();

        admin.namespaces().deleteNamespace(namespace, true);

        assertScalableTopicDeleted(namespace, topic);
    }

    private void publishString(String topic, String value) throws Exception {
        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .create();
        producer.newMessage().key("k").value(value).send();
    }

    private MetadataStore store() {
        return getPulsar().getPulsarResources().getScalableTopicResources().getStore();
    }

    private void assertScalableTopicDeleted(String namespace, String topic) {
        ScalableTopicResources resources = getPulsar().getPulsarResources().getScalableTopicResources();
        SoftAssertions softly = new SoftAssertions();
        softly.assertThat(store().getChildren(resources.namespacePath(NamespaceName.get(namespace))).join())
                .as("scalable topic records of the deleted namespace %s", namespace)
                .isEmpty();
        softly.assertThat(store().getChildren("/managed-ledgers/" + namespace + "/segment").join())
                .as("managed ledgers of the segments of the deleted namespace %s", namespace)
                .isEmpty();
        softly.assertThat(getPulsar().getSchemaRegistryService()
                        .getSchema(TopicName.get(topic).getSchemaName()).join())
                .as("schema of %s", topic)
                .isNull();
        softly.assertAll();
    }
}
