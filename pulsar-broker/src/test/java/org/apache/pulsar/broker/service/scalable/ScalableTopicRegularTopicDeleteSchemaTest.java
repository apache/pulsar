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
import java.util.UUID;
import lombok.Cleanup;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.v5.Producer;
import org.apache.pulsar.client.api.v5.V5ClientBaseTest;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Deleting a regular topic with the name of a scalable topic must leave the schema of the scalable topic in
 * place: the {@code persistent://} topic that was migrated to the scalable topic (PIP-475), or a
 * {@code non-persistent://} topic of the same name. They map to the same schema id
 * ({@link TopicName#getSchemaName()}), which belongs to the scalable topic while it exists: only deleting the
 * scalable topic itself deletes it.
 */
public class ScalableTopicRegularTopicDeleteSchemaTest extends V5ClientBaseTest {

    @Test
    public void testDeleteMigratedTopicKeepsSchema() throws Exception {
        String base = migrateAfterPublishing(0);

        admin.topics().delete("persistent://" + base, true);

        assertSchemaKept(base);
    }

    @Test
    public void testDeleteUnloadedMigratedTopicKeepsSchema() throws Exception {
        String base = migrateAfterPublishing(0);
        admin.topics().unload("persistent://" + base);
        assertThat(getPulsar().getBrokerService().getTopicReference("persistent://" + base))
                .as("the migrated topic is no longer loaded, so the delete goes through the metadata")
                .isEmpty();

        admin.topics().delete("persistent://" + base, true);

        assertSchemaKept(base);
    }

    @Test
    public void testDeleteMigratedPartitionedTopicKeepsSchema() throws Exception {
        String base = migrateAfterPublishing(2);

        admin.topics().deletePartitionedTopic("persistent://" + base, true);

        assertSchemaKept(base);
    }

    @Test
    public void testInactiveTopicGcOfMigratedTopicKeepsSchema() throws Exception {
        String base = migrateAfterPublishing(0);
        // The migrated topic is terminated and has no subscriptions, so the inactive-topic GC deletes it. The
        // shared cluster disables that GC broker-wide: enable it for the test namespace.
        admin.namespaces().setInactiveTopicPolicies(getNamespace(),
                new InactiveTopicPolicies(InactiveTopicDeleteMode.delete_when_no_subscriptions, 0, true));
        PersistentTopic migrated = (PersistentTopic) getPulsar().getBrokerService()
                .getTopicIfExists("persistent://" + base).get().orElseThrow();
        Awaitility.await().until(migrated::isDeleteWhileInactive);

        migrated.checkGC();

        Awaitility.await().untilAsserted(() -> assertThat(getPulsar().getPulsarResources().getTopicResources()
                .persistentTopicExists(TopicName.get("persistent://" + base)).get())
                .as("the inactive-topic GC deletes the migrated topic")
                .isFalse());
        assertSchemaKept(base);
    }

    @Test
    public void testDeleteNonPersistentTopicKeepsSchema() throws Exception {
        String base = getNamespace() + "/scalable-" + UUID.randomUUID().toString().substring(0, 8);
        admin.scalableTopics().createScalableTopic("topic://" + base, 1);
        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic("topic://" + base)
                .create();
        producer.newMessage().key("k").value("v").send();
        assertSchemaKept(base);
        pulsarClient.newProducer().topic("non-persistent://" + base).create().close();
        assertThat(getPulsar().getBrokerService().getTopicReference("non-persistent://" + base))
                .as("the non-persistent topic with the name of the scalable topic is loaded")
                .isPresent();

        admin.topics().delete("non-persistent://" + base, true);

        assertThat(getPulsar().getBrokerService().getTopicReference("non-persistent://" + base))
                .as("the non-persistent topic is deleted")
                .isEmpty();
        assertSchemaKept(base);
    }

    @Test
    public void testDeleteScalableTopicDeletesSchemaKeptByMigratedTopicDelete() throws Exception {
        String base = migrateAfterPublishing(0);
        admin.topics().delete("persistent://" + base, true);
        assertSchemaKept(base);

        admin.scalableTopics().deleteScalableTopic("topic://" + base, true);

        assertThat(getPulsar().getSchemaRegistryService()
                .getSchema(TopicName.get("topic://" + base).getSchemaName()).get())
                .as("schema of topic://%s after deleting the scalable topic", base)
                .isNull();
    }

    /**
     * Creates a regular topic ({@code partitions <= 0} for a non-partitioned one), publishes {@code "pre"} with a
     * V5 string producer, migrates it to a scalable topic, then publishes {@code "post"} on the scalable topic.
     * Returns the {@code tenant/namespace/name} the regular and the scalable topic share.
     */
    private String migrateAfterPublishing(int partitions) throws Exception {
        String base = getNamespace() + "/migrated-" + UUID.randomUUID().toString().substring(0, 8);
        if (partitions > 0) {
            admin.topics().createPartitionedTopic("persistent://" + base, partitions);
        } else {
            admin.topics().createNonPartitionedTopic("persistent://" + base);
        }
        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic("persistent://" + base)
                .create();
        producer.newMessage().key("k").value("pre").send();
        admin.scalableTopics().migrateToScalable(base, false);
        producer.newMessage().key("k").value("post").send();
        assertSchemaKept(base);
        return base;
    }

    private void assertSchemaKept(String base) throws Exception {
        assertThat(admin.schemas().getAllSchemas("topic://" + base))
                .as("schemas of the scalable topic topic://%s", base)
                .extracting(SchemaInfo::getType)
                .containsExactly(SchemaType.STRING);
    }
}
