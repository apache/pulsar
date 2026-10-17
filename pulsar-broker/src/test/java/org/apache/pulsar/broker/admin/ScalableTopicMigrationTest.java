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
package org.apache.pulsar.broker.admin;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import java.util.UUID;
import lombok.Cleanup;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ScalableTopicMetadata;
import org.testng.annotations.Test;

/**
 * End-to-end tests for the PIP-475 regular-to-scalable migration admin command
 * ({@code admin.scalableTopics().migrateToScalable(...)}).
 */
public class ScalableTopicMigrationTest extends SharedPulsarBaseTest {

    private String baseName(String suffix) {
        return getNamespace() + "/" + suffix + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    @Test
    public void testMigratePartitionedTopic() throws Exception {
        String topic = baseName("part");
        admin.topics().createPartitionedTopic(topic, 4);

        admin.scalableTopics().migrateToScalable(topic, false);

        // Scalable metadata now exists: 4 sealed legacy parents (ids 0..3) + 4 active
        // children (ids 4..7).
        ScalableTopicMetadata md = admin.scalableTopics().getMetadata("topic://" + topic);
        assertEquals(md.getSegments().size(), 8);
        assertEquals(md.getNextSegmentId(), 8L);
        for (long id = 0; id < 4; id++) {
            ScalableTopicMetadata.SegmentInfo parent = md.getSegments().get(id);
            assertTrue(parent.isSealed(), "parent " + id + " must be sealed");
            assertTrue(parent.isLegacy(), "parent " + id + " must wrap a legacy topic");
            assertEquals(parent.getLegacyTopicName(),
                    "persistent://" + topic + "-partition-" + id);
        }
        for (long id = 4; id < 8; id++) {
            ScalableTopicMetadata.SegmentInfo child = md.getSegments().get(id);
            assertTrue(child.isActive(), "child " + id + " must be active");
            assertFalse(child.isLegacy(), "child " + id + " must be a regular segment");
        }
    }

    @Test
    public void testMigrateNonPartitionedTopic() throws Exception {
        String topic = baseName("np");
        admin.topics().createNonPartitionedTopic("persistent://" + topic);

        admin.scalableTopics().migrateToScalable(topic, false);

        ScalableTopicMetadata md = admin.scalableTopics().getMetadata("topic://" + topic);
        assertEquals(md.getSegments().size(), 2);
        assertTrue(md.getSegments().get(0L).isSealed());
        assertEquals(md.getSegments().get(0L).getLegacyTopicName(), "persistent://" + topic);
        assertTrue(md.getSegments().get(1L).isActive());
    }

    @Test
    public void testMigrateFailsWhenAlreadyScalable() throws Exception {
        String topic = baseName("already");
        admin.topics().createNonPartitionedTopic("persistent://" + topic);
        admin.scalableTopics().migrateToScalable(topic, false);

        // A second migration must be rejected — the topic is already scalable.
        PulsarAdminException ex = expectThrows(PulsarAdminException.class,
                () -> admin.scalableTopics().migrateToScalable(topic, false));
        assertEquals(ex.getStatusCode(), 409);
    }

    @Test
    public void testMigrateFailsForNonExistentTopic() {
        String topic = baseName("ghost");
        PulsarAdminException ex = expectThrows(PulsarAdminException.class,
                () -> admin.scalableTopics().migrateToScalable(topic, false));
        assertEquals(ex.getStatusCode(), 404);
    }

    @Test
    public void testMigrateRejectsLegacyV4ConnectionWithoutForce() throws Exception {
        String topic = baseName("legacy");
        admin.topics().createNonPartitionedTopic("persistent://" + topic);

        // A plain v4 producer carries no V5-managed marker → counts as a legacy connection.
        @Cleanup
        Producer<byte[]> v4Producer = pulsarClient.newProducer()
                .topic("persistent://" + topic)
                .create();

        PulsarAdminException ex = expectThrows(PulsarAdminException.class,
                () -> admin.scalableTopics().migrateToScalable(topic, false));
        assertEquals(ex.getStatusCode(), 409);

        // No scalable metadata should have been written.
        PulsarAdminException notFound = expectThrows(PulsarAdminException.class,
                () -> admin.scalableTopics().getMetadata("topic://" + topic));
        assertEquals(notFound.getStatusCode(), 404);
    }

    @Test
    public void testMigrateWithForceSucceedsDespiteLegacyConnection() throws Exception {
        String topic = baseName("force");
        admin.topics().createNonPartitionedTopic("persistent://" + topic);

        @Cleanup
        Producer<byte[]> v4Producer = pulsarClient.newProducer()
                .topic("persistent://" + topic)
                .create();

        // force=true bypasses the legacy-connection precheck.
        admin.scalableTopics().migrateToScalable(topic, true);

        ScalableTopicMetadata md = admin.scalableTopics().getMetadata("topic://" + topic);
        assertEquals(md.getSegments().size(), 2);
    }

    @Test
    public void testAutoCreateBlockedWhenScalableTopicShadowsName() throws Exception {
        // A scalable topic owns topic://t/n/x; the broker must refuse to auto-create the
        // shadowing persistent://t/n/x (the long-term "once scalable, always scalable"
        // guard that survives even after a migrated topic's old ledgers are GC'd).
        String name = baseName("shadow");

        // Sanity: before the scalable topic exists, auto-create of the persistent name is
        // allowed (confirms the shared cluster has auto-creation enabled).
        assertTrue(getPulsar().getBrokerService()
                        .isAllowAutoTopicCreationAsync(TopicName.get("persistent://" + name)).get(),
                "auto-create should be allowed before the name is claimed by a scalable topic");

        admin.scalableTopics().createScalableTopic("topic://" + name, 1);

        assertFalse(getPulsar().getBrokerService()
                        .isAllowAutoTopicCreationAsync(TopicName.get("persistent://" + name)).get(),
                "auto-create must be blocked when a scalable topic shadows the persistent:// name");
    }
}
