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
package org.apache.pulsar.broker.resources;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Cleanup;
import org.apache.pulsar.broker.resources.ScalableTopicMetadata.PendingLayoutTransition;
import org.apache.pulsar.broker.service.scalable.ScalableTopicController;
import org.apache.pulsar.broker.service.scalable.SegmentLayout;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AutoScalePolicyOverride;
import org.apache.pulsar.common.scalable.SegmentInfo;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.impl.LocalMemoryMetadataStore;
import org.testng.annotations.Test;

/** CAS and identity invariants of a reserved layout; physical recovery is covered by V5 tests. */
public class ScalableTopicLayoutTransitionTest {

    @Test
    public void testReservationIsIdempotentAndDoesNotPublishTheSuccessors() throws Exception {
        @Cleanup
        Fixture fixture = new Fixture(initialMetadata());
        PendingLayoutTransition plan = plan(fixture.initial, UUID.randomUUID().toString());
        fixture.reserve(plan);
        fixture.reserve(plan);

        ScalableTopicMetadata reserved = fixture.read();
        assertThat(reserved.getEpoch()).isEqualTo(fixture.initial.getEpoch());
        assertThat(reserved.getNextSegmentId()).isEqualTo(fixture.initial.getNextSegmentId());
        assertThat(reserved.getSegments()).isEqualTo(fixture.initial.getSegments());
        assertThat(reserved.getPendingLayoutTransition()).isEqualTo(plan);
        assertRejected(fixture.resources.reserveLayoutTransitionAsync(fixture.topic,
                plan(fixture.initial, UUID.randomUUID().toString()), () -> true));
        assertThat(fixture.read()).isEqualTo(reserved);
    }

    @Test
    public void testCommitPreservesConcurrentPropertiesPolicyAndRecordIdentity() throws Exception {
        @Cleanup
        Fixture fixture = new Fixture(initialMetadata());
        PendingLayoutTransition plan = plan(fixture.initial, UUID.randomUUID().toString());
        fixture.reserve(plan);
        AutoScalePolicyOverride policy = AutoScalePolicyOverride.builder().enabled(false).build();
        // A generic writer's projection may predate the private identity/recovery fields.
        fixture.resources.updateScalableTopicAsync(fixture.topic, md -> md.toBuilder()
                .properties(Map.of("concurrent", "value")).autoScalePolicy(policy)
                .incarnationId(null).pendingLayoutTransition(null).build()).get(10, TimeUnit.SECONDS);
        assertThat(fixture.read().getPendingLayoutTransition()).isEqualTo(plan);
        fixture.commit(plan);

        ScalableTopicMetadata committed = fixture.read();
        assertThat(committed.getEpoch()).isEqualTo(plan.baseEpoch() + 1);
        assertThat(committed.getNextSegmentId()).isEqualTo(plan.nextSegmentId());
        assertThat(committed.getSegments()).containsAllEntriesOf(plan.replacements());
        assertThat(committed.getProperties()).containsExactlyInAnyOrderEntriesOf(Map.of("concurrent", "value"));
        assertThat(committed.getAutoScalePolicy()).isEqualTo(policy);
        assertThat(committed.getIncarnationId()).isEqualTo(fixture.initial.getIncarnationId());
        assertThat(committed.getPendingLayoutTransition()).isNull();
        assertRejected(fixture.resources.commitLayoutTransitionAsync(fixture.topic, plan, () -> true));
        assertThat(fixture.read()).as("a stale completion cannot publish a second generation").isEqualTo(committed);
    }

    @Test
    public void testGenericLayoutMutationCannotOverwriteAReservedPlan() throws Exception {
        @Cleanup
        Fixture fixture = new Fixture(initialMetadata());
        PendingLayoutTransition plan = plan(fixture.initial, UUID.randomUUID().toString());
        fixture.reserve(plan);
        ScalableTopicMetadata reserved = fixture.read();

        assertRejected(fixture.resources.updateScalableTopicAsync(fixture.topic,
                md -> SegmentLayout.fromMetadata(md).splitSegment(0, plan.startedAtMs()).toMetadata(md)));
        assertThat(fixture.read()).isEqualTo(reserved);
        PendingLayoutTransition wrongId = plan(fixture.initial, UUID.randomUUID().toString());
        assertRejected(fixture.resources.commitLayoutTransitionAsync(fixture.topic, wrongId, () -> true));
        assertThat(fixture.read()).as("a completion for another UUID cannot clear the active intent")
                .isEqualTo(reserved);
        fixture.commit(plan);
    }

    @Test
    public void testStalePlanCannotReserveOrCommitOnAnIdenticalRecreatedTopic() throws Exception {
        @Cleanup
        Fixture fixture = new Fixture(initialMetadata());
        PendingLayoutTransition stale = plan(fixture.initial, UUID.randomUUID().toString());
        fixture.reserve(stale);
        fixture.resources.deleteScalableTopicAsync(fixture.topic).get(10, TimeUnit.SECONDS);
        // Reuse every source descriptor, epoch and ID. Only this record's lifetime changes.
        ScalableTopicMetadata recreated = fixture.initial.toBuilder().incarnationId(UUID.randomUUID().toString())
                .build();
        fixture.resources.createScalableTopicAsync(fixture.topic, recreated).get(10, TimeUnit.SECONDS);
        assertThat(fixture.read().getSegments()).isEqualTo(fixture.initial.getSegments());

        assertRejected(fixture.resources.reserveLayoutTransitionAsync(fixture.topic, stale, () -> true));
        assertRejected(fixture.resources.commitLayoutTransitionAsync(fixture.topic, stale, () -> true));
        assertThat(fixture.read()).as("callbacks from the deleted lifetime leave the new record intact")
                .isEqualTo(recreated);
        PendingLayoutTransition fresh = plan(recreated, UUID.randomUUID().toString());
        fixture.reserve(fresh);
        assertRejected(fixture.resources.commitLayoutTransitionAsync(fixture.topic, stale, () -> true));
        assertThat(fixture.read().getPendingLayoutTransition()).isEqualTo(fresh);
        fixture.commit(fresh);
        assertThat(fixture.read().getIncarnationId()).isEqualTo(recreated.getIncarnationId());
    }

    @Test
    public void testEndedLeadershipCannotReserveOrCommit() throws Exception {
        @Cleanup
        Fixture fixture = new Fixture(initialMetadata());
        PendingLayoutTransition plan = plan(fixture.initial, UUID.randomUUID().toString());
        AtomicBoolean currentTerm = new AtomicBoolean();
        assertRejected(fixture.resources.reserveLayoutTransitionAsync(fixture.topic, plan, currentTerm::get));
        currentTerm.set(true);
        fixture.resources.reserveLayoutTransitionAsync(fixture.topic, plan, currentTerm::get)
                .get(10, TimeUnit.SECONDS);
        ScalableTopicMetadata reserved = fixture.read();
        currentTerm.set(false);
        assertRejected(fixture.resources.commitLayoutTransitionAsync(fixture.topic, plan, currentTerm::get));
        assertThat(fixture.read()).isEqualTo(reserved);
        // The next elected leader is allowed to finish the exact persisted reservation.
        fixture.commit(plan);
        assertThat(fixture.read().getEpoch()).isEqualTo(plan.baseEpoch() + 1);
    }

    @Test
    public void testLegacyRecordGetsOneStableIncarnationBeforeServing() throws Exception {
        @Cleanup
        Fixture fixture = new Fixture(initialMetadata().toBuilder().incarnationId(null).build());
        String first = fixture.resources.ensureScalableTopicIncarnationAsync(fixture.topic, () -> true)
                .get(10, TimeUnit.SECONDS);
        String next = fixture.resources.ensureScalableTopicIncarnationAsync(fixture.topic, () -> true)
                .get(10, TimeUnit.SECONDS);
        assertThat(first).isNotBlank().isEqualTo(next);
        assertThat(fixture.read().getEpoch()).isEqualTo(fixture.initial.getEpoch());
        assertThat(fixture.read().getSegments()).isEqualTo(fixture.initial.getSegments());
    }

    @Test
    public void testLegacyJsonWithoutRecoveryFieldsCanStillBeRead() throws Exception {
        ScalableTopicMetadata legacy = initialMetadata().withoutRecoveryMetadata();
        byte[] value = ObjectMapperFactory.getMapper().writer().writeValueAsBytes(legacy);
        JsonNode json = ObjectMapperFactory.getMapper().reader().readTree(value);
        assertThat(json.has("incarnationId")).isFalse();
        assertThat(json.has("pendingLayoutTransition")).isFalse();

        ScalableTopicMetadata decoded = ObjectMapperFactory.getMapper().reader()
                .forType(ScalableTopicMetadata.class).readValue(value);
        assertThat(decoded).isEqualTo(legacy);
        assertThat(decoded.getIncarnationId()).isNull();
        assertThat(decoded.getPendingLayoutTransition()).isNull();
    }

    @Test
    public void testAdminProjectionOmitsRecoveryFieldsWithoutMutatingTheStoredPlan() throws Exception {
        ScalableTopicMetadata original = initialMetadata();
        PendingLayoutTransition plan = plan(original, UUID.randomUUID().toString());
        ScalableTopicMetadata reserved = original.toBuilder().pendingLayoutTransition(plan).build();
        byte[] stored = ObjectMapperFactory.getMapper().writer().writeValueAsBytes(reserved);
        ScalableTopicMetadata roundTrip = ObjectMapperFactory.getMapper().reader()
                .forType(ScalableTopicMetadata.class).readValue(stored);
        assertThat(roundTrip).as("a broker can deserialize the fixed reservation after restart").isEqualTo(reserved);

        ScalableTopicMetadata projected = reserved.withoutRecoveryMetadata();
        JsonNode json = ObjectMapperFactory.getMapper().reader()
                .readTree(ObjectMapperFactory.getMapper().writer().writeValueAsBytes(projected));
        assertThat(json.has("incarnationId")).as("private lifetime identity is absent from REST JSON").isFalse();
        assertThat(json.has("pendingLayoutTransition")).as("private recovery plan is absent from REST JSON").isFalse();
        assertThat(projected.getEpoch()).isEqualTo(original.getEpoch());
        assertThat(projected.getNextSegmentId()).isEqualTo(original.getNextSegmentId());
        assertThat(projected.getSegments()).isEqualTo(original.getSegments());
        assertThat(reserved.getPendingLayoutTransition()).isEqualTo(plan);
        assertThat(reserved.getIncarnationId()).isEqualTo(original.getIncarnationId());
    }

    private static ScalableTopicMetadata initialMetadata() {
        return ScalableTopicController.createInitialMetadata(1, 1, Map.of());
    }

    private static PendingLayoutTransition plan(ScalableTopicMetadata original, String id) {
        long now = original.getSegments().get(0L).createdAtMs() + 1000;
        SegmentLayout planned = SegmentLayout.fromMetadata(original).splitSegment(0, now);
        Map<Long, SegmentInfo> changed = new LinkedHashMap<>();
        planned.getAllSegments().forEach((segmentId, segment) -> {
            if (!segment.equals(original.getSegments().get(segmentId))) {
                changed.put(segmentId, segment);
            }
        });
        return new PendingLayoutTransition(id, original.getIncarnationId(), "split", "manual", original.getEpoch(),
                original.getNextSegmentId(), now, Map.of(0L, original.getSegments().get(0L)), changed,
                planned.getNextSegmentId());
    }

    private static void assertRejected(CompletableFuture<?> result) {
        assertThatThrownBy(() -> result.get(10, TimeUnit.SECONDS)).hasRootCauseInstanceOf(IllegalStateException.class);
    }

    private static class Fixture implements AutoCloseable {
        private final TopicName topic = TopicName.get("topic://tenant/ns/layout-transition");
        private final MetadataStoreExtended store;
        private final ScalableTopicResources resources;
        private final ScalableTopicMetadata initial;

        Fixture(ScalableTopicMetadata initial) throws Exception {
            this.initial = initial;
            store = new LocalMemoryMetadataStore("memory:local", MetadataStoreConfig.builder().build());
            resources = new ScalableTopicResources(store, 10);
            resources.createScalableTopicAsync(topic, initial).get(10, TimeUnit.SECONDS);
        }

        ScalableTopicMetadata read() throws Exception {
            return resources.getScalableTopicMetadataAsync(topic, true).get(10, TimeUnit.SECONDS).orElseThrow();
        }

        void reserve(PendingLayoutTransition plan) throws Exception {
            resources.reserveLayoutTransitionAsync(topic, plan, () -> true).get(10, TimeUnit.SECONDS);
        }

        void commit(PendingLayoutTransition plan) throws Exception {
            resources.commitLayoutTransitionAsync(topic, plan, () -> true).get(10, TimeUnit.SECONDS);
        }

        @Override
        public void close() throws Exception {
            store.close();
        }
    }
}
