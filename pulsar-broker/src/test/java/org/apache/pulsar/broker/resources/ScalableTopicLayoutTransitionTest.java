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
import java.util.function.BooleanSupplier;
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
        assertRejected(fixture.commitAsync(plan, () -> true));
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
        assertRejected(fixture.commitAsync(wrongId, () -> true));
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
        assertRejected(fixture.commitAsync(stale, () -> true));
        assertThat(fixture.read()).as("callbacks from the deleted lifetime leave the new record intact")
                .isEqualTo(recreated);
        PendingLayoutTransition fresh = plan(recreated, UUID.randomUUID().toString());
        fixture.reserve(fresh);
        assertRejected(fixture.commitAsync(stale, () -> true));
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
        assertRejected(fixture.commitAsync(plan, currentTerm::get));
        assertThat(fixture.read()).isEqualTo(reserved);
        // The next elected leader is allowed to finish the exact persisted reservation.
        fixture.commit(plan);
        assertThat(fixture.read().getEpoch()).isEqualTo(plan.baseEpoch() + 1);
    }

    @Test
    public void testUnsealedPlanCanBeCancelledWithoutReusingItsSuccessorIds() throws Exception {
        @Cleanup
        Fixture fixture = new Fixture(initialMetadata());
        PendingLayoutTransition plan = plan(fixture.initial, UUID.randomUUID().toString());
        fixture.reserve(plan);
        fixture.resources.cancelLayoutTransitionAsync(fixture.topic, plan, () -> true).get(10, TimeUnit.SECONDS);

        ScalableTopicMetadata cancelled = fixture.read();
        assertThat(cancelled.getPendingLayoutTransition()).isNull();
        assertThat(cancelled.getEpoch()).as("the published DAG is unchanged").isEqualTo(fixture.initial.getEpoch());
        assertThat(cancelled.getSegments()).isEqualTo(fixture.initial.getSegments());
        assertThat(cancelled.getNextSegmentId())
                .as("successor topics of the cancelled plan may exist: their IDs are not reused")
                .isEqualTo(plan.nextSegmentId());
        // A replay of the cancelled plan by an older leader can neither seal nor publish it.
        assertRejected(fixture.resources.markLayoutTransitionSealingAsync(fixture.topic, plan, () -> true));
        assertRejected(fixture.commitAsync(plan, () -> true));
        assertRejected(fixture.resources.reserveLayoutTransitionAsync(fixture.topic, plan, () -> true));
        assertThat(fixture.read()).isEqualTo(cancelled);

        PendingLayoutTransition next = plan(cancelled, UUID.randomUUID().toString());
        assertThat(next.replacements().keySet()).as("successor IDs of the next plan")
                .doesNotContainAnyElementsOf(plan.replacements().keySet().stream()
                        .filter(id -> !plan.sources().containsKey(id)).toList());
        fixture.reserve(next);
        fixture.commit(next);
        assertThat(fixture.read().getEpoch()).isEqualTo(fixture.initial.getEpoch() + 1);
    }

    @Test
    public void testCommitRecordsTheSourceAsSealedWhenItsTerminationWasConfirmed() throws Exception {
        @Cleanup
        Fixture fixture = new Fixture(initialMetadata());
        PendingLayoutTransition plan = plan(fixture.initial, UUID.randomUUID().toString());
        fixture.reserve(plan);
        // A delayed recovery terminated the source long after the plan was made.
        long terminatedAtMs = plan.startedAtMs() + TimeUnit.MINUTES.toMillis(10);
        fixture.resources.commitLayoutTransitionAsync(fixture.topic, plan, terminatedAtMs, () -> true)
                .get(10, TimeUnit.SECONDS);

        ScalableTopicMetadata committed = fixture.read();
        SegmentInfo planned = plan.replacements().get(0L);
        assertThat(committed.getSegments().get(0L))
                .isEqualTo(planned.sealed(planned.sealedAtEpoch(), terminatedAtMs, planned.childIds()));
        plan.replacements().forEach((id, segment) -> {
            if (id != 0L) {
                assertThat(committed.getSegments().get(id)).as("successors are published as planned")
                        .isEqualTo(segment);
            }
        });
    }

    @Test
    public void testCommitKeepsTheSealTimeOfThePlanIfTheTerminationTimeIsEarlier() throws Exception {
        @Cleanup
        Fixture fixture = new Fixture(initialMetadata());
        PendingLayoutTransition plan = plan(fixture.initial, UUID.randomUUID().toString());
        fixture.reserve(plan);
        // The clock of the broker that terminated the source is behind the one that made the plan.
        fixture.resources.commitLayoutTransitionAsync(fixture.topic, plan, plan.startedAtMs() - 1000, () -> true)
                .get(10, TimeUnit.SECONDS);

        assertThat(fixture.read().getSegments().get(0L)).isEqualTo(plan.replacements().get(0L));
    }

    @Test
    public void testPlanThatStartedSealingCannotBeCancelled() throws Exception {
        @Cleanup
        Fixture fixture = new Fixture(initialMetadata());
        PendingLayoutTransition plan = plan(fixture.initial, UUID.randomUUID().toString());
        fixture.reserve(plan);
        PendingLayoutTransition sealing = fixture.resources.markLayoutTransitionSealingAsync(fixture.topic, plan,
                () -> true).get(10, TimeUnit.SECONDS);
        assertThat(sealing).isEqualTo(plan.withSealing());
        assertThat(sealing.sealing()).isTrue();
        assertThat(fixture.resources.markLayoutTransitionSealingAsync(fixture.topic, plan, () -> true)
                .get(10, TimeUnit.SECONDS)).as("marking is idempotent").isEqualTo(sealing);
        ScalableTopicMetadata marked = fixture.read();
        assertThat(marked.getPendingLayoutTransition()).isEqualTo(sealing);
        assertThat(marked.getEpoch()).isEqualTo(fixture.initial.getEpoch());

        assertRejected(fixture.resources.cancelLayoutTransitionAsync(fixture.topic, plan, () -> true));
        assertRejected(fixture.resources.cancelLayoutTransitionAsync(fixture.topic, sealing, () -> true));
        assertThat(fixture.read()).as("a source may be sealed: only completing the plan is safe").isEqualTo(marked);
        fixture.commit(sealing);
        assertThat(fixture.read().getEpoch()).isEqualTo(plan.baseEpoch() + 1);
        assertThat(fixture.read().getPendingLayoutTransition()).isNull();
    }

    @Test
    public void testEndedLeadershipCannotMarkOrCancel() throws Exception {
        @Cleanup
        Fixture fixture = new Fixture(initialMetadata());
        PendingLayoutTransition plan = plan(fixture.initial, UUID.randomUUID().toString());
        fixture.reserve(plan);
        ScalableTopicMetadata reserved = fixture.read();
        assertRejected(fixture.resources.markLayoutTransitionSealingAsync(fixture.topic, plan, () -> false));
        assertRejected(fixture.resources.cancelLayoutTransitionAsync(fixture.topic, plan, () -> false));
        assertThat(fixture.read()).isEqualTo(reserved);
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
                planned.getNextSegmentId(), false);
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
            commitAsync(plan, () -> true).get(10, TimeUnit.SECONDS);
        }

        // Its sources were terminated when the plan was made.
        CompletableFuture<Void> commitAsync(PendingLayoutTransition plan, BooleanSupplier currentTerm) {
            return resources.commitLayoutTransitionAsync(topic, plan, plan.startedAtMs(), currentTerm);
        }

        @Override
        public void close() throws Exception {
            store.close();
        }
    }
}
