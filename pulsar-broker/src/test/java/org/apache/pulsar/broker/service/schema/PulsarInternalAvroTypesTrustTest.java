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
package org.apache.pulsar.broker.service.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.avro.util.ClassSecurityValidator;
import org.apache.avro.util.ClassSecurityValidator.ClassSecurityPredicate;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateData;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLoadData;
import org.apache.pulsar.broker.loadbalance.extensions.data.TopBundlesLoadData;
import org.apache.pulsar.broker.transaction.buffer.metadata.AbortTxnMetadata;
import org.apache.pulsar.broker.transaction.buffer.metadata.TransactionBufferSnapshot;
import org.apache.pulsar.broker.transaction.buffer.metadata.v2.TransactionBufferSnapshotIndexes;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.PulsarAvroClassSecurity;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.events.ActionType;
import org.apache.pulsar.common.events.EventType;
import org.apache.pulsar.common.events.PulsarEvent;
import org.apache.pulsar.common.events.TopicPoliciesEvent;
import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SubscribeRate;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.policies.data.impl.BacklogQuotaImpl;
import org.apache.pulsar.common.policies.data.impl.DispatchRateImpl;
import org.apache.pulsar.metadata.api.MetadataEvent;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Verifies that {@link PulsarAvroClassSecurity} on its own is enough to serialize every type Pulsar
 * writes to its system topics.
 *
 * <p>The Gradle test JVM trusts the whole org.apache.pulsar namespace through
 * org.apache.avro.SERIALIZABLE_PACKAGES (see pulsar.java-conventions.gradle.kts), which would hide a
 * missing entry in the allow-list. These tests therefore drop the global validator down to Avro's
 * hardcoded {@code DEFAULT_TRUSTED_CLASSES} — the baseline of a production JVM that sets no Avro system
 * properties — and then install Pulsar's own trust on top of it.
 */
@Test(groups = "broker")
public class PulsarInternalAvroTypesTrustTest {

    private ClassSecurityPredicate previousValidator;

    @BeforeMethod
    public void useProductionBaselineValidator() {
        previousValidator = ClassSecurityValidator.getGlobal();
        // DEFAULT_TRUSTED_CLASSES excludes the system-properties predicate, unlike DEFAULT.
        ClassSecurityValidator.setGlobal(ClassSecurityValidator.DEFAULT_TRUSTED_CLASSES);
        PulsarAvroClassSecurity.install();
    }

    @AfterMethod(alwaysRun = true)
    public void restoreValidator() {
        ClassSecurityValidator.setGlobal(previousValidator);
    }

    @Test
    public void testMetadataEventRoundTrips() {
        // Published by PulsarMetadataEventSynchronizer. Exercises the HashSet "java-class" property and
        // the CreateOption / NotificationType enums.
        MetadataEvent event = new MetadataEvent("/path", "value".getBytes(), new HashSet<>(List.of(
                CreateOption.Ephemeral)), 1L, 2L, "cluster", NotificationType.Created);

        Schema<MetadataEvent> schema = Schema.AVRO(MetadataEvent.class);

        assertThat(schema.decode(schema.encode(event))).isEqualTo(event);
    }

    @Test
    public void testPulsarEventRoundTrips() {
        // Published to the __change_events system topic, and reaches TopicPolicies through
        // TopicPoliciesEvent.
        PulsarEvent event = PulsarEvent.builder()
                .eventType(EventType.TOPIC_POLICY)
                .actionType(ActionType.UPDATE)
                .replicateTo(new HashSet<>(List.of("cluster-a")))
                .topicPoliciesEvent(TopicPoliciesEvent.builder()
                        .domain("persistent")
                        .tenant("public")
                        .namespace("default")
                        .topic("t1")
                        // Populate the nested types too: Avro only resolves the class behind a schema
                        // node when it actually writes it, so leaving these null would skip the enum
                        // and the policy impl types entirely and hide a missing allow-list entry.
                        .policies(TopicPolicies.builder()
                                .subscriptionTypesEnabled(List.of(SubType.Shared, SubType.Key_Shared))
                                .replicationClusters(List.of("cluster-a"))
                                .backLogQuotaMap(Map.of("destination_storage",
                                        (BacklogQuotaImpl) BacklogQuotaImpl.builder().limitSize(1024L).build()))
                                .dispatchRate(DispatchRateImpl.builder().dispatchThrottlingRateInMsg(10).build())
                                .retentionPolicies(new RetentionPolicies(1, 2))
                                .persistence(new PersistencePolicies(1, 1, 1, 1.0))
                                .inactiveTopicPolicies(new InactiveTopicPolicies(
                                        InactiveTopicDeleteMode.delete_when_no_subscriptions, 60, true))
                                .publishRate(new PublishRate(10, 1024))
                                .subscribeRate(new SubscribeRate(10, 30))
                                .build())
                        .build())
                .build();

        Schema<PulsarEvent> schema = Schema.AVRO(PulsarEvent.class);

        assertThat(schema.decode(schema.encode(event))).isEqualTo(event);
    }

    @Test
    public void testTransactionBufferSnapshotRoundTrips() {
        // The original snapshot format lives in ...buffer.metadata, not ...buffer.metadata.v2, so it is
        // only covered if the parent package is trusted.
        TransactionBufferSnapshot snapshot = new TransactionBufferSnapshot();
        snapshot.setTopicName("persistent://public/default/t1");
        snapshot.setMaxReadPositionLedgerId(1L);
        snapshot.setMaxReadPositionEntryId(2L);
        AbortTxnMetadata aborted = new AbortTxnMetadata();
        aborted.setTxnIdMostBits(3L);
        aborted.setTxnIdLeastBits(4L);
        snapshot.setAborts(List.of(aborted));

        Schema<TransactionBufferSnapshot> schema = Schema.AVRO(TransactionBufferSnapshot.class);

        // These snapshot classes do not define equals(), so compare field by field.
        assertThat(schema.decode(schema.encode(snapshot)))
                .usingRecursiveComparison().isEqualTo(snapshot);
    }

    @Test
    public void testTransactionBufferSnapshotIndexesRoundTrip() {
        TransactionBufferSnapshotIndexes indexes = new TransactionBufferSnapshotIndexes();
        indexes.setTopicName("persistent://public/default/t1");
        indexes.setIndexList(List.of());

        Schema<TransactionBufferSnapshotIndexes> schema = Schema.AVRO(TransactionBufferSnapshotIndexes.class);

        assertThat(schema.decode(schema.encode(indexes)))
                .usingRecursiveComparison().isEqualTo(indexes);
    }

    @Test
    public void testJsonSchemaOverBrokerInternalTypesRoundTrips() {
        // Schema.JSON also derives an Avro schema from the POJO, but reads and writes it with Jackson
        // rather than ReflectDatumReader/Writer, so it does not resolve classes reflectively and does not
        // need its types in the allow-list. These are the broker's own JSON-schema types, used by the
        // extensible load balancer; pin the behaviour so a future change to the JSON read/write path
        // cannot start requiring trust without a test noticing.
        Schema<ServiceUnitStateData> unitStateSchema = Schema.JSON(ServiceUnitStateData.class);
        ServiceUnitStateData unitState = new ServiceUnitStateData(
                ServiceUnitState.Owned, "dst-broker", "src-broker", 1L);
        assertThat(unitStateSchema.decode(unitStateSchema.encode(unitState)))
                .usingRecursiveComparison().isEqualTo(unitState);

        Schema<BrokerLoadData> brokerLoadSchema = Schema.JSON(BrokerLoadData.class);
        assertThat(brokerLoadSchema.decode(brokerLoadSchema.encode(new BrokerLoadData()))).isNotNull();

        Schema<TopBundlesLoadData> topBundlesSchema = Schema.JSON(TopBundlesLoadData.class);
        assertThat(topBundlesSchema.decode(topBundlesSchema.encode(new TopBundlesLoadData()))).isNotNull();
    }

    @Test
    public void testApplicationClassesRemainUntrusted() {
        // Pulsar widens the trusted set only for its own types; application POJOs stay the
        // application's responsibility, which is what the upgrade notes tell users.
        Schema<UntrustedPojo> schema = Schema.AVRO(UntrustedPojo.class);

        assertThatThrownBy(() -> schema.encode(new UntrustedPojo()))
                .hasRootCauseInstanceOf(SecurityException.class);
    }

    /** Stands in for a user-supplied POJO that Pulsar does not trust on its behalf. */
    public static class UntrustedPojo {
        private String field = "value";

        public String getField() {
            return field;
        }

        public void setField(String field) {
            this.field = field;
        }
    }
}
