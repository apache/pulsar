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

import org.apache.pulsar.client.schema.AvroTrustedClasses;
import org.apache.pulsar.common.classification.InterfaceAudience;

/**
 * Declares the types the broker itself serializes with Avro, so Avro will reflect over them.
 *
 * <p>Avro 1.12.2 resolves a class for every named type in a schema, on both the write and the read
 * path, and refuses any class that is not explicitly trusted. The broker writes several of its own
 * record types to system topics, so without this the broker cannot read or write its own state.
 *
 * <p>This covers the broker's types only. Application POJOs are declared by the application through
 * {@link AvroTrustedClasses}, and the code a Pulsar Function or connector deploys is covered by
 * trusting its class loader.
 */
@InterfaceAudience.Private
public final class BrokerAvroTrustedClasses {

    private BrokerAvroTrustedClasses() {
    }

    /**
     * Declares the broker's own Avro-serialized types. Idempotent, and cheap to call again: after the
     * first call each declaration is a no-op set insertion.
     */
    public static void trustBrokerTypes() {
        AvroTrustedClasses.trustPackages(
                // Transaction buffer snapshots, written to system topics by
                // TransactionBufferSnapshotBaseSystemTopicClient. Sub-package matching covers both the
                // original format (TransactionBufferSnapshot, AbortTxnMetadata) and the .v2 segmented one.
                "org.apache.pulsar.broker.transaction.buffer.metadata",
                // Topic policy events (PulsarEvent, TopicPoliciesEvent, EventType, ActionType)
                // published to the __change_events system topic.
                "org.apache.pulsar.common.events",
                // TopicPolicies and the policy value types it embeds, reached from TopicPoliciesEvent.
                // Sub-package matching also covers org.apache.pulsar.common.policies.data.impl.
                "org.apache.pulsar.common.policies.data");
        AvroTrustedClasses.trustClasses(
                // Replicated by PulsarMetadataEventSynchronizer via Schema.AVRO(MetadataEvent.class).
                "org.apache.pulsar.metadata.api.MetadataEvent",
                "org.apache.pulsar.metadata.api.NotificationType",
                "org.apache.pulsar.metadata.api.extended.CreateOption",
                // Element type of TopicPolicies.subscriptionTypesEnabled. This is a lightproto-generated
                // nested enum, so the name must be the binary one with '$': Avro derives the schema name
                // "...CommandSubscribe.SubType" but resolves it by retrying with '$' separators, and the
                // validator matches on Class.getName(). Trusted per class rather than by package, since
                // org.apache.pulsar.common.api.proto is the whole wire-protocol surface.
                "org.apache.pulsar.common.api.proto.CommandSubscribe$SubType",
                // ReflectData records the *declared* collection type as a "java-class" property on the
                // array or map schema it generates for a field, then resolves it reflectively on both
                // read and write. These are the three declared in the types above: List in the snapshot
                // and policy types, HashSet in MetadataEvent.options and PulsarEvent.replicateTo, and
                // Map in TopicPolicies.
                "java.util.List",
                "java.util.Map",
                "java.util.HashSet");
    }
}
