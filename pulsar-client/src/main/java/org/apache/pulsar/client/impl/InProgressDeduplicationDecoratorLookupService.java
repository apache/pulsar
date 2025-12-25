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
package org.apache.pulsar.client.impl;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace.Mode;
import org.apache.pulsar.common.lookup.GetTopicsResult;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.schema.SchemaInfo;

/**
 * Decorator for {@link LookupService} that deduplicates in-progress lookups for topics, schemas, partitioned topics
 * and topic listings for namespace.
 */
public class InProgressDeduplicationDecoratorLookupService implements LookupService {
    private final LookupService delegate;
    private final Supplier<Map<String, String>> lookupPropertiesSupplier;
    private final InProgressHolder<LookupBrokerKey, CompletableFuture<LookupTopicResult>> topicLookupsInProgress =
            new InProgressHolder<>();
    private final InProgressHolder<PartitionedTopicMetadataKey, CompletableFuture<PartitionedTopicMetadata>>
            partitionedTopicMetadataInProgress = new InProgressHolder<>();
    private final InProgressHolder<LookupSchemaKey, CompletableFuture<Optional<SchemaInfo>>> schemasInProgress =
            new InProgressHolder<>();
    private final InProgressHolder<TopicsUnderNamespaceKey, CompletableFuture<GetTopicsResult>>
            topicsUnderNamespaceInProgress = new InProgressHolder<>();

    public InProgressDeduplicationDecoratorLookupService(LookupService delegate,
                                                         Supplier<Map<String, String>> lookupPropertiesSupplier) {
        this.delegate = delegate;
        this.lookupPropertiesSupplier = lookupPropertiesSupplier;
    }

    @Override
    public void updateServiceUrl(String serviceUrl) throws PulsarClientException {
        delegate.updateServiceUrl(serviceUrl);
    }

    @Override
    public String getServiceUrl() {
        return delegate.getServiceUrl();
    }

    @Override
    public InetSocketAddress resolveHost() {
        return delegate.resolveHost();
    }

    @Override
    public CompletableFuture<LookupTopicResult> getBroker(TopicName topicName) {
        Map<String, String> lookupPropertiesToUse = lookupPropertiesSupplier.get();
        return topicLookupsInProgress.getOrComputeIfAbsent(
                new LookupBrokerKey(topicName.toString(), lookupPropertiesToUse),
                () -> delegate.getBroker(topicName));
    }

    @Override
    public CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadata(
            TopicName topicName,
            boolean metadataAutoCreationEnabled,
            boolean useFallbackForNonPIP344Brokers) {
        return partitionedTopicMetadataInProgress.getOrComputeIfAbsent(
                new PartitionedTopicMetadataKey(topicName, metadataAutoCreationEnabled, useFallbackForNonPIP344Brokers),
                () -> delegate.getPartitionedTopicMetadata(topicName, metadataAutoCreationEnabled,
                        useFallbackForNonPIP344Brokers));
    }

    @Override
    public CompletableFuture<Optional<SchemaInfo>> getSchema(TopicName topicName) {
        // all partitions of a partitioned topic share the same schema
        // therefore, perform the lookup with the partitioned topic name
        String topicForSchemaLookup = topicName.getPartitionedTopicName();
        return schemasInProgress.getOrComputeIfAbsent(new LookupSchemaKey(topicForSchemaLookup, null),
                () -> delegate.getSchema(TopicName.get(topicForSchemaLookup)));
    }

    @Override
    public CompletableFuture<Optional<SchemaInfo>> getSchema(TopicName topicName, byte[] version) {
        // all partitions of a partitioned topic share the same schema
        // therefore, perform the lookup with the partitioned topic name
        String topicForSchemaLookup = topicName.getPartitionedTopicName();
        return schemasInProgress.getOrComputeIfAbsent(new LookupSchemaKey(topicForSchemaLookup, version),
                () -> delegate.getSchema(TopicName.get(topicForSchemaLookup), version));
    }

    @Override
    public CompletableFuture<GetTopicsResult> getTopicsUnderNamespace(NamespaceName namespace, Mode mode,
                                                                      String topicPattern, String topicsHash) {
        return topicsUnderNamespaceInProgress.getOrComputeIfAbsent(
                new TopicsUnderNamespaceKey(namespace, mode, topicPattern, topicsHash),
                () -> delegate.getTopicsUnderNamespace(namespace, mode, topicPattern, topicsHash));
    }

    @Override
    public void close() throws Exception {
        delegate.close();
    }

    private static class InProgressHolder<K, V extends CompletableFuture<?>> {
        private final ConcurrentHashMap<K, V> inProgress = new ConcurrentHashMap<>();

        public V getOrComputeIfAbsent(K key, Supplier<V> supplier) {
            final MutableObject<V> newFutureCreated = new MutableObject<>();
            try {
                return inProgress.computeIfAbsent(key, k -> {
                    V newFuture = supplier.get();
                    newFutureCreated.setValue(newFuture);
                    return newFuture;
                });
            } finally {
                V newFutureCreatedValue = newFutureCreated.getValue();
                if (newFutureCreatedValue != null) {
                    newFutureCreatedValue.whenComplete((v, ex) -> {
                        inProgress.remove(key, newFutureCreatedValue);
                    });
                }
            }
        }
    }

    private static final class LookupBrokerKey {
        private final String topic;
        private final Map<String, String> properties;

        private LookupBrokerKey(String topic, Map<String, String> properties) {
            this.topic = topic;
            this.properties = properties.isEmpty() ? Collections.emptyMap() : new HashMap<>(properties);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            LookupBrokerKey lookupBrokerKey = (LookupBrokerKey) o;
            return Objects.equals(topic, lookupBrokerKey.topic) && properties.equals(lookupBrokerKey.properties);
        }

        @Override
        public int hashCode() {
            int result = Objects.hashCode(topic);
            result = 31 * result + properties.hashCode();
            return result;
        }
    }

    private static final class LookupSchemaKey {
        private final String topic;
        private final byte[] version;

        private LookupSchemaKey(String topic, byte[] version) {
            this.topic = topic;
            this.version = version != null ? version.clone() : null;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            LookupSchemaKey that = (LookupSchemaKey) o;
            return Objects.equals(topic, that.topic) && Arrays.equals(version, that.version);
        }

        @Override
        public int hashCode() {
            int result = Objects.hashCode(topic);
            result = 31 * result + Arrays.hashCode(version);
            return result;
        }
    }

    private static final class TopicsUnderNamespaceKey {
        private final NamespaceName namespace;
        private final Mode mode;
        private final String topicsPattern;
        private final String topicsHash;

        TopicsUnderNamespaceKey(NamespaceName namespace, Mode mode, String topicsPattern, String topicsHash) {
            this.namespace = namespace;
            this.mode = mode;
            this.topicsPattern = topicsPattern;
            this.topicsHash = topicsHash;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TopicsUnderNamespaceKey that = (TopicsUnderNamespaceKey) o;
            return Objects.equals(namespace, that.namespace) && mode == that.mode && Objects.equals(topicsPattern,
                    that.topicsPattern) && Objects.equals(topicsHash, that.topicsHash);
        }

        @Override
        public int hashCode() {
            return Objects.hash(namespace, mode, topicsPattern, topicsHash);
        }

        @Override
        public String toString() {
            return "TopicsUnderNamespaceKey{" + "namespace=" + namespace + ", mode=" + mode + ", topicsPattern='"
                    + topicsPattern + '\'' + ", topicsHash='" + topicsHash + '\'' + '}';
        }
    }

    private static final class PartitionedTopicMetadataKey {
        private final TopicName topicName;
        private final boolean metadataAutoCreationEnabled;
        private final boolean useFallbackForNonPIP344Brokers;

        PartitionedTopicMetadataKey(TopicName topicName, boolean metadataAutoCreationEnabled,
                                    boolean useFallbackForNonPIP344Brokers) {
            this.topicName = topicName;
            this.metadataAutoCreationEnabled = metadataAutoCreationEnabled;
            this.useFallbackForNonPIP344Brokers = useFallbackForNonPIP344Brokers;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PartitionedTopicMetadataKey that = (PartitionedTopicMetadataKey) o;
            return metadataAutoCreationEnabled == that.metadataAutoCreationEnabled
                    && useFallbackForNonPIP344Brokers == that.useFallbackForNonPIP344Brokers && Objects.equals(
                    topicName, that.topicName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topicName, metadataAutoCreationEnabled, useFallbackForNonPIP344Brokers);
        }

        @Override
        public String toString() {
            return "PartitionedTopicMetadataKey{" + "topicName=" + topicName + ", metadataAutoCreationEnabled="
                    + metadataAutoCreationEnabled + ", useFallbackForNonPIP344Brokers=" + useFallbackForNonPIP344Brokers
                    + '}';
        }
    }
}