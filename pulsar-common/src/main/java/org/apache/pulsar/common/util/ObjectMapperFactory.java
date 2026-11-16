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

package org.apache.pulsar.common.util;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.module.SimpleAbstractTypeResolver;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import lombok.CustomLog;
import lombok.Getter;
import org.apache.pulsar.client.admin.internal.data.AuthPoliciesImpl;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.FunctionState;
import org.apache.pulsar.common.functions.JsonIgnorePropertiesMixIn;
import org.apache.pulsar.common.policies.data.AuthPolicies;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyData;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyDataImpl;
import org.apache.pulsar.common.policies.data.AutoSubscriptionCreationOverride;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BookieAffinityGroupData;
import org.apache.pulsar.common.policies.data.BookieInfo;
import org.apache.pulsar.common.policies.data.BookiesClusterInfo;
import org.apache.pulsar.common.policies.data.BrokerInfo;
import org.apache.pulsar.common.policies.data.BrokerNamespaceIsolationData;
import org.apache.pulsar.common.policies.data.BrokerNamespaceIsolationDataImpl;
import org.apache.pulsar.common.policies.data.BrokerStatus;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.CompactionStats;
import org.apache.pulsar.common.policies.data.ConsumerStats;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.DrainingHash;
import org.apache.pulsar.common.policies.data.FailureDomain;
import org.apache.pulsar.common.policies.data.FailureDomainImpl;
import org.apache.pulsar.common.policies.data.FunctionInstanceStats;
import org.apache.pulsar.common.policies.data.FunctionInstanceStatsData;
import org.apache.pulsar.common.policies.data.FunctionInstanceStatsDataBase;
import org.apache.pulsar.common.policies.data.FunctionInstanceStatsDataBaseImpl;
import org.apache.pulsar.common.policies.data.FunctionInstanceStatsDataImpl;
import org.apache.pulsar.common.policies.data.FunctionInstanceStatsImpl;
import org.apache.pulsar.common.policies.data.FunctionStats;
import org.apache.pulsar.common.policies.data.FunctionStatsImpl;
import org.apache.pulsar.common.policies.data.NamespaceIsolationData;
import org.apache.pulsar.common.policies.data.NamespaceIsolationDataImpl;
import org.apache.pulsar.common.policies.data.NonPersistentPartitionedTopicStats;
import org.apache.pulsar.common.policies.data.NonPersistentPublisherStats;
import org.apache.pulsar.common.policies.data.NonPersistentReplicatorStats;
import org.apache.pulsar.common.policies.data.NonPersistentSubscriptionStats;
import org.apache.pulsar.common.policies.data.NonPersistentTopicStats;
import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.policies.data.PartitionedTopicStats;
import org.apache.pulsar.common.policies.data.PublisherStats;
import org.apache.pulsar.common.policies.data.ReplicatorStats;
import org.apache.pulsar.common.policies.data.ResourceQuota;
import org.apache.pulsar.common.policies.data.ResourceQuotaMixIn;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.policies.data.impl.AutoSubscriptionCreationOverrideImpl;
import org.apache.pulsar.common.policies.data.impl.AutoTopicCreationOverrideImpl;
import org.apache.pulsar.common.policies.data.impl.BacklogQuotaImpl;
import org.apache.pulsar.common.policies.data.impl.BookieAffinityGroupDataImpl;
import org.apache.pulsar.common.policies.data.impl.BookieInfoImpl;
import org.apache.pulsar.common.policies.data.impl.BookiesClusterInfoImpl;
import org.apache.pulsar.common.policies.data.impl.BrokerInfoImpl;
import org.apache.pulsar.common.policies.data.impl.BrokerStatusImpl;
import org.apache.pulsar.common.policies.data.impl.BundlesDataImpl;
import org.apache.pulsar.common.policies.data.impl.DelayedDeliveryPoliciesImpl;
import org.apache.pulsar.common.policies.data.impl.DispatchRateImpl;
import org.apache.pulsar.common.policies.data.stats.CompactionStatsImpl;
import org.apache.pulsar.common.policies.data.stats.ConsumerStatsImpl;
import org.apache.pulsar.common.policies.data.stats.DrainingHashImpl;
import org.apache.pulsar.common.policies.data.stats.NonPersistentPartitionedTopicStatsImpl;
import org.apache.pulsar.common.policies.data.stats.NonPersistentPublisherStatsImpl;
import org.apache.pulsar.common.policies.data.stats.NonPersistentReplicatorStatsImpl;
import org.apache.pulsar.common.policies.data.stats.NonPersistentSubscriptionStatsImpl;
import org.apache.pulsar.common.policies.data.stats.NonPersistentTopicStatsImpl;
import org.apache.pulsar.common.policies.data.stats.PartitionedTopicStatsImpl;
import org.apache.pulsar.common.policies.data.stats.PublisherStatsImpl;
import org.apache.pulsar.common.policies.data.stats.ReplicatorStatsImpl;
import org.apache.pulsar.common.policies.data.stats.SubscriptionStatsImpl;
import org.apache.pulsar.common.policies.data.stats.TopicStatsImpl;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.common.stats.MetricsMixIn;
import org.apache.pulsar.policies.data.loadbalancer.LoadManagerReport;
import org.apache.pulsar.policies.data.loadbalancer.LoadReportDeserializer;

@SuppressWarnings("checkstyle:JavadocType")
@CustomLog
public class ObjectMapperFactory {
    public static class MapperReference {
        @Getter
        private final ObjectMapper objectMapper;
        private final ObjectWriter objectWriter;
        private final ObjectReader objectReader;

        MapperReference(ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
            this.objectWriter = objectMapper.writer();
            this.objectReader = objectMapper.reader();
        }

        public ObjectWriter writer() {
            return objectWriter;
        }

        public ObjectReader reader() {
            return objectReader;
        }
    }

    /**
     * The concrete class that Jackson instantiates when it deserializes a value declared as one of these
     * interfaces, in insertion order. Built before the mapper singletons below, which use it.
     */
    private static final Map<Class<?>, Class<?>> ABSTRACT_TYPE_MAPPINGS = createAbstractTypeMappings();

    private static final AtomicReference<MapperReference> MAPPER_REFERENCE =
            new AtomicReference<>(new MapperReference(createObjectMapperInstance()));

    private static final AtomicReference<MapperReference> INSTANCE_WITH_INCLUDE_ALWAYS =
            new AtomicReference<>(new MapperReference(createObjectMapperWithIncludeAlways()));

    private static final AtomicReference<MapperReference> YAML_MAPPER_REFERENCE =
            new AtomicReference<>(new MapperReference(createYamlInstance()));

    private static ObjectMapper createObjectMapperInstance() {
        return ProtectedObjectMapper.protectedCopyOf(configureObjectMapper(new ObjectMapper()));
    }

    private static ObjectMapper createObjectMapperWithIncludeAlways() {
        return MAPPER_REFERENCE
                .get().getObjectMapper().copy()
                .setDefaultPropertyInclusion(Include.ALWAYS);
    }

    public static ObjectMapper create() {
        return getMapper().getObjectMapper().copy();
    }

    private static ObjectMapper createYamlInstance() {
        return ProtectedObjectMapper.protectedCopyOf(configureObjectMapper(new ObjectMapper(new YAMLFactory())));
    }

    private static ObjectMapper configureObjectMapper(ObjectMapper mapper) {
        // forward compatibility for the properties may go away in the future
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
        mapper.setDefaultPropertyInclusion(Include.NON_NULL);

        // enable Jackson Java 8 support modules
        // https://github.com/FasterXML/jackson-modules-java8
        mapper.registerModule(new ParameterNamesModule())
            .registerModule(new Jdk8Module())
            .registerModule(new JavaTimeModule());

        setAnnotationsModule(mapper);
        return mapper;
    }

    public static ObjectMapper createYaml() {
        return getYamlMapper().getObjectMapper().copy();
    }

    public static MapperReference getMapper() {
        return MAPPER_REFERENCE.get();
    }


    public static MapperReference getMapperWithIncludeAlways() {
        return INSTANCE_WITH_INCLUDE_ALWAYS.get();
    }

    /**
     * This method is deprecated. Use {@link #getMapper()} and {@link MapperReference#getObjectMapper()}
     */
    @Deprecated
    public static ObjectMapper getThreadLocal() {
        return getMapper().getObjectMapper();
    }

    public static MapperReference getYamlMapper() {
        return YAML_MAPPER_REFERENCE.get();
    }

    /**
     * This method is deprecated. Use {@link #getYamlMapper()} and {@link MapperReference#getObjectMapper()}
     */
    @Deprecated
    public static ObjectMapper getThreadLocalYaml() {
        return getYamlMapper().getObjectMapper();
    }

    private static Map<Class<?>, Class<?>> createAbstractTypeMappings() {
        Map<Class<?>, Class<?>> mappings = new LinkedHashMap<>();
        put(mappings, AutoFailoverPolicyData.class, AutoFailoverPolicyDataImpl.class);
        put(mappings, BrokerNamespaceIsolationData.class, BrokerNamespaceIsolationDataImpl.class);
        put(mappings, BacklogQuota.class, BacklogQuotaImpl.class);
        put(mappings, ClusterData.class, ClusterDataImpl.class);
        put(mappings, FailureDomain.class, FailureDomainImpl.class);
        put(mappings, NamespaceIsolationData.class, NamespaceIsolationDataImpl.class);
        put(mappings, TenantInfo.class, TenantInfoImpl.class);
        put(mappings, OffloadPolicies.class, OffloadPoliciesImpl.class);
        put(mappings, FunctionStats.class, FunctionStatsImpl.class);
        put(mappings, FunctionInstanceStats.class, FunctionInstanceStatsImpl.class);
        put(mappings, FunctionInstanceStatsData.class, FunctionInstanceStatsDataImpl.class);
        put(mappings, FunctionInstanceStatsDataBase.class, FunctionInstanceStatsDataBaseImpl.class);
        put(mappings, BundlesData.class, BundlesDataImpl.class);
        put(mappings, BookieAffinityGroupData.class, BookieAffinityGroupDataImpl.class);
        put(mappings, AuthPolicies.class, AuthPoliciesImpl.class);
        put(mappings, AutoTopicCreationOverride.class, AutoTopicCreationOverrideImpl.class);
        put(mappings, BookieInfo.class, BookieInfoImpl.class);
        put(mappings, BookiesClusterInfo.class, BookiesClusterInfoImpl.class);
        put(mappings, BrokerInfo.class, BrokerInfoImpl.class);
        put(mappings, BrokerStatus.class, BrokerStatusImpl.class);
        put(mappings, DelayedDeliveryPolicies.class, DelayedDeliveryPoliciesImpl.class);
        put(mappings, DispatchRate.class, DispatchRateImpl.class);
        put(mappings, TopicStats.class, TopicStatsImpl.class);
        put(mappings, ConsumerStats.class, ConsumerStatsImpl.class);
        put(mappings, DrainingHash.class, DrainingHashImpl.class);
        put(mappings, NonPersistentPublisherStats.class, NonPersistentPublisherStatsImpl.class);
        put(mappings, NonPersistentReplicatorStats.class, NonPersistentReplicatorStatsImpl.class);
        put(mappings, NonPersistentSubscriptionStats.class, NonPersistentSubscriptionStatsImpl.class);
        put(mappings, NonPersistentTopicStats.class, NonPersistentTopicStatsImpl.class);
        put(mappings, PartitionedTopicStats.class, PartitionedTopicStatsImpl.class);
        put(mappings, NonPersistentPartitionedTopicStats.class, NonPersistentPartitionedTopicStatsImpl.class);
        put(mappings, PublisherStats.class, PublisherStatsImpl.class);
        put(mappings, ReplicatorStats.class, ReplicatorStatsImpl.class);
        put(mappings, SubscriptionStats.class, SubscriptionStatsImpl.class);
        put(mappings, AutoSubscriptionCreationOverride.class, AutoSubscriptionCreationOverrideImpl.class);
        put(mappings, CompactionStats.class, CompactionStatsImpl.class);
        return Collections.unmodifiableMap(mappings);
    }

    private static <T> void put(Map<Class<?>, Class<?>> mappings, Class<T> abstractType,
                                Class<? extends T> concreteType) {
        mappings.put(abstractType, concreteType);
    }

    @SuppressWarnings("unchecked") // createAbstractTypeMappings only adds subtypes of their key
    private static <T> void addMapping(SimpleAbstractTypeResolver resolver, Class<T> abstractType,
                                       Class<?> concreteType) {
        resolver.addMapping(abstractType, (Class<? extends T>) concreteType);
    }

    /**
     * Returns the interface-to-implementation mappings that the mappers created by this factory use to
     * deserialize values declared as an interface type, such as {@code TopicStats} to {@code TopicStatsImpl}.
     *
     * <p>The OpenAPI specification generator reads these too, so that it describes each interface with the
     * properties and JSON names of the implementation that is actually serialized.
     *
     * @return an unmodifiable map from each interface to its implementation class
     */
    public static Map<Class<?>, Class<?>> getAbstractTypeMappings() {
        return ABSTRACT_TYPE_MAPPINGS;
    }

    private static void setAnnotationsModule(ObjectMapper mapper) {
        SimpleModule module = new SimpleModule("AnnotationsModule");

        // we use customized deserializer to replace jackson annotations in some POJOs
        SimpleAbstractTypeResolver resolver = new SimpleAbstractTypeResolver();
        ABSTRACT_TYPE_MAPPINGS.forEach((abstractType, concreteType) ->
                addMapping(resolver, abstractType, concreteType));

        // we use MixIn class to add jackson annotations
        mapper.addMixIn(ResourceQuota.class, ResourceQuotaMixIn.class);
        mapper.addMixIn(FunctionConfig.class, JsonIgnorePropertiesMixIn.class);
        mapper.addMixIn(FunctionState.class, JsonIgnorePropertiesMixIn.class);
        mapper.addMixIn(Metrics.class, MetricsMixIn.class);

        module.addDeserializer(LoadManagerReport.class, new LoadReportDeserializer());

        module.setAbstractTypes(resolver);

        mapper.registerModule(module);
    }

    /**
     * Clears the caches tied to the ObjectMapper instances and replaces the singleton ObjectMapper instance.
     * This can be used in tests to ensure that classloaders and class references don't leak across tests.
     */
    public static void clearCaches() {
        clearTypeFactoryCache(getMapper().getObjectMapper());
        clearTypeFactoryCache(getYamlMapper().getObjectMapper());
        clearTypeFactoryCache(getMapperWithIncludeAlways().getObjectMapper());
        replaceSingletonInstances();
    }

    private static void clearTypeFactoryCache(ObjectMapper objectMapper) {
        objectMapper.getTypeFactory().clearCache();
    }

    /*
     * Replaces the existing singleton ObjectMapper instances with new instances.
     * This is used in tests to ensure that classloaders and class references don't leak between tests.
     */
    private static void replaceSingletonInstances() {
        MAPPER_REFERENCE.set(new MapperReference(createObjectMapperInstance()));
        INSTANCE_WITH_INCLUDE_ALWAYS.set(new MapperReference(createObjectMapperWithIncludeAlways()));
        YAML_MAPPER_REFERENCE.set(new MapperReference(createYamlInstance()));
    }
}