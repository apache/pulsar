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
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ClassUtils;
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
import org.apache.pulsar.common.policies.data.ConsumerStats;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.apache.pulsar.common.policies.data.DispatchRate;
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
import org.apache.pulsar.common.policies.data.stats.ConsumerStatsImpl;
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
@Slf4j
public class ObjectMapperFactory {
    public static class MapperReference {
        private final ObjectMapper objectMapper;
        private final ObjectWriter objectWriter;
        private final ObjectReader objectReader;

        MapperReference(ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
            this.objectWriter = objectMapper.writer();
            this.objectReader = objectMapper.reader();
        }

        public ObjectMapper getObjectMapper() {
            return objectMapper;
        }

        public ObjectWriter writer() {
            return objectWriter;
        }

        public ObjectReader reader() {
            return objectReader;
        }
    }

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
                .setSerializationInclusion(Include.ALWAYS);
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
        mapper.setSerializationInclusion(Include.NON_NULL);
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

    private static void setAnnotationsModule(ObjectMapper mapper) {
        SimpleModule module = new SimpleModule("AnnotationsModule");

        // we use customized deserializer to replace jackson annotations in some POJOs
        SimpleAbstractTypeResolver resolver = new SimpleAbstractTypeResolver();
        resolver.addMapping(AutoFailoverPolicyData.class, AutoFailoverPolicyDataImpl.class);
        resolver.addMapping(BrokerNamespaceIsolationData.class, BrokerNamespaceIsolationDataImpl.class);
        resolver.addMapping(BacklogQuota.class, BacklogQuotaImpl.class);
        resolver.addMapping(ClusterData.class, ClusterDataImpl.class);
        resolver.addMapping(FailureDomain.class, FailureDomainImpl.class);
        resolver.addMapping(NamespaceIsolationData.class, NamespaceIsolationDataImpl.class);
        resolver.addMapping(TenantInfo.class, TenantInfoImpl.class);
        resolver.addMapping(OffloadPolicies.class, OffloadPoliciesImpl.class);
        resolver.addMapping(FunctionStats.class, FunctionStatsImpl.class);
        resolver.addMapping(FunctionInstanceStats.class, FunctionInstanceStatsImpl.class);
        resolver.addMapping(FunctionInstanceStatsData.class, FunctionInstanceStatsDataImpl.class);
        resolver.addMapping(FunctionInstanceStatsDataBase.class, FunctionInstanceStatsDataBaseImpl.class);
        resolver.addMapping(BundlesData.class, BundlesDataImpl.class);
        resolver.addMapping(BookieAffinityGroupData.class, BookieAffinityGroupDataImpl.class);
        resolver.addMapping(AuthPolicies.class, AuthPoliciesImpl.class);
        resolver.addMapping(AutoTopicCreationOverride.class, AutoTopicCreationOverrideImpl.class);
        resolver.addMapping(BookieInfo.class, BookieInfoImpl.class);
        resolver.addMapping(BookiesClusterInfo.class, BookiesClusterInfoImpl.class);
        resolver.addMapping(BrokerInfo.class, BrokerInfoImpl.class);
        resolver.addMapping(BrokerStatus.class, BrokerStatusImpl.class);
        resolver.addMapping(DelayedDeliveryPolicies.class, DelayedDeliveryPoliciesImpl.class);
        resolver.addMapping(DispatchRate.class, DispatchRateImpl.class);
        resolver.addMapping(TopicStats.class, TopicStatsImpl.class);
        resolver.addMapping(ConsumerStats.class, ConsumerStatsImpl.class);
        resolver.addMapping(NonPersistentPublisherStats.class, NonPersistentPublisherStatsImpl.class);
        resolver.addMapping(NonPersistentReplicatorStats.class, NonPersistentReplicatorStatsImpl.class);
        resolver.addMapping(NonPersistentSubscriptionStats.class, NonPersistentSubscriptionStatsImpl.class);
        resolver.addMapping(NonPersistentTopicStats.class, NonPersistentTopicStatsImpl.class);
        resolver.addMapping(PartitionedTopicStats.class, PartitionedTopicStatsImpl.class);
        resolver.addMapping(NonPersistentPartitionedTopicStats.class, NonPersistentPartitionedTopicStatsImpl.class);
        resolver.addMapping(PublisherStats.class, PublisherStatsImpl.class);
        resolver.addMapping(ReplicatorStats.class, ReplicatorStatsImpl.class);
        resolver.addMapping(SubscriptionStats.class, SubscriptionStatsImpl.class);
        resolver.addMapping(AutoSubscriptionCreationOverride.class, AutoSubscriptionCreationOverrideImpl.class);

        // we use MixIn class to add jackson annotations
        mapper.addMixIn(ResourceQuota.class, ResourceQuotaMixIn.class);
        mapper.addMixIn(FunctionConfig.class, JsonIgnorePropertiesMixIn.class);
        mapper.addMixIn(FunctionState.class, JsonIgnorePropertiesMixIn.class);
        mapper.addMixIn(Metrics.class, MetricsMixIn.class);

        try {
            // We look for LoadManagerReport first, then add deserializer to the module
            // With shaded client, org.apache.pulsar.policies is relocated to
            // org.apache.pulsar.shade.org.apache.pulsar.policies
            ClassUtils.getClass("org.apache.pulsar.policies.data.loadbalancer.LoadManagerReport");
            module.addDeserializer(LoadManagerReport.class, new LoadReportDeserializer());
        } catch (ClassNotFoundException e) {
            log.debug("Add LoadManagerReport deserializer failed because LoadManagerReport.class has been shaded", e);
        }

        module.setAbstractTypes(resolver);

        mapper.registerModule(module);
    }

    /**
     * Clears the caches tied to the ObjectMapper instances.
     * This is used in tests to ensure that classloaders and class references don't leak between tests.
     * Jackson's ObjectMapper doesn't expose a public API for clearing all caches so this solution is partial.
     */
    public static void clearCaches() {
        clearCachesForObjectMapper(getMapper().getObjectMapper());
        clearCachesForObjectMapper(getYamlMapper().getObjectMapper());
    }

    private static void clearCachesForObjectMapper(ObjectMapper objectMapper) {
        objectMapper.getTypeFactory().clearCache();
    }

    /**
     * Replaces the existing singleton ObjectMapper instances with new instances.
     * This is used in tests to ensure that classloaders and class references don't leak between tests.
     */
    public static void refresh() {
        MAPPER_REFERENCE.set(new MapperReference(createObjectMapperInstance()));
        INSTANCE_WITH_INCLUDE_ALWAYS.set(new MapperReference(createObjectMapperWithIncludeAlways()));
        YAML_MAPPER_REFERENCE.set(new MapperReference(createYamlInstance()));
    }
}