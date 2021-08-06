/**
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
import com.fasterxml.jackson.databind.module.SimpleAbstractTypeResolver;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.netty.util.concurrent.FastThreadLocal;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ClassUtils;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.FunctionState;
import org.apache.pulsar.common.functions.JsonIgnorePropertiesMixIn;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyDataImpl;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyData;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BacklogQuotaMixIn;
import org.apache.pulsar.common.policies.data.BrokerNamespaceIsolationDataImpl;
import org.apache.pulsar.common.policies.data.BrokerNamespaceIsolationData;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.FailureDomainImpl;
import org.apache.pulsar.common.policies.data.FailureDomain;
import org.apache.pulsar.common.policies.data.FunctionInstanceStatsImpl;
import org.apache.pulsar.common.policies.data.FunctionInstanceStatsDataImpl;
import org.apache.pulsar.common.policies.data.FunctionInstanceStatsDataBaseImpl;
import org.apache.pulsar.common.policies.data.FunctionInstanceStatsDataBase;
import org.apache.pulsar.common.policies.data.FunctionInstanceStatsData;
import org.apache.pulsar.common.policies.data.FunctionInstanceStats;
import org.apache.pulsar.common.policies.data.FunctionStatsImpl;
import org.apache.pulsar.common.policies.data.FunctionStats;
import org.apache.pulsar.common.policies.data.NamespaceIsolationDataImpl;
import org.apache.pulsar.common.policies.data.NamespaceIsolationData;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.apache.pulsar.common.policies.data.ResourceQuota;
import org.apache.pulsar.common.policies.data.ResourceQuotaMixIn;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.common.stats.MetricsMixIn;
import org.apache.pulsar.policies.data.loadbalancer.LoadManagerReport;
import org.apache.pulsar.policies.data.loadbalancer.LoadReportDeserializer;

@SuppressWarnings("checkstyle:JavadocType")
@Slf4j
public class ObjectMapperFactory {
    public static ObjectMapper create() {
        ObjectMapper mapper = new ObjectMapper();
        // forward compatibility for the properties may go away in the future
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
        mapper.setSerializationInclusion(Include.NON_NULL);
        setAnnotationsModule(mapper);
        return mapper;
    }

    public static ObjectMapper createYaml() {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        // forward compatibility for the properties may go away in the future
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
        mapper.setSerializationInclusion(Include.NON_NULL);
        setAnnotationsModule(mapper);
        return mapper;
    }

    private static final FastThreadLocal<ObjectMapper> JSON_MAPPER = new FastThreadLocal<ObjectMapper>() {
        @Override
        protected ObjectMapper initialValue() throws Exception {
            return create();
        }
    };

    private static final FastThreadLocal<ObjectMapper> YAML_MAPPER = new FastThreadLocal<ObjectMapper>() {
        @Override
        protected ObjectMapper initialValue() throws Exception {
            return createYaml();
        }
    };

    public static ObjectMapper getThreadLocal() {
        return JSON_MAPPER.get();
    }

    public static ObjectMapper getThreadLocalYaml() {
        return YAML_MAPPER.get();
    }

    private static void setAnnotationsModule(ObjectMapper mapper) {
        SimpleModule module = new SimpleModule("AnnotationsModule");

        // we use customized deserializer to replace jackson annotations in some POJOs
        SimpleAbstractTypeResolver resolver = new SimpleAbstractTypeResolver();
        resolver.addMapping(AutoFailoverPolicyData.class, AutoFailoverPolicyDataImpl.class);
        resolver.addMapping(BrokerNamespaceIsolationData.class, BrokerNamespaceIsolationDataImpl.class);
        resolver.addMapping(ClusterData.class, ClusterDataImpl.class);
        resolver.addMapping(FailureDomain.class, FailureDomainImpl.class);
        resolver.addMapping(NamespaceIsolationData.class, NamespaceIsolationDataImpl.class);
        resolver.addMapping(TenantInfo.class, TenantInfoImpl.class);
        resolver.addMapping(OffloadPolicies.class, OffloadPoliciesImpl.class);
        resolver.addMapping(FunctionStats.class, FunctionStatsImpl.class);
        resolver.addMapping(FunctionInstanceStats.class, FunctionInstanceStatsImpl.class);
        resolver.addMapping(FunctionInstanceStatsData.class, FunctionInstanceStatsDataImpl.class);
        resolver.addMapping(FunctionInstanceStatsDataBase.class, FunctionInstanceStatsDataBaseImpl.class);

        // we use MixIn class to add jackson annotations
        mapper.addMixIn(BacklogQuota.class, BacklogQuotaMixIn.class);
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


}
