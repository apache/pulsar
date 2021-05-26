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
package org.apache.pulsar.client.admin.internal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleAbstractTypeResolver;
import com.fasterxml.jackson.databind.module.SimpleModule;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;
import org.apache.pulsar.client.admin.OffloadProcessStatus;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.FunctionState;
import org.apache.pulsar.common.functions.JsonIgnorePropertiesMixIn;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BacklogQuotaMixIn;
import org.apache.pulsar.common.policies.data.FunctionStats;
import org.apache.pulsar.common.policies.data.FunctionStatsMixIn;
import org.apache.pulsar.common.policies.data.ResourceQuota;
import org.apache.pulsar.common.policies.data.ResourceQuotaMixIn;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.common.stats.MetricsMixIn;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.policies.data.loadbalancer.LoadManagerReport;
import org.apache.pulsar.policies.data.loadbalancer.LoadReportDeserializer;

/**
 * Provides custom configuration for jackson.
 */
@Provider
public class JacksonConfigurator implements ContextResolver<ObjectMapper> {

    private final ObjectMapper mapper;

    public JacksonConfigurator() {
        mapper = ObjectMapperFactory.create();
        setInterfaceDefaultMapperModule();
    }

    private void setInterfaceDefaultMapperModule() {
        SimpleModule module = new SimpleModule("InterfaceDefaultMapperModule");

        // we not specific @JsonDeserialize annotation in OffloadProcessStatus
        // because we do not want to have jackson dependency in pulsar-client-admin-api
        // In this case we use SimpleAbstractTypeResolver to map interfaces to impls
        SimpleAbstractTypeResolver resolver = new SimpleAbstractTypeResolver();
        resolver.addMapping(OffloadProcessStatus.class, OffloadProcessStatusImpl.class);

        // we use customized deserializer to replace jackson annotations in some POJOs
        module.addDeserializer(LoadManagerReport.class, new LoadReportDeserializer());

        // we use MixIn class to add jackson annotations
        mapper.addMixIn(BacklogQuota.class, BacklogQuotaMixIn.class);
        mapper.addMixIn(ResourceQuota.class, ResourceQuotaMixIn.class);
        mapper.addMixIn(FunctionConfig.class, JsonIgnorePropertiesMixIn.class);
        mapper.addMixIn(FunctionState.class, JsonIgnorePropertiesMixIn.class);
        mapper.addMixIn(FunctionStats.class, FunctionStatsMixIn.class);
        mapper.addMixIn(FunctionStats.FunctionInstanceStats.class,
                FunctionStatsMixIn.FunctionInstanceStatsMixIn.class);
        mapper.addMixIn(FunctionStats.FunctionInstanceStats.FunctionInstanceStatsData.class,
                FunctionStatsMixIn.FunctionInstanceStatsMixIn.FunctionInstanceStatsDataMixIn.class);
        mapper.addMixIn(FunctionStats.FunctionInstanceStats.FunctionInstanceStatsDataBase.class,
                FunctionStatsMixIn.FunctionInstanceStatsMixIn.FunctionInstanceStatsDataBaseMixIn.class);
        mapper.addMixIn(Metrics.class, MetricsMixIn.class);

        module.setAbstractTypes(resolver);
        mapper.registerModule(module);
    }

    @Override
    public ObjectMapper getContext(Class<?> type) {
        return mapper;
    }

}
