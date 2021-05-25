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
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.netty.util.concurrent.FastThreadLocal;
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
import org.apache.pulsar.policies.data.loadbalancer.LoadManagerReport;
import org.apache.pulsar.policies.data.loadbalancer.LoadReportDeserializer;

@SuppressWarnings("checkstyle:JavadocType")
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

        mapper.registerModule(module);
    }


}
