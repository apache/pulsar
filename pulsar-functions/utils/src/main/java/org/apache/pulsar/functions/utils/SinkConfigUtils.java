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

package org.apache.pulsar.functions.utils;

import com.google.gson.Gson;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.functions.api.utils.IdentityFunction;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.utils.io.ConnectorUtils;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.pulsar.functions.utils.Utils.convertProcessingGuarantee;

public class SinkConfigUtils {

    public static FunctionDetails convert(SinkConfig sinkConfig) throws IOException {

        String sinkClassName = null;
        String typeArg = null;

        FunctionDetails.Builder functionDetailsBuilder = FunctionDetails.newBuilder();

        boolean isBuiltin = sinkConfig.getArchive().startsWith(Utils.BUILTIN);

        if (!isBuiltin) {
            if (sinkConfig.getArchive().startsWith(Utils.FILE)) {
                if (isBlank(sinkConfig.getClassName())) {
                    throw new IllegalArgumentException("Class-name must be present for archive with file-url");
                }
                sinkClassName = sinkConfig.getClassName(); // server derives the arg-type by loading a class
            } else {
                sinkClassName = ConnectorUtils.getIOSinkClass(sinkConfig.getArchive());
                try (NarClassLoader ncl = NarClassLoader.getFromArchive(new File(sinkConfig.getArchive()),
                        Collections.emptySet())) {
                    typeArg = Utils.getSinkType(sinkClassName, ncl).getName();
                }
            }
        }

        if (sinkConfig.getTenant() != null) {
            functionDetailsBuilder.setTenant(sinkConfig.getTenant());
        }
        if (sinkConfig.getNamespace() != null) {
            functionDetailsBuilder.setNamespace(sinkConfig.getNamespace());
        }
        if (sinkConfig.getName() != null) {
            functionDetailsBuilder.setName(sinkConfig.getName());
        }
        functionDetailsBuilder.setRuntime(FunctionDetails.Runtime.JAVA);
        functionDetailsBuilder.setParallelism(sinkConfig.getParallelism());
        functionDetailsBuilder.setClassName(IdentityFunction.class.getName());
        if (sinkConfig.getProcessingGuarantees() != null) {
            functionDetailsBuilder.setProcessingGuarantees(
                    convertProcessingGuarantee(sinkConfig.getProcessingGuarantees()));
        }

        // set source spec
        // source spec classname should be empty so that the default pulsar source will be used
        Function.SourceSpec.Builder sourceSpecBuilder = Function.SourceSpec.newBuilder();
        sourceSpecBuilder.setSubscriptionType(Function.SubscriptionType.SHARED);
        if (sinkConfig.getInputs() != null) {
            sinkConfig.getInputs().forEach(topicName ->
                    sourceSpecBuilder.putInputSpecs(topicName,
                            Function.ConsumerSpec.newBuilder()
                                    .setIsRegexPattern(false)
                                    .build()));
        }
        if (!StringUtils.isEmpty(sinkConfig.getTopicsPattern())) {
            sourceSpecBuilder.putInputSpecs(sinkConfig.getTopicsPattern(),
                    Function.ConsumerSpec.newBuilder()
                            .setIsRegexPattern(true)
                            .build());
        }
        if (sinkConfig.getTopicToSerdeClassName() != null) {
            sinkConfig.getTopicToSerdeClassName().forEach((topicName, serde) -> {
                sourceSpecBuilder.putInputSpecs(topicName,
                        Function.ConsumerSpec.newBuilder()
                                .setSerdeClassName(serde == null ? "" : serde)
                                .setIsRegexPattern(false)
                                .build());
            });
        }
        if (sinkConfig.getTopicToSchemaType() != null) {
            sinkConfig.getTopicToSchemaType().forEach((topicName, schemaType) -> {
                sourceSpecBuilder.putInputSpecs(topicName,
                        Function.ConsumerSpec.newBuilder()
                                .setSchemaType(schemaType == null ? "" : schemaType)
                                .setIsRegexPattern(false)
                                .build());
            });
        }
        if (sinkConfig.getInputSpecs() != null) {
            sinkConfig.getInputSpecs().forEach((topic, spec) -> {
                sourceSpecBuilder.putInputSpecs(topic,
                        Function.ConsumerSpec.newBuilder()
                                .setSerdeClassName(spec.getSerdeClassName() != null ? spec.getSerdeClassName() : "")
                                .setSchemaType(spec.getSchemaType() != null ? spec.getSchemaType() : "")
                                .setIsRegexPattern(spec.isRegexPattern())
                                .build());
            });
        }

        if (typeArg != null) {
            sourceSpecBuilder.setTypeClassName(typeArg);
        }
        if (isNotBlank(sinkConfig.getSourceSubscriptionName())) {
            sourceSpecBuilder.setSubscriptionName(sinkConfig.getSourceSubscriptionName());
        }

        Function.SubscriptionType subType = (sinkConfig.isRetainOrdering()
                || FunctionConfig.ProcessingGuarantees.EFFECTIVELY_ONCE.equals(sinkConfig.getProcessingGuarantees()))
                ? Function.SubscriptionType.FAILOVER
                : Function.SubscriptionType.SHARED;
        sourceSpecBuilder.setSubscriptionType(subType);

        functionDetailsBuilder.setAutoAck(sinkConfig.isAutoAck());
        if (sinkConfig.getTimeoutMs() != null) {
            sourceSpecBuilder.setTimeoutMs(sinkConfig.getTimeoutMs());
        }

        functionDetailsBuilder.setSource(sourceSpecBuilder);

        // set up sink spec
        Function.SinkSpec.Builder sinkSpecBuilder = Function.SinkSpec.newBuilder();
        if (sinkClassName != null) {
            sinkSpecBuilder.setClassName(sinkClassName);
        }

        if (isBuiltin) {
            String builtin = sinkConfig.getArchive().replaceFirst("^builtin://", "");
            sinkSpecBuilder.setBuiltin(builtin);
        }

        if (sinkConfig.getConfigs() != null) {
            sinkSpecBuilder.setConfigs(new Gson().toJson(sinkConfig.getConfigs()));
        }
        if (typeArg != null) {
            sinkSpecBuilder.setTypeClassName(typeArg);
        }
        functionDetailsBuilder.setSink(sinkSpecBuilder);

        if (sinkConfig.getResources() != null) {
            Function.Resources.Builder bldr = Function.Resources.newBuilder();
            if (sinkConfig.getResources().getCpu() != null) {
                bldr.setCpu(sinkConfig.getResources().getCpu());
            }
            if (sinkConfig.getResources().getRam() != null) {
                bldr.setRam(sinkConfig.getResources().getRam());
            }
            if (sinkConfig.getResources().getDisk() != null) {
                bldr.setDisk(sinkConfig.getResources().getDisk());
            }
            functionDetailsBuilder.setResources(bldr.build());
        }
        return functionDetailsBuilder.build();
    }
}