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
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.functions.api.utils.IdentityFunction;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.utils.io.ConnectorUtils;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

import static org.apache.pulsar.functions.utils.Utils.convertProcessingGuarantee;
import static org.apache.pulsar.functions.utils.Utils.getSourceType;

public class SourceConfigUtils {

    public static FunctionDetails convert(SourceConfig sourceConfig)
            throws IllegalArgumentException, IOException {

        String sourceClassName = null;
        String typeArg = null;

        FunctionDetails.Builder functionDetailsBuilder = FunctionDetails.newBuilder();

        boolean isBuiltin = sourceConfig.getArchive().startsWith(Utils.BUILTIN);

        if (!isBuiltin) {
            if (sourceConfig.getArchive().startsWith(Utils.FILE)) {
                if (org.apache.commons.lang3.StringUtils.isBlank(sourceConfig.getClassName())) {
                    throw new IllegalArgumentException("Class-name must be present for archive with file-url");
                }
                sourceClassName = sourceConfig.getClassName(); // server derives the arg-type by loading a class
            } else {
                sourceClassName = ConnectorUtils.getIOSourceClass(sourceConfig.getArchive());

                try (NarClassLoader ncl = NarClassLoader.getFromArchive(new File(sourceConfig.getArchive()),
                        Collections.emptySet())) {
                    typeArg = getSourceType(sourceClassName, ncl).getName();
                }
            }
        }

        if (sourceConfig.getTenant() != null) {
            functionDetailsBuilder.setTenant(sourceConfig.getTenant());
        }
        if (sourceConfig.getNamespace() != null) {
            functionDetailsBuilder.setNamespace(sourceConfig.getNamespace());
        }
        if (sourceConfig.getName() != null) {
            functionDetailsBuilder.setName(sourceConfig.getName());
        }
        functionDetailsBuilder.setRuntime(FunctionDetails.Runtime.JAVA);
        functionDetailsBuilder.setParallelism(sourceConfig.getParallelism());
        functionDetailsBuilder.setClassName(IdentityFunction.class.getName());
        functionDetailsBuilder.setAutoAck(true);
        if (sourceConfig.getProcessingGuarantees() != null) {
            functionDetailsBuilder.setProcessingGuarantees(
                    convertProcessingGuarantee(sourceConfig.getProcessingGuarantees()));
        }

        // set source spec
        Function.SourceSpec.Builder sourceSpecBuilder = Function.SourceSpec.newBuilder();
        if (sourceClassName != null) {
            sourceSpecBuilder.setClassName(sourceClassName);
        }

        if (isBuiltin) {
            String builtin = sourceConfig.getArchive().replaceFirst("^builtin://", "");
            sourceSpecBuilder.setBuiltin(builtin);
        }

        if (sourceConfig.getConfigs() != null) {
            sourceSpecBuilder.setConfigs(new Gson().toJson(sourceConfig.getConfigs()));
        }

        if (typeArg != null) {
            sourceSpecBuilder.setTypeClassName(typeArg);
        }
        functionDetailsBuilder.setSource(sourceSpecBuilder);

        // set up sink spec.
        // Sink spec classname should be empty so that the default pulsar sink will be used
        Function.SinkSpec.Builder sinkSpecBuilder = Function.SinkSpec.newBuilder();
        if (!org.apache.commons.lang3.StringUtils.isEmpty(sourceConfig.getSchemaType())) {
            sinkSpecBuilder.setSchemaType(sourceConfig.getSchemaType());
        }
        if (!org.apache.commons.lang3.StringUtils.isEmpty(sourceConfig.getSerdeClassName())) {
            sinkSpecBuilder.setSerDeClassName(sourceConfig.getSerdeClassName());
        }

        sinkSpecBuilder.setTopic(sourceConfig.getTopicName());

        if (typeArg != null) {
            sinkSpecBuilder.setTypeClassName(typeArg);
        }

        functionDetailsBuilder.setSink(sinkSpecBuilder);

        if (sourceConfig.getResources() != null) {
            Function.Resources.Builder bldr = Function.Resources.newBuilder();
            if (sourceConfig.getResources().getCpu() != null) {
                bldr.setCpu(sourceConfig.getResources().getCpu());
            }
            if (sourceConfig.getResources().getRam() != null) {
                bldr.setRam(sourceConfig.getResources().getRam());
            }
            if (sourceConfig.getResources().getDisk() != null) {
                bldr.setDisk(sourceConfig.getResources().getDisk());
            }
            functionDetailsBuilder.setResources(bldr.build());
        }

        return functionDetailsBuilder.build();
    }
}
