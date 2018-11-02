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
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.functions.Resources;
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.functions.api.utils.IdentityFunction;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.utils.io.ConnectorUtils;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Path;
import java.util.Map;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.pulsar.common.naming.TopicName.DEFAULT_NAMESPACE;
import static org.apache.pulsar.common.naming.TopicName.PUBLIC_TENANT;
import static org.apache.pulsar.functions.utils.Utils.convertProcessingGuarantee;
import static org.apache.pulsar.functions.utils.Utils.getSourceType;

public class SourceConfigUtils {

    public static FunctionDetails convert(SourceConfig sourceConfig, NarClassLoader classLoader)
            throws IllegalArgumentException, IOException {

        String sourceClassName = null;
        String typeArg = null;

        FunctionDetails.Builder functionDetailsBuilder = FunctionDetails.newBuilder();

        boolean isBuiltin = !StringUtils.isEmpty(sourceConfig.getArchive()) && sourceConfig.getArchive().startsWith(Utils.BUILTIN);

        if (!isBuiltin) {
            if (!StringUtils.isEmpty(sourceConfig.getArchive()) && sourceConfig.getArchive().startsWith(Utils.FILE)) {
                if (org.apache.commons.lang3.StringUtils.isBlank(sourceConfig.getClassName())) {
                    throw new IllegalArgumentException("Class-name must be present for archive with file-url");
                }
                sourceClassName = sourceConfig.getClassName(); // server derives the arg-type by loading a class
            } else {
                sourceClassName = ConnectorUtils.getIOSourceClass(classLoader);
                typeArg = getSourceType(sourceClassName, classLoader).getName();
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

        if (sourceConfig.getSecrets() != null && !sourceConfig.getSecrets().isEmpty()) {
            functionDetailsBuilder.setSecretsMap(new Gson().toJson(sourceConfig.getSecrets()));
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

    public static SourceConfig convertFromDetails(FunctionDetails functionDetails) {
        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setTenant(functionDetails.getTenant());
        sourceConfig.setNamespace(functionDetails.getNamespace());
        sourceConfig.setName(functionDetails.getName());
        sourceConfig.setParallelism(functionDetails.getParallelism());
        sourceConfig.setProcessingGuarantees(Utils.convertProcessingGuarantee(functionDetails.getProcessingGuarantees()));
        Function.SourceSpec sourceSpec = functionDetails.getSource();
        if (!StringUtils.isEmpty(sourceSpec.getClassName())) {
            sourceConfig.setClassName(sourceSpec.getClassName());
        }
        if (!StringUtils.isEmpty(sourceSpec.getBuiltin())) {
            sourceConfig.setArchive("builtin://" + sourceSpec.getBuiltin());
        }
        if (!StringUtils.isEmpty(sourceSpec.getConfigs())) {
            Type type = new TypeToken<Map<String, String>>() {}.getType();
            sourceConfig.setConfigs(new Gson().fromJson(sourceSpec.getConfigs(), type));
        }
        if (!isEmpty(functionDetails.getSecretsMap())) {
            Type type = new TypeToken<Map<String, Object>>() {}.getType();
            Map<String, Object> secretsMap = new Gson().fromJson(functionDetails.getSecretsMap(), type);
            sourceConfig.setSecrets(secretsMap);
        }
        Function.SinkSpec sinkSpec = functionDetails.getSink();
        sourceConfig.setTopicName(sinkSpec.getTopic());
        if (!StringUtils.isEmpty(sinkSpec.getSchemaType())) {
            sourceConfig.setSchemaType(sinkSpec.getSchemaType());
        }
        if (!StringUtils.isEmpty(sinkSpec.getSerDeClassName())) {
            sourceConfig.setSerdeClassName(sinkSpec.getSerDeClassName());
        }
        if (functionDetails.hasResources()) {
            Resources resources = new Resources();
            resources.setCpu(functionDetails.getResources().getCpu());
            resources.setRam(functionDetails.getResources().getRam());
            resources.setDisk(functionDetails.getResources().getDisk());
            sourceConfig.setResources(resources);
        }
        return sourceConfig;
    }

    public static NarClassLoader validate(SourceConfig sourceConfig, Path archivePath, String functionPkgUrl, File uploadedInputStreamAsFile) {
        if (isEmpty(sourceConfig.getTenant())) {
            throw new IllegalArgumentException("Source tenant cannot be null");
        }
        if (isEmpty(sourceConfig.getNamespace())) {
            throw new IllegalArgumentException("Source namespace cannot be null");
        }
        if (isEmpty(sourceConfig.getName())) {
            throw new IllegalArgumentException("Source name cannot be null");
        }
        if (isEmpty(sourceConfig.getTopicName())) {
            throw new IllegalArgumentException("Topic name cannot be null");
        }
        if (!TopicName.isValid(sourceConfig.getTopicName())) {
            throw new IllegalArgumentException("Topic name is invalid");
        }
        if (sourceConfig.getParallelism() <= 0) {
            throw new IllegalArgumentException("Source parallelism should positive number");
        }
        if (sourceConfig.getResources() != null) {
            ResourceConfigUtils.validate(sourceConfig.getResources());
        }

        NarClassLoader classLoader = Utils.extractNarClassLoader(archivePath, functionPkgUrl, uploadedInputStreamAsFile);
        if (classLoader == null) {
            throw new IllegalArgumentException("Source Package is not provided");
        }

        String sourceClassName;
        try {
            sourceClassName = ConnectorUtils.getIOSourceClass(classLoader);
        } catch (IOException e1) {
            throw new IllegalArgumentException("Failed to extract source class from archive", e1);
        }

        Class<?> typeArg = getSourceType(sourceClassName, classLoader);

        // Only one of serdeClassName or schemaType should be set
        if (!StringUtils.isEmpty(sourceConfig.getSerdeClassName()) && !StringUtils.isEmpty(sourceConfig.getSchemaType())) {
            throw new IllegalArgumentException("Only one of serdeClassName or schemaType should be set");
        }

        if (!StringUtils.isEmpty(sourceConfig.getSerdeClassName())) {
            ValidatorUtils.validateSerde(sourceConfig.getSerdeClassName(), typeArg, classLoader, false);
        }
        if (!StringUtils.isEmpty(sourceConfig.getSchemaType())) {
            ValidatorUtils.validateSchema(sourceConfig.getSchemaType(), typeArg, classLoader, false);
        }
        return classLoader;
    }

    public static void inferMissingArguments(SourceConfig sourceConfig) {
        if (sourceConfig.getTenant() == null) {
            sourceConfig.setTenant(PUBLIC_TENANT);
        }
        if (sourceConfig.getNamespace() == null) {
            sourceConfig.setNamespace(DEFAULT_NAMESPACE);
        }
    }
}
