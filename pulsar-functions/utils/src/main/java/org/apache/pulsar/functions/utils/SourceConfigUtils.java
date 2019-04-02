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
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
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
import static org.apache.pulsar.functions.utils.FunctionCommon.convertProcessingGuarantee;
import static org.apache.pulsar.functions.utils.FunctionCommon.getSourceType;

public class SourceConfigUtils {

    @Getter
    @Setter
    @AllArgsConstructor
    public static class ExtractedSourceDetails {
        private String sourceClassName;
        private String typeArg;
    }

    public static FunctionDetails convert(SourceConfig sourceConfig, ExtractedSourceDetails sourceDetails)
            throws IllegalArgumentException {
        FunctionDetails.Builder functionDetailsBuilder = FunctionDetails.newBuilder();

        boolean isBuiltin = !StringUtils.isEmpty(sourceConfig.getArchive()) && sourceConfig.getArchive().startsWith(org.apache.pulsar.common.functions.Utils.BUILTIN);

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
        if (sourceConfig.getParallelism() != null) {
            functionDetailsBuilder.setParallelism(sourceConfig.getParallelism());
        } else {
            functionDetailsBuilder.setParallelism(1);
        }
        functionDetailsBuilder.setClassName(IdentityFunction.class.getName());
        functionDetailsBuilder.setAutoAck(true);
        if (sourceConfig.getProcessingGuarantees() != null) {
            functionDetailsBuilder.setProcessingGuarantees(
                    convertProcessingGuarantee(sourceConfig.getProcessingGuarantees()));
        }

        // set source spec
        Function.SourceSpec.Builder sourceSpecBuilder = Function.SourceSpec.newBuilder();
        if (sourceDetails.getSourceClassName() != null) {
            sourceSpecBuilder.setClassName(sourceDetails.getSourceClassName());
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

        if (sourceDetails.getTypeArg() != null) {
            sourceSpecBuilder.setTypeClassName(sourceDetails.getTypeArg());
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

        if (sourceDetails.getTypeArg() != null) {
            sinkSpecBuilder.setTypeClassName(sourceDetails.getTypeArg());
        }

        functionDetailsBuilder.setSink(sinkSpecBuilder);

        // use default resources if resources not set
        Resources resources = Resources.mergeWithDefault(sourceConfig.getResources());

        Function.Resources.Builder bldr = Function.Resources.newBuilder();
        bldr.setCpu(resources.getCpu());
        bldr.setRam(resources.getRam());
        bldr.setDisk(resources.getDisk());
        functionDetailsBuilder.setResources(bldr);

        if (!org.apache.commons.lang3.StringUtils.isEmpty(sourceConfig.getRuntimeFlags())) {
            functionDetailsBuilder.setRuntimeFlags(sourceConfig.getRuntimeFlags());
        }

        return functionDetailsBuilder.build();
    }

    public static SourceConfig convertFromDetails(FunctionDetails functionDetails) {
        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setTenant(functionDetails.getTenant());
        sourceConfig.setNamespace(functionDetails.getNamespace());
        sourceConfig.setName(functionDetails.getName());
        sourceConfig.setParallelism(functionDetails.getParallelism());
        sourceConfig.setProcessingGuarantees(FunctionCommon.convertProcessingGuarantee(functionDetails.getProcessingGuarantees()));
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

        if (!org.apache.commons.lang3.StringUtils.isEmpty(functionDetails.getRuntimeFlags())) {
            sourceConfig .setRuntimeFlags(functionDetails.getRuntimeFlags());
        }
        return sourceConfig;
    }

    public static ExtractedSourceDetails validate(SourceConfig sourceConfig, Path archivePath, File sourcePackageFile) {
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
        if (sourceConfig.getParallelism() != null && sourceConfig.getParallelism() <= 0) {
            throw new IllegalArgumentException("Source parallelism should positive number");
        }
        if (sourceConfig.getResources() != null) {
            ResourceConfigUtils.validate(sourceConfig.getResources());
        }

        String sourceClassName;
        final Class<?> typeArg;
        final ClassLoader classLoader;
        if (!isEmpty(sourceConfig.getClassName())) {
            sourceClassName = sourceConfig.getClassName();
            // We really don't know if we should use nar class loader or regular classloader
            ClassLoader jarClassLoader = null;
            ClassLoader narClassLoader = null;
            try {
                jarClassLoader = FunctionCommon.extractClassLoader(archivePath, sourcePackageFile);
            } catch (Exception e) {
            }
            try {
                narClassLoader = FunctionCommon.extractNarClassLoader(archivePath, sourcePackageFile);
            } catch (Exception e) {
            }
            if (jarClassLoader == null && narClassLoader == null) {
                throw new IllegalArgumentException("Invalid Source Package");
            }
            // We use typeArg and classLoader as arguments for lambda functions that require them to be final
            // Thus we use these tmp vars
            Class<?> tmptypeArg;
            ClassLoader tmpclassLoader;
            try {
                tmptypeArg = getSourceType(sourceClassName, narClassLoader);
                tmpclassLoader = narClassLoader;
            } catch (Exception e) {
                tmptypeArg = getSourceType(sourceClassName, jarClassLoader);
                tmpclassLoader = jarClassLoader;
            }
            typeArg = tmptypeArg;
            classLoader = tmpclassLoader;
        } else if (!StringUtils.isEmpty(sourceConfig.getArchive()) && sourceConfig.getArchive().startsWith(org.apache.pulsar.common.functions.Utils.FILE)) {
            throw new IllegalArgumentException("Class-name must be present for archive with file-url");
        } else {
            classLoader = FunctionCommon.extractNarClassLoader(archivePath, sourcePackageFile);
            if (classLoader == null) {
                throw new IllegalArgumentException("Source Package is not provided");
            }
            try {
                sourceClassName = ConnectorUtils.getIOSourceClass((NarClassLoader) classLoader);
            } catch (IOException e1) {
                throw new IllegalArgumentException("Failed to extract source class from archive", e1);
            }
            typeArg = getSourceType(sourceClassName, classLoader);
        }

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

        return new ExtractedSourceDetails(sourceClassName, typeArg.getName());
    }

    public static SourceConfig validateUpdate(SourceConfig existingConfig, SourceConfig newConfig) {
        SourceConfig mergedConfig = existingConfig.toBuilder().build();
        if (!existingConfig.getTenant().equals(newConfig.getTenant())) {
            throw new IllegalArgumentException("Tenants differ");
        }
        if (!existingConfig.getNamespace().equals(newConfig.getNamespace())) {
            throw new IllegalArgumentException("Namespaces differ");
        }
        if (!existingConfig.getName().equals(newConfig.getName())) {
            throw new IllegalArgumentException("Function Names differ");
        }
        if (!StringUtils.isEmpty(newConfig.getClassName())) {
            mergedConfig.setClassName(newConfig.getClassName());
        }
        if (!StringUtils.isEmpty(newConfig.getTopicName()) && !newConfig.getTopicName().equals(existingConfig.getTopicName())) {
            throw new IllegalArgumentException("Destination topics differ");
        }
        if (!StringUtils.isEmpty(newConfig.getSerdeClassName())) {
            mergedConfig.setSerdeClassName(newConfig.getSerdeClassName());
        }
        if (!StringUtils.isEmpty(newConfig.getSchemaType())) {
            mergedConfig.setSchemaType(newConfig.getSchemaType());
        }
        if (newConfig.getConfigs() != null) {
            mergedConfig.setConfigs(newConfig.getConfigs());
        }
        if (newConfig.getSecrets() != null) {
            mergedConfig.setSecrets(newConfig.getSecrets());
        }
        if (newConfig.getProcessingGuarantees() != null && !newConfig.getProcessingGuarantees().equals(existingConfig.getProcessingGuarantees())) {
            throw new IllegalArgumentException("Processing Guarantess cannot be altered");
        }
        if (newConfig.getParallelism() != null) {
            mergedConfig.setParallelism(newConfig.getParallelism());
        }
        if (newConfig.getResources() != null) {
            mergedConfig.setResources(ResourceConfigUtils.merge(existingConfig.getResources(), newConfig.getResources()));
        }
        if (!StringUtils.isEmpty(newConfig.getArchive())) {
            mergedConfig.setArchive(newConfig.getArchive());
        }
        return mergedConfig;
    }

}
