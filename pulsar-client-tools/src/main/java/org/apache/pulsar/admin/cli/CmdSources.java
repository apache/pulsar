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
package org.apache.pulsar.admin.cli;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.StringConverter;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import lombok.Getter;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.internal.FunctionsImpl;
import org.apache.pulsar.functions.api.utils.IdentityFunction;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.Function.Resources;
import org.apache.pulsar.functions.proto.Function.SinkSpec;
import org.apache.pulsar.functions.proto.Function.SourceSpec;
import org.apache.pulsar.functions.utils.FunctionConfig;
import org.apache.pulsar.functions.utils.Reflections;
import org.apache.pulsar.functions.utils.SourceConfig;
import org.apache.pulsar.functions.utils.Utils;
import org.apache.pulsar.functions.utils.validation.ConfigValidation;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.MalformedURLException;
import java.util.Map;

import static org.apache.pulsar.common.naming.TopicName.DEFAULT_NAMESPACE;
import static org.apache.pulsar.common.naming.TopicName.PUBLIC_TENANT;
import static org.apache.pulsar.functions.utils.Utils.convertProcessingGuarantee;
import static org.apache.pulsar.functions.utils.Utils.getSourceType;
import static org.apache.pulsar.functions.utils.Utils.loadConfig;

@Getter
@Parameters(commandDescription = "Interface for managing Pulsar Source (Ingress data to Pulsar)")
public class CmdSources extends CmdBase {

    private final CreateSource createSource;
    private final DeleteSource deleteSource;
    private final UpdateSource updateSource;
    private final LocalSourceRunner localSourceRunner;

    public CmdSources(PulsarAdmin admin) {
        super("source", admin);
        createSource = new CreateSource();
        updateSource = new UpdateSource();
        deleteSource = new DeleteSource();
        localSourceRunner = new LocalSourceRunner();

        jcommander.addCommand("create", createSource);
        jcommander.addCommand("update", updateSource);
        jcommander.addCommand("delete", deleteSource);
        jcommander.addCommand("localrun", localSourceRunner);
    }

    /**
     * Base command
     */
    @Getter
    abstract class BaseCommand extends CliCommand {
        @Override
        void run() throws Exception {
            processArguments();
            runCmd();
        }

        void processArguments() throws Exception {
        }

        abstract void runCmd() throws Exception;
    }

    @Parameters(commandDescription = "Run the Pulsar source locally (rather than deploying it to the Pulsar cluster)")
    class LocalSourceRunner extends CreateSource {

        @Parameter(names = "--brokerServiceUrl", description = "The URL for the Pulsar broker")
        protected String brokerServiceUrl;

        @Override
        void runCmd() throws Exception {
            CmdFunctions.startLocalRun(createSourceConfigProto2(sourceConfig),
                    sourceConfig.getParallelism(), brokerServiceUrl, jarFile, admin);
        }
    }

    @Parameters(commandDescription = "Create Pulsar source connectors")
    public class CreateSource extends SourceCommand {
        @Override
        void runCmd() throws Exception {
            admin.functions().createFunction(createSourceConfig(sourceConfig), jarFile);
            print("Created successfully");
        }
    }

    @Parameters(commandDescription = "Update Pulsar source connectors")
    public class UpdateSource extends SourceCommand {
        @Override
        void runCmd() throws Exception {
            admin.functions().updateFunction(createSourceConfig(sourceConfig), jarFile);
            print("Updated successfully");
        }
    }

    abstract class SourceCommand extends BaseCommand {
        @Parameter(names = "--tenant", description = "The source's tenant")
        protected String tenant;
        @Parameter(names = "--namespace", description = "The source's namespace")
        protected String namespace;
        @Parameter(names = "--name", description = "The source's name")
        protected String name;
        @Parameter(names = "--processingGuarantees", description = "The processing guarantees (aka delivery semantics) applied to the Source")
        protected FunctionConfig.ProcessingGuarantees processingGuarantees;
        @Parameter(names = "--className", description = "The source's class name")
        protected String className;
        @Parameter(names = "--destinationTopicName", description = "Pulsar topic to ingress data to")
        protected String destinationTopicName;
        @Parameter(names = "--deserializationClassName", description = "The classname for SerDe class for the source")
        protected String deserializationClassName;
        @Parameter(names = "--parallelism", description = "The source's parallelism factor (i.e. the number of source instances to run)")
        protected Integer parallelism;
        @Parameter(
                names = "--jar",
                description = "Path to the jar file for the Source",
                listConverter = StringConverter.class)
        protected String jarFile;

        @Parameter(names = "--sourceConfigFile", description = "The path to a YAML config file specifying the "
                + "source's configuration")
        protected String sourceConfigFile;
        @Parameter(names = "--cpu", description = "The cpu in cores that need to be allocated per function instance(applicable only to docker runtime)")
        protected Double cpu;
        @Parameter(names = "--ram", description = "The ram in bytes that need to be allocated per function instance(applicable only to process/docker runtime)")
        protected Long ram;
        @Parameter(names = "--disk", description = "The disk in bytes that need to be allocated per function instance(applicable only to docker runtime)")
        protected Long disk;
        @Parameter(names = "--sourceConfig", description = "Source config key/values")
        protected String sourceConfigString;

        protected SourceConfig sourceConfig;

        @Override
        void processArguments() throws Exception {
            super.processArguments();

            if (null != sourceConfigFile) {
                this.sourceConfig = loadConfig(sourceConfigFile, SourceConfig.class);
            } else {
                this.sourceConfig = new SourceConfig();
            }
            if (null != tenant) {
                sourceConfig.setTenant(tenant);
            }
            if (null != namespace) {
                sourceConfig.setNamespace(namespace);
            }
            if (null != name) {
                sourceConfig.setName(name);
            }
            if (null != className) {
                this.sourceConfig.setClassName(className);
            }
            if (null != destinationTopicName) {
                sourceConfig.setTopicName(destinationTopicName);
            }
            if (null != deserializationClassName) {
                sourceConfig.setSerdeClassName(deserializationClassName);
            }
            if (null != processingGuarantees) {
                sourceConfig.setProcessingGuarantees(processingGuarantees);
            }
            if (parallelism != null) {
                sourceConfig.setParallelism(parallelism);
            }

            if (jarFile != null) {
                sourceConfig.setJar(jarFile);
            }

            sourceConfig.setResources(new org.apache.pulsar.functions.utils.Resources(cpu, ram, disk));

            if (null != sourceConfigString) {
                Type type = new TypeToken<Map<String, String>>(){}.getType();
                Map<String, Object> sourceConfigMap = new Gson().fromJson(sourceConfigString, type);
                sourceConfig.setConfigs(sourceConfigMap);
            }

            inferMissingArguments(sourceConfig);
        }

        private void inferMissingArguments(SourceConfig sourceConfig) {
            if (sourceConfig.getTenant() == null) {
                sourceConfig.setTenant(PUBLIC_TENANT);
            }
            if (sourceConfig.getNamespace() == null) {
                sourceConfig.setNamespace(DEFAULT_NAMESPACE);
            }
        }

        protected void validateSourceConfigs(SourceConfig sourceConfig) {
            if (null == sourceConfig.getJar()) {
                throw new ParameterException("Source jar not specfied");
            }

            if (!new File(sourceConfig.getJar()).exists()) {
                throw new ParameterException("Jar file " + sourceConfig.getJar() + " does not exist");
            }

            File file = new File(jarFile);
            ClassLoader userJarLoader;
            try {
                userJarLoader = Reflections.loadJar(file);
            } catch (MalformedURLException e) {
                throw new ParameterException("Failed to load user jar " + file + " with error " + e.getMessage());
            }
            // make sure the function class loader is accessible thread-locally
            Thread.currentThread().setContextClassLoader(userJarLoader);

            try {
                // Need to load jar and set context class loader before calling
                ConfigValidation.validateConfig(sourceConfig, FunctionConfig.Runtime.JAVA.name());
            } catch (Exception e) {
                throw new ParameterException(e.getMessage());
            }
        }

        protected org.apache.pulsar.functions.proto.Function.FunctionDetails createSourceConfigProto2(SourceConfig sourceConfig)
                throws IOException {
            org.apache.pulsar.functions.proto.Function.FunctionDetails.Builder functionDetailsBuilder
                    = org.apache.pulsar.functions.proto.Function.FunctionDetails.newBuilder();
            Utils.mergeJson(FunctionsImpl.printJson(createSourceConfig(sourceConfig)), functionDetailsBuilder);
            return functionDetailsBuilder.build();
        }

        protected FunctionDetails createSourceConfig(SourceConfig sourceConfig) {

            // check if source configs are valid
            validateSourceConfigs(sourceConfig);

            Class<?> typeArg = getSourceType(sourceConfig.getClassName());

            FunctionDetails.Builder functionDetailsBuilder = FunctionDetails.newBuilder();
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
            SourceSpec.Builder sourceSpecBuilder = SourceSpec.newBuilder();
            sourceSpecBuilder.setClassName(sourceConfig.getClassName());
            sourceSpecBuilder.setConfigs(new Gson().toJson(sourceConfig.getConfigs()));
            sourceSpecBuilder.setTypeClassName(typeArg.getName());
            functionDetailsBuilder.setSource(sourceSpecBuilder);

            // set up sink spec.
            // Sink spec classname should be empty so that the default pulsar sink will be used
            SinkSpec.Builder sinkSpecBuilder = SinkSpec.newBuilder();
            if (sourceConfig.getSerdeClassName() != null && !sourceConfig.getSerdeClassName().isEmpty()) {
                sinkSpecBuilder.setSerDeClassName(sourceConfig.getSerdeClassName());
            }
            sinkSpecBuilder.setTopic(sourceConfig.getTopicName());
            sinkSpecBuilder.setTypeClassName(typeArg.getName());

            functionDetailsBuilder.setSink(sinkSpecBuilder);

            if (sourceConfig.getResources() != null) {
                Resources.Builder bldr = Resources.newBuilder();
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

    @Parameters(commandDescription = "Stops a Pulsar source")
    class DeleteSource extends BaseCommand {

        @Parameter(names = "--tenant", description = "The tenant of a sink or source")
        protected String tenant;

        @Parameter(names = "--namespace", description = "The namespace of a sink or source")
        protected String namespace;

        @Parameter(names = "--name", description = "The name of a sink or source")
        protected String name;

        @Override
        void processArguments() throws Exception {
            super.processArguments();
            if (null == name) {
                throw new RuntimeException(
                        "You must specify a name for the source");
            }
            if (tenant == null) {
                tenant = PUBLIC_TENANT;
            }
            if (namespace == null) {
                namespace = DEFAULT_NAMESPACE;
            }
        }

        @Override
        void runCmd() throws Exception {
            admin.functions().deleteFunction(tenant, namespace, name);
            print("Delete source successfully");
        }
    }
}
