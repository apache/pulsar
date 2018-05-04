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
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.StringConverter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.internal.FunctionsImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.connect.core.Sink;
import org.apache.pulsar.connect.core.Source;
import org.apache.pulsar.functions.api.utils.IdentityFunction;
import org.apache.pulsar.functions.shaded.proto.Function;
import org.apache.pulsar.functions.shaded.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.shaded.proto.Function.SinkSpec;
import org.apache.pulsar.functions.shaded.proto.Function.SourceSpec;
import org.apache.pulsar.functions.shaded.proto.Function.ProcessingGuarantees;
import org.apache.pulsar.functions.utils.FunctionConfig;
import org.apache.pulsar.functions.utils.SinkConfig;
import org.apache.pulsar.functions.utils.SourceConfig;
import org.apache.pulsar.functions.sink.PulsarSink;
import org.apache.pulsar.functions.source.PulsarSource;
import org.apache.pulsar.functions.utils.Reflections;
import org.apache.pulsar.functions.utils.Utils;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import net.jodah.typetools.TypeResolver;

@Slf4j
@Getter
@Parameters(commandDescription = "Interface for managing Pulsar Connectors (Ingress and egress data to and from Pulsar)")
public class CmdConnectors extends CmdBase {

    private final CreateSource createSource;
    private final CreateSink createSink;
    private final DeleteConnector deleteConnector;
    private final LocalSourceRunner localSourceRunner;
    private final LocalSinkRunner localSinkRunner;

    public CmdConnectors(PulsarAdmin admin) {
        super("connectors", admin);
        createSource = new CreateSource();
        createSink = new CreateSink();
        deleteConnector = new DeleteConnector();
        localSourceRunner = new LocalSourceRunner();
        localSinkRunner = new LocalSinkRunner();

        jcommander.addCommand("create-source", createSource);
        jcommander.addCommand("create-sink", createSink);
        jcommander.addCommand("delete", deleteConnector);
        jcommander.addCommand("localrun-source", localSourceRunner);
        jcommander.addCommand("localrun-sink", localSinkRunner);
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

    @Parameters(commandDescription = "Run the Pulsar Function locally (rather than deploying it to the Pulsar cluster)")

    class LocalSourceRunner extends CreateSource {

        @Parameter(names = "--brokerServiceUrl", description = "The URL for the Pulsar broker")
        protected String brokerServiceUrl;

        @Override
        void runCmd() throws Exception {
            CmdFunctions.startLocalRun(createSourceConfigProto2(sourceConfig),
                    sourceConfig.getParallelism(), brokerServiceUrl, jarFile, admin);
        }
    }

    class LocalSinkRunner extends CreateSink {

        @Parameter(names = "--brokerServiceUrl", description = "The URL for the Pulsar broker")
        protected String brokerServiceUrl;

        @Override
        void runCmd() throws Exception {
            CmdFunctions.startLocalRun(createSinkConfigProto2(sinkConfig),
                    sinkConfig.getParallelism(), brokerServiceUrl, jarFile, admin);
        }
    }

    @Parameters(commandDescription = "Create Pulsar source connectors")
    class CreateSource extends BaseCommand {
        @Parameter(names = "--fqfn", description = "The Fully Qualified Function Name (FQFN) for the function")
        protected String fqfn;
        @Parameter(names = "--tenant", description = "The function's tenant")
        protected String tenant;
        @Parameter(names = "--namespace", description = "The function's namespace")
        protected String namespace;
        @Parameter(names = "--name", description = "The function's name")
        protected String name;
        @Parameter(names = "--processingGuarantees", description = "The processing guarantees (aka delivery semantics) applied to the Source")
        protected FunctionConfig.ProcessingGuarantees processingGuarantees;
        @Parameter(names = "--className", description = "The function's class name")
        protected String className;
        @Parameter(names = "--topicName", description = "Pulsar topic to ingress data to")
        protected String topicName;
        @Parameter(names = "--serdeClassName", description = "")
        protected String serdeClassName;
        @Parameter(names = "--parallelism", description = "")
        protected String parallelism;
        @Parameter(
                names = "--jar",
                description = "Path to the jar file for the function (if the function is written in Java)",
                listConverter = StringConverter.class)
        protected String jarFile;

        @Parameter(names = "--functionConfigFile", description = "The path to a YAML config file specifying the "
                + "function's configuration")
        protected String fnConfigFile;

        protected SourceConfig sourceConfig;

        @Override
        void processArguments() throws Exception {
            super.processArguments();

            if (null != fnConfigFile) {
                this.sourceConfig = loadSourceConfig(fnConfigFile);
            } else {
                this.sourceConfig = new SourceConfig();
            }

            if (null != fqfn) {
                parseFullyQualifiedFunctionName(fqfn, sourceConfig);
            } else {
                if (null != tenant) {
                    sourceConfig.setTenant(tenant);
                }
                if (null != namespace) {
                    sourceConfig.setNamespace(namespace);
                }
                if (null != name) {
                    sourceConfig.setName(name);
                }
            }
            if (null != className) {
                this.sourceConfig.setClassName(className);
            }
            if (null != topicName) {
                sourceConfig.setTopicName(topicName);
            }
            if (null != serdeClassName) {
                sourceConfig.setSerdeClassName(serdeClassName);
            }
            if (null != processingGuarantees) {
                sourceConfig.setProcessingGuarantees(processingGuarantees);
            }
            if (parallelism == null) {
                if (sourceConfig.getParallelism() == 0) {
                    sourceConfig.setParallelism(1);
                }
            } else {
                int num = Integer.parseInt(parallelism);
                if (num <= 0) {
                    throw new IllegalArgumentException("The parallelism factor (the number of instances) for the "
                            + "connector must be positive");
                }
                sourceConfig.setParallelism(num);
            }

            if (null == jarFile) {
                throw new IllegalArgumentException("Connector JAR not specfied");
            }
        }

        @Override
        void runCmd() throws Exception {
            if (!areAllRequiredFieldsPresentForSource(sourceConfig)) {
                throw new RuntimeException("Missing arguments");
            }
            admin.functions().createFunction(createSourceConfig(sourceConfig), jarFile);
            print("Created successfully");
        }

        private Class<?> getSourceType(File file) {
            if (!Reflections.classExistsInJar(file, sourceConfig.getClassName())) {
                throw new IllegalArgumentException(String.format("Pulsar function class %s does not exist in jar %s",
                        sourceConfig.getClassName(), jarFile));
            } else if (!Reflections.classInJarImplementsIface(file, sourceConfig.getClassName(), Source.class)) {
                throw new IllegalArgumentException(String.format("The Pulsar function class %s in jar %s implements neither org.apache.pulsar.functions.api.Function nor java.util.function.Function",
                        sourceConfig.getClassName(), jarFile));
            }

            Object userClass = Reflections.createInstance(sourceConfig.getClassName(), file);
            Class<?> typeArg;
            Source source = (Source) userClass;
            if (source == null) {
                throw new IllegalArgumentException(String.format("The Pulsar function class %s could not be instantiated from jar %s",
                        sourceConfig.getClassName(), jarFile));
            }
            typeArg = TypeResolver.resolveRawArgument(Source.class, source.getClass());

            return typeArg;
        }

        protected org.apache.pulsar.functions.proto.Function.FunctionDetails createSourceConfigProto2(SourceConfig sourceConfig)
                throws IOException {
            org.apache.pulsar.functions.proto.Function.FunctionDetails.Builder functionDetailsBuilder
                    = org.apache.pulsar.functions.proto.Function.FunctionDetails.newBuilder();
            Utils.mergeJson(FunctionsImpl.printJson(createSourceConfig(sourceConfig)), functionDetailsBuilder);
            return functionDetailsBuilder.build();
        }

        protected FunctionDetails createSourceConfig(SourceConfig sourceConfig) {

            File file = new File(jarFile);
            try {
                Reflections.loadJar(file);
            } catch (MalformedURLException e) {
                throw new RuntimeException("Failed to load user jar " + file, e);
            }
            Class<?> typeArg = getSourceType(file);

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

            // set up sink spec
            SinkSpec.Builder sinkSpecBuilder = SinkSpec.newBuilder();
            sinkSpecBuilder.setClassName(PulsarSink.class.getName());
            if (sourceConfig.getSerdeClassName() != null && !sourceConfig.getSerdeClassName().isEmpty()) {
                sinkSpecBuilder.setSerDeClassName(sourceConfig.getSerdeClassName());
            }
            sinkSpecBuilder.setTopic(sourceConfig.getTopicName());
            sinkSpecBuilder.setTypeClassName(typeArg.getName());

            functionDetailsBuilder.setSink(sinkSpecBuilder);
            return functionDetailsBuilder.build();
        }
    }

    @Parameters(commandDescription = "Create Pulsar sink connectors")
    class CreateSink extends BaseCommand {

        @Parameter(names = "--fqfn", description = "The Fully Qualified Function Name (FQFN) for the function")
        protected String fqfn;
        @Parameter(names = "--tenant", description = "The function's tenant")
        protected String tenant;
        @Parameter(names = "--namespace", description = "The function's namespace")
        protected String namespace;
        @Parameter(names = "--name", description = "The function's name")
        protected String name;
        @Parameter(names = "--className", description = "The function's class name")
        protected String className;
        @Parameter(names = "--inputs", description = "The function's input topic or topics (multiple topics can be specified as a comma-separated list)")
        protected String inputs;
        @Parameter(names = "--customSerdeInputs", description = "The map of input topics to SerDe class names (as a JSON string)")
        protected String customSerdeInputString;
        @Parameter(names = "--processingGuarantees", description = "The processing guarantees (aka delivery semantics) applied to the Sink")
        protected FunctionConfig.ProcessingGuarantees processingGuarantees;
        @Parameter(names = "--parallelism", description = "")
        protected String parallelism;
        @Parameter(
                names = "--jar",
                description = "Path to the jar file for the function (if the function is written in Java)",
                listConverter = StringConverter.class)
        protected String jarFile;

        @Parameter(names = "--functionConfigFile", description = "The path to a YAML config file specifying the "
                + "function's configuration")
        protected String fnConfigFile;

        protected SinkConfig sinkConfig;

        @Override
        void processArguments() throws Exception {
            super.processArguments();

            if (null != fnConfigFile) {
                this.sinkConfig = loadSinkConfig(fnConfigFile);
            } else {
                this.sinkConfig = new SinkConfig();
            }

            if (null != fqfn) {
                parseFullyQualifiedFunctionName(fqfn, sinkConfig);
            } else {
                if (null != tenant) {
                    sinkConfig.setTenant(tenant);
                }
                if (null != namespace) {
                    sinkConfig.setNamespace(namespace);
                }
                if (null != name) {
                    sinkConfig.setName(name);
                }
            }
            if (null != className) {
                sinkConfig.setClassName(className);
            }
            if (null != processingGuarantees) {
                sinkConfig.setProcessingGuarantees(processingGuarantees);
            }
            Map<String, String> topicsToSerDeClassName = new HashMap<>();
            if (null != inputs) {
                List<String> inputTopics = Arrays.asList(inputs.split(","));
                inputTopics.forEach(new Consumer<String>() {
                    @Override
                    public void accept(String s) {
                        CmdConnectors.validateTopicName(s);
                        topicsToSerDeClassName.put(s, "");
                    }
                });
            }
            if (null != customSerdeInputString) {
                Type type = new TypeToken<Map<String, String>>(){}.getType();
                Map<String, String> customSerdeInputMap = new Gson().fromJson(customSerdeInputString, type);
                customSerdeInputMap.forEach((topic, serde) -> {
                    CmdConnectors.validateTopicName(topic);
                    topicsToSerDeClassName.put(topic, serde);
                });
            }
            sinkConfig.setTopicToSerdeClassName(topicsToSerDeClassName);

            if (parallelism == null) {
                if (sinkConfig.getParallelism() == 0) {
                    sinkConfig.setParallelism(1);
                }
            } else {
                int num = Integer.parseInt(parallelism);
                if (num <= 0) {
                    throw new IllegalArgumentException("The parallelism factor (the number of instances) for the "
                            + "connector must be positive");
                }
                sinkConfig.setParallelism(num);
            }

            if (null == jarFile) {
                throw new IllegalArgumentException("Connector JAR not specfied");
            }
        }

        @Override
        void runCmd() throws Exception {
            log.info("sinkConfig: {}", sinkConfig);
            if (!areAllRequiredFieldsPresentForSink(sinkConfig)) {
                throw new RuntimeException("Missing arguments");
            }
            admin.functions().createFunction(createSinkConfig(sinkConfig), jarFile);
            print("Created successfully");
        }

        private Class<?> getSinkType(File file) {
            if (!Reflections.classExistsInJar(file, sinkConfig.getClassName())) {
                throw new IllegalArgumentException(String.format("Pulsar function class %s does not exist in jar %s",
                        sinkConfig.getClassName(), jarFile));
            } else if (!Reflections.classInJarImplementsIface(file, sinkConfig.getClassName(), Sink.class)) {
                throw new IllegalArgumentException(String.format("The Pulsar function class %s in jar %s implements neither org.apache.pulsar.functions.api.Function nor java.util.function.Function",
                        sinkConfig.getClassName(), jarFile));
            }

            Object userClass = Reflections.createInstance(sinkConfig.getClassName(), file);
            Class<?> typeArg;
            Sink sink = (Sink) userClass;
            if (sink == null) {
                throw new IllegalArgumentException(String.format("The Pulsar function class %s could not be instantiated from jar %s",
                        sinkConfig.getClassName(), jarFile));
            }
            typeArg = TypeResolver.resolveRawArgument(Sink.class, sink.getClass());

            return typeArg;
        }

        protected org.apache.pulsar.functions.proto.Function.FunctionDetails createSinkConfigProto2(SinkConfig sinkConfig)
                throws IOException {
            org.apache.pulsar.functions.proto.Function.FunctionDetails.Builder functionDetailsBuilder
                    = org.apache.pulsar.functions.proto.Function.FunctionDetails.newBuilder();
            Utils.mergeJson(FunctionsImpl.printJson(createSinkConfig(sinkConfig)), functionDetailsBuilder);
            return functionDetailsBuilder.build();
        }

        protected FunctionDetails createSinkConfig(SinkConfig sinkConfig) {

            File file = new File(jarFile);
            try {
                Reflections.loadJar(file);
            } catch (MalformedURLException e) {
                throw new RuntimeException("Failed to load user jar " + file, e);
            }
            Class<?> typeArg = getSinkType(file);

            FunctionDetails.Builder functionDetailsBuilder = FunctionDetails.newBuilder();
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
            SourceSpec.Builder sourceSpecBuilder = SourceSpec.newBuilder();
            sourceSpecBuilder.setClassName(PulsarSource.class.getName());
            sourceSpecBuilder.setSubscriptionType(Function.SubscriptionType.SHARED);
            sourceSpecBuilder.putAllTopicsToSerDeClassName(sinkConfig.getTopicToSerdeClassName());
            sourceSpecBuilder.setTypeClassName(typeArg.getName());
            functionDetailsBuilder.setSource(sourceSpecBuilder);

            // set up sink spec
            SinkSpec.Builder sinkSpecBuilder = SinkSpec.newBuilder();
            sinkSpecBuilder.setClassName(sinkConfig.getClassName());
            sinkSpecBuilder.setConfigs(new Gson().toJson(sinkConfig.getConfigs()));
            sinkSpecBuilder.setTypeClassName(typeArg.getName());
            functionDetailsBuilder.setSink(sinkSpecBuilder);
            return functionDetailsBuilder.build();
        }
    }

    @Parameters(commandDescription = "Stops a Pulsar connector")
    class DeleteConnector extends BaseCommand {

        @Parameter(names = "--fqfn", description = "The Fully Qualified Function Name (FQFN) for the function")
        protected String fqfn;

        @Parameter(names = "--tenant", description = "The function's tenant")
        protected String tenant;

        @Parameter(names = "--namespace", description = "The function's namespace")
        protected String namespace;

        @Parameter(names = "--name", description = "The function's name")
        protected String name;

        @Override
        void processArguments() throws Exception {
            super.processArguments();

            boolean usesSetters = (null != tenant || null != namespace || null != name);
            boolean usesFqfn = (null != fqfn);

            // Throw an exception if --fqfn is set alongside any combination of --tenant, --namespace, and --name
            if (usesFqfn && usesSetters) {
                throw new RuntimeException(
                        "You must specify either a Fully Qualified Function Name (FQFN) or tenant, namespace, and " +
                                "function name");
            } else if (usesFqfn) {
                // If the --fqfn flag is used, parse tenant, namespace, and name using that flag
                String[] fqfnParts = fqfn.split("/");
                if (fqfnParts.length != 3) {
                    throw new RuntimeException(
                            "Fully qualified function names (FQFNs) must be of the form tenant/namespace/name");
                }
                tenant = fqfnParts[0];
                namespace = fqfnParts[1];
                name = fqfnParts[2];
            } else {
                if (null == tenant || null == namespace || null == name) {
                    throw new RuntimeException(
                            "You must specify a tenant, namespace, and name for the function or a Fully Qualified " +
                                    "Function Name (FQFN)");
                }
            }
        }

        @Override
        void runCmd() throws Exception {
            admin.functions().deleteFunction(tenant, namespace, name);
            print("Deleted successfully");
        }
    }

    private static SinkConfig loadSinkConfig(String file) throws IOException {
        return (SinkConfig) loadConfig(file, SinkConfig.class);
    }

    private static SourceConfig loadSourceConfig(String file) throws IOException {
        return (SourceConfig) loadConfig(file, SourceConfig.class);
    }

    private static Object loadConfig(String file, Class<?> clazz) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(file), clazz);
    }

    private void parseFullyQualifiedFunctionName(String fqfn, SourceConfig sourceConfig) {
        String[] args = fqfn.split("/");
        if (args.length != 3) {
            throw new RuntimeException("Fully qualified function names (FQFNs) must be of the form "
                    + "tenant/namespace/name");
        } else {
            sourceConfig.setTenant(args[0]);
            sourceConfig.setNamespace(args[1]);
            sourceConfig.setName(args[2]);
        }
    }

    private void parseFullyQualifiedFunctionName(String fqfn, SinkConfig SinkConfig) {
        String[] args = fqfn.split("/");
        if (args.length != 3) {
            throw new RuntimeException("Fully qualified function names (FQFNs) must be of the form "
                    + "tenant/namespace/name");
        } else {
            SinkConfig.setTenant(args[0]);
            SinkConfig.setNamespace(args[1]);
            SinkConfig.setName(args[2]);
        }
    }

    public static boolean areAllRequiredFieldsPresentForSource(SourceConfig sourceConfig) {
        return sourceConfig.getTenant() != null && !sourceConfig.getTenant().isEmpty()
                && sourceConfig.getNamespace() != null && !sourceConfig.getNamespace().isEmpty()
                && sourceConfig.getName() != null && !sourceConfig.getName().isEmpty()
                && sourceConfig.getClassName() != null && !sourceConfig.getClassName().isEmpty()
                && sourceConfig.getTopicName() != null && !sourceConfig.getTopicName().isEmpty()
                || sourceConfig.getSerdeClassName() != null
                && sourceConfig.getParallelism() > 0;
    }

    public static boolean areAllRequiredFieldsPresentForSink(SinkConfig sinkConfig) {
        return sinkConfig.getTenant() != null && !sinkConfig.getTenant().isEmpty()
                && sinkConfig.getNamespace() != null && !sinkConfig.getNamespace().isEmpty()
                && sinkConfig.getName() != null && !sinkConfig.getName().isEmpty()
                && sinkConfig.getClassName() != null && !sinkConfig.getClassName().isEmpty()
                && sinkConfig.getTopicToSerdeClassName() != null && !sinkConfig.getTopicToSerdeClassName().isEmpty()
                && sinkConfig.getParallelism() > 0;
    }

    private static void validateTopicName(String topic) {
        if (!TopicName.isValid(topic)) {
            throw new IllegalArgumentException(String.format("The topic name %s is invalid", topic));
        }
    }

    private static ProcessingGuarantees convertProcessingGuarantee(
            FunctionConfig.ProcessingGuarantees processingGuarantees) {
        for (ProcessingGuarantees type : ProcessingGuarantees.values()) {
            if (type.name().equals(processingGuarantees.name())) {
                return type;
            }
        }
        throw new RuntimeException("Unrecognized processing guarantee: " + processingGuarantees.name());
    }
}