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
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.WordUtils;
import org.apache.pulsar.admin.cli.utils.CmdUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.internal.FunctionsImpl;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.functions.api.utils.IdentityFunction;
import org.apache.pulsar.functions.instance.AuthenticationConfig;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.Function.Resources;
import org.apache.pulsar.functions.proto.Function.SinkSpec;
import org.apache.pulsar.functions.proto.Function.SourceSpec;
import org.apache.pulsar.functions.utils.FunctionConfig;
import org.apache.pulsar.functions.utils.SinkConfig;
import org.apache.pulsar.functions.utils.Utils;
import org.apache.pulsar.functions.utils.io.ConnectorUtils;
import org.apache.pulsar.functions.utils.validation.ConfigValidation;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.pulsar.common.naming.TopicName.DEFAULT_NAMESPACE;
import static org.apache.pulsar.common.naming.TopicName.PUBLIC_TENANT;
import static org.apache.pulsar.functions.utils.Utils.convertProcessingGuarantee;
import static org.apache.pulsar.functions.utils.Utils.fileExists;
import static org.apache.pulsar.functions.worker.Utils.downloadFromHttpUrl;

@Getter
@Parameters(commandDescription = "Interface for managing Pulsar IO sinks (egress data from Pulsar)")
@Slf4j
public class CmdSinks extends CmdBase {

    private final CreateSink createSink;
    private final UpdateSink updateSink;
    private final DeleteSink deleteSink;
    private final LocalSinkRunner localSinkRunner;

    public CmdSinks(PulsarAdmin admin) {
        super("sink", admin);
        createSink = new CreateSink();
        updateSink = new UpdateSink();
        deleteSink = new DeleteSink();
        localSinkRunner = new LocalSinkRunner();

        jcommander.addCommand("create", createSink);
        jcommander.addCommand("update", updateSink);
        jcommander.addCommand("delete", deleteSink);
        jcommander.addCommand("localrun", localSinkRunner);
        jcommander.addCommand("available-sinks", new ListSinks());
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

    @Parameters(commandDescription = "Run a Pulsar IO sink connector locally (rather than deploying it to the Pulsar cluster)")
    protected class LocalSinkRunner extends CreateSink {

        @Parameter(names = "--brokerServiceUrl", description = "The URL for the Pulsar broker")
        protected String brokerServiceUrl;

        @Parameter(names = "--clientAuthPlugin", description = "Client authentication plugin using which function-process can connect to broker")
        protected String clientAuthPlugin;

        @Parameter(names = "--clientAuthParams", description = "Client authentication param")
        protected String clientAuthParams;

        @Parameter(names = "--use_tls", description = "Use tls connection\n")
        protected boolean useTls;

        @Parameter(names = "--tls_allow_insecure", description = "Allow insecure tls connection\n")
        protected boolean tlsAllowInsecureConnection;

        @Parameter(names = "--hostname_verification_enabled", description = "Enable hostname verification")
        protected boolean tlsHostNameVerificationEnabled;

        @Parameter(names = "--tls_trust_cert_path", description = "tls trust cert file path")
        protected String tlsTrustCertFilePath;

        @Override
        void runCmd() throws Exception {
            CmdFunctions.startLocalRun(createSinkConfigProto2(sinkConfig), sinkConfig.getParallelism(),
                    0, brokerServiceUrl, null,
                    AuthenticationConfig.builder().clientAuthenticationPlugin(clientAuthPlugin)
                            .clientAuthenticationParameters(clientAuthParams).useTls(useTls)
                            .tlsAllowInsecureConnection(tlsAllowInsecureConnection)
                            .tlsHostnameVerificationEnable(tlsHostNameVerificationEnabled)
                            .tlsTrustCertsFilePath(tlsTrustCertFilePath).build(),
                    sinkConfig.getArchive(), admin);
        }
    }

    @Parameters(commandDescription = "Submit a Pulsar IO sink connector to run in a Pulsar cluster")
    protected class CreateSink extends SinkCommand {
        @Override
        void runCmd() throws Exception {
            if (Utils.isFunctionPackageUrlSupported(archive)) {
                admin.functions().createFunctionWithUrl(createSinkConfig(sinkConfig), sinkConfig.getArchive());
            } else {
                admin.functions().createFunction(createSinkConfig(sinkConfig), sinkConfig.getArchive());
            }
            print("Created successfully");
        }
    }

    @Parameters(commandDescription = "Update a Pulsar IO sink connector")
    protected class UpdateSink extends SinkCommand {
        @Override
        void runCmd() throws Exception {
            if (Utils.isFunctionPackageUrlSupported(archive)) {
                admin.functions().updateFunctionWithUrl(createSinkConfig(sinkConfig), sinkConfig.getArchive());
            } else {
                admin.functions().updateFunction(createSinkConfig(sinkConfig), sinkConfig.getArchive());
            }
            print("Updated successfully");
        }
    }

    abstract class SinkCommand extends BaseCommand {
        @Parameter(names = "--tenant", description = "The sink's tenant")
        protected String tenant;
        @Parameter(names = "--namespace", description = "The sink's namespace")
        protected String namespace;
        @Parameter(names = "--name", description = "The sink's name")
        protected String name;
        @Parameter(names = "--inputs", description = "The sink's input topic or topics (multiple topics can be specified as a comma-separated list)")
        protected String inputs;
        @Parameter(names = "--topicsPattern", description = "TopicsPattern to consume from list of topics under a namespace that match the pattern. [--input] and [--topicsPattern] are mutually exclusive. Add SerDe class name for a pattern in --customSerdeInputs  (supported for java fun only)")
        protected String topicsPattern;
        @Parameter(names = "--customSerdeInputs", description = "The map of input topics to SerDe class names (as a JSON string)")
        protected String customSerdeInputString;
        @Parameter(names = "--processingGuarantees", description = "The processing guarantees (aka delivery semantics) applied to the sink")
        protected FunctionConfig.ProcessingGuarantees processingGuarantees;
        @Parameter(names = "--parallelism", description = "The sink's parallelism factor (i.e. the number of sink instances to run)")
        protected Integer parallelism;
        @Parameter(names = {"-a", "--archive"}, description = "Path to the archive file for the sink. It also supports url-path [http/https/file (file protocol assumes that file already exists on worker host)] from which worker can download the package.", listConverter = StringConverter.class)
        protected String archive;

        @Parameter(names = "--sinkConfigFile", description = "The path to a YAML config file specifying the "
                + "sink's configuration")
        protected String sinkConfigFile;
        @Parameter(names = "--cpu", description = "The CPU (in cores) that needs to be allocated per sink instance (applicable only to Docker runtime)")
        protected Double cpu;
        @Parameter(names = "--ram", description = "The RAM (in bytes) that need to be allocated per sink instance (applicable only to the process and Docker runtimes)")
        protected Long ram;
        @Parameter(names = "--disk", description = "The disk (in bytes) that need to be allocated per sink instance (applicable only to Docker runtime)")
        protected Long disk;
        @Parameter(names = "--sinkConfig", description = "User defined configs key/values")
        protected String sinkConfigString;

        protected SinkConfig sinkConfig;

        @Override
        void processArguments() throws Exception {
            super.processArguments();

            if (null != sinkConfigFile) {
                this.sinkConfig = CmdUtils.loadConfig(sinkConfigFile, SinkConfig.class);
            } else {
                this.sinkConfig = new SinkConfig();
            }

            if (null != tenant) {
                sinkConfig.setTenant(tenant);
            }

            if (null != namespace) {
                sinkConfig.setNamespace(namespace);
            }

            if (null != name) {
                sinkConfig.setName(name);
            }
            if (null != processingGuarantees) {
                sinkConfig.setProcessingGuarantees(processingGuarantees);
            }

            Map<String, String> topicsToSerDeClassName = new HashMap<>();
            if (null != inputs) {
                parseInputs(inputs, topicsToSerDeClassName);
            }
            if (null != customSerdeInputString) {
                parseCustomSerdeInput(customSerdeInputString, topicsToSerDeClassName);
            }

            if (!topicsToSerDeClassName.isEmpty()) {
                sinkConfig.setTopicToSerdeClassName(topicsToSerDeClassName);
            }
            
            if (null != topicsPattern) {
                sinkConfig.setTopicsPattern(topicsPattern);
            }

            if (parallelism != null) {
                sinkConfig.setParallelism(parallelism);
            }

            if (null != archive) {
                sinkConfig.setArchive(archive);
            }

            org.apache.pulsar.functions.utils.Resources resources = sinkConfig.getResources();
            if (resources == null) {
                resources = new org.apache.pulsar.functions.utils.Resources();
            }
            if (cpu != null) {
                resources.setCpu(cpu);
            }

            if (ram != null) {
                resources.setRam(ram);
            }

            if (disk != null) {
                resources.setDisk(disk);
            }
            sinkConfig.setResources(resources);

            if (null != sinkConfigString) {
                sinkConfig.setConfigs(parseConfigs(sinkConfigString));
            }

            inferMissingArguments(sinkConfig);
        }

        protected Map<String, Object> parseConfigs(String str) {
            Type type = new TypeToken<Map<String, String>>(){}.getType();
            return new Gson().fromJson(str, type);
        }

        protected void parseCustomSerdeInput(String str, Map<String, String> topicsToSerDeClassName) {
            Type type = new TypeToken<Map<String, String>>(){}.getType();
            Map<String, String> customSerdeInputMap = new Gson().fromJson(str, type);
            customSerdeInputMap.forEach((topic, serde) -> {
                topicsToSerDeClassName.put(topic, serde);
            });
        }

        protected void parseInputs(String str, Map<String, String> topicsToSerDeClassName) {
            List<String> inputTopics = Arrays.asList(str.split(","));
            inputTopics.forEach(s -> topicsToSerDeClassName.put(s, ""));
        }

        protected void inferMissingArguments(SinkConfig sinkConfig) {
            if (sinkConfig.getTenant() == null) {
                sinkConfig.setTenant(PUBLIC_TENANT);
            }
            if (sinkConfig.getNamespace() == null) {
                sinkConfig.setNamespace(DEFAULT_NAMESPACE);
            }
        }

        protected void validateSinkConfigs(SinkConfig sinkConfig) {

            if (isBlank(sinkConfig.getArchive())) {
                throw new ParameterException("Sink archive not specfied");
            }

            boolean isArchivePathUrl = Utils.isFunctionPackageUrlSupported(sinkConfig.getArchive());

            String archivePath = null;
            if (isArchivePathUrl) {
                // download jar file if url is http
                if(sinkConfig.getArchive().startsWith(Utils.HTTP)) {
                    File tempPkgFile = null;
                    try {
                        tempPkgFile = File.createTempFile(sinkConfig.getName(), "sink");
                        downloadFromHttpUrl(sinkConfig.getArchive(), new FileOutputStream(tempPkgFile));
                        archivePath = tempPkgFile.getAbsolutePath();
                    } catch(Exception e) {
                        if(tempPkgFile!=null ) {
                            tempPkgFile.deleteOnExit();
                        }
                        throw new ParameterException("Failed to download archive from " + sinkConfig.getArchive()
                                + ", due to =" + e.getMessage());
                    }
                }
            } else {
                archivePath = sinkConfig.getArchive();
            }

            // if jar file is present locally then load jar and validate SinkClass in it
            if (archivePath != null) {
                if (!fileExists(archivePath)) {
                    throw new ParameterException("Archive file " + archivePath + " does not exist");
                }

                try {
                    ConnectorDefinition connector = ConnectorUtils.getConnectorDefinition(archivePath);
                    log.info("Connector: {}", connector);
                    // Validate sink class
                    ConnectorUtils.getIOSinkClass(archivePath);
                } catch (IOException e) {
                    throw new ParameterException("Connector from " + archivePath + " has error: " + e.getMessage());
                }
            }

            try {
                // Need to load jar and set context class loader before calling
                ConfigValidation.validateConfig(sinkConfig, FunctionConfig.Runtime.JAVA.name());
            } catch (Exception e) {
                throw new ParameterException(e.getMessage());
            }
        }


        protected org.apache.pulsar.functions.proto.Function.FunctionDetails createSinkConfigProto2(SinkConfig sinkConfig)
                throws IOException {
            org.apache.pulsar.functions.proto.Function.FunctionDetails.Builder functionDetailsBuilder
                    = org.apache.pulsar.functions.proto.Function.FunctionDetails.newBuilder();
            Utils.mergeJson(FunctionsImpl.printJson(createSinkConfig(sinkConfig)), functionDetailsBuilder);
            return functionDetailsBuilder.build();
        }

        protected FunctionDetails createSinkConfig(SinkConfig sinkConfig) throws IOException {

            // check if configs are valid
            validateSinkConfigs(sinkConfig);

            String sinkClassName = ConnectorUtils.getIOSinkClass(sinkConfig.getArchive());

            String typeArg;
            try (NarClassLoader ncl = NarClassLoader.getFromArchive(new File(sinkConfig.getArchive()),
                    Collections.emptySet())) {
                typeArg = sinkConfig.getArchive().startsWith(Utils.FILE) ? null
                        : Utils.getSinkType(sinkClassName, ncl).getName();
            }

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
            // source spec classname should be empty so that the default pulsar source will be used
            SourceSpec.Builder sourceSpecBuilder = SourceSpec.newBuilder();
            sourceSpecBuilder.setSubscriptionType(Function.SubscriptionType.SHARED);
            if (sinkConfig.getTopicToSerdeClassName() != null) {
                sourceSpecBuilder.putAllTopicsToSerDeClassName(sinkConfig.getTopicToSerdeClassName());
            }

            if (sinkConfig.getTopicsPattern() != null) {
                sourceSpecBuilder.setTopicsPattern(sinkConfig.getTopicsPattern());
            }
            if (typeArg != null) {
                sourceSpecBuilder.setTypeClassName(typeArg);
            }
            functionDetailsBuilder.setAutoAck(true);
            functionDetailsBuilder.setSource(sourceSpecBuilder);

            // set up sink spec
            SinkSpec.Builder sinkSpecBuilder = SinkSpec.newBuilder();
            sinkSpecBuilder.setClassName(sinkClassName);
            if (sinkConfig.getConfigs() != null) {
                sinkSpecBuilder.setConfigs(new Gson().toJson(sinkConfig.getConfigs()));
            }
            if (typeArg != null) {
                sinkSpecBuilder.setTypeClassName(typeArg);
            }
            functionDetailsBuilder.setSink(sinkSpecBuilder);

            if (sinkConfig.getResources() != null) {
                Resources.Builder bldr = Resources.newBuilder();
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

    @Parameters(commandDescription = "Stops a Pulsar IO sink connector")
    protected class DeleteSink extends BaseCommand {

        @Parameter(names = "--tenant", description = "The tenant of the sink")
        protected String tenant;

        @Parameter(names = "--namespace", description = "The namespace of the sink")
        protected String namespace;

        @Parameter(names = "--name", description = "The name of the sink")
        protected String name;

        @Override
        void processArguments() throws Exception {
            super.processArguments();
            if (null == name) {
                throw new ParameterException(
                        "You must specify a name for the sink");
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
            print("Deleted successfully");
        }
    }

    @Parameters(commandDescription = "Get the list of Pulsar IO connector sinks supported by Pulsar cluster")
    public class ListSinks extends SinkCommand {
        @Override
        void runCmd() throws Exception {
            admin.functions().getConnectorsList().stream().filter(x -> !StringUtils.isEmpty(x.getSinkClass()))
                    .forEach(connector -> {
                        System.out.println(connector.getName());
                        System.out.println(WordUtils.wrap(connector.getDescription(), 80));
                        System.out.println("----------------------------------------");
                    });
        }
    }
}
