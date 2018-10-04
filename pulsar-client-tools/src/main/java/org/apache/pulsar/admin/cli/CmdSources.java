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

import static org.apache.pulsar.common.naming.TopicName.DEFAULT_NAMESPACE;
import static org.apache.pulsar.common.naming.TopicName.PUBLIC_TENANT;
import static org.apache.pulsar.functions.utils.Utils.fileExists;
import static org.apache.pulsar.functions.worker.Utils.downloadFromHttpUrl;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.StringConverter;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.WordUtils;
import org.apache.pulsar.admin.cli.utils.CmdUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.internal.FunctionsImpl;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.functions.instance.AuthenticationConfig;
import org.apache.pulsar.functions.utils.FunctionConfig;
import org.apache.pulsar.functions.utils.SourceConfig;
import org.apache.pulsar.functions.utils.SourceConfigUtils;
import org.apache.pulsar.functions.utils.Utils;
import org.apache.pulsar.functions.utils.io.ConnectorUtils;
import org.apache.pulsar.functions.utils.io.Connectors;
import org.apache.pulsar.functions.utils.validation.ConfigValidation;

@Getter
@Parameters(commandDescription = "Interface for managing Pulsar IO Sources (ingress data into Pulsar)")
@Slf4j
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
        jcommander.addCommand("available-sources", new ListSources());
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

    @Parameters(commandDescription = "Run a Pulsar IO source connector locally (rather than deploying it to the Pulsar cluster)")
    protected class LocalSourceRunner extends CreateSource {

        @Parameter(names = "--brokerServiceUrl", description = "The URL for the Pulsar broker", hidden = true)
        protected String DEPRECATED_brokerServiceUrl;
        @Parameter(names = "--broker-service-url", description = "The URL for the Pulsar broker")
        protected String brokerServiceUrl;

        @Parameter(names = "--clientAuthPlugin", description = "Client authentication plugin using which function-process can connect to broker", hidden = true)
        protected String DEPRECATED_clientAuthPlugin;
        @Parameter(names = "--client-auth-plugin", description = "Client authentication plugin using which function-process can connect to broker")
        protected String clientAuthPlugin;

        @Parameter(names = "--clientAuthParams", description = "Client authentication param", hidden = true)
        protected String DEPRECATED_clientAuthParams;
        @Parameter(names = "--client-auth-params", description = "Client authentication param")
        protected String clientAuthParams;

        @Parameter(names = "--use_tls", description = "Use tls connection", hidden = true)
        protected Boolean DEPRECATED_useTls;
        @Parameter(names = "--use-tls", description = "Use tls connection")
        protected boolean useTls;

        @Parameter(names = "--tls_allow_insecure", description = "Allow insecure tls connection", hidden = true)
        protected Boolean DEPRECATED_tlsAllowInsecureConnection;
        @Parameter(names = "--tls-allow-insecure", description = "Allow insecure tls connection")
        protected boolean tlsAllowInsecureConnection;

        @Parameter(names = "--hostname_verification_enabled", description = "Enable hostname verification", hidden = true)
        protected Boolean DEPRECATED_tlsHostNameVerificationEnabled;
        @Parameter(names = "--hostname-verification-enabled", description = "Enable hostname verification")
        protected boolean tlsHostNameVerificationEnabled;

        @Parameter(names = "--tls_trust_cert_path", description = "tls trust cert file path", hidden = true)
        protected String DEPRECATED_tlsTrustCertFilePath;
        @Parameter(names = "--tls-trust-cert-path", description = "tls trust cert file path")
        protected String tlsTrustCertFilePath;

        private void mergeArgs() {
            if (!StringUtils.isBlank(DEPRECATED_brokerServiceUrl)) brokerServiceUrl = DEPRECATED_brokerServiceUrl;
            if (!StringUtils.isBlank(DEPRECATED_clientAuthPlugin)) clientAuthPlugin = DEPRECATED_clientAuthPlugin;
            if (!StringUtils.isBlank(DEPRECATED_clientAuthParams)) clientAuthParams = DEPRECATED_clientAuthParams;
            if (DEPRECATED_useTls != null) useTls = DEPRECATED_useTls;
            if (DEPRECATED_tlsAllowInsecureConnection != null) tlsAllowInsecureConnection = DEPRECATED_tlsAllowInsecureConnection;
            if (DEPRECATED_tlsHostNameVerificationEnabled != null) tlsHostNameVerificationEnabled = DEPRECATED_tlsHostNameVerificationEnabled;
            if (!StringUtils.isBlank(DEPRECATED_tlsTrustCertFilePath)) tlsTrustCertFilePath = DEPRECATED_tlsTrustCertFilePath;
        }

        @Override
        void runCmd() throws Exception {
            // merge deprecated args with new args
            mergeArgs();

            CmdFunctions.startLocalRun(createSourceConfigProto2(sourceConfig), sourceConfig.getParallelism(),
                    0, brokerServiceUrl, null,
                    AuthenticationConfig.builder().clientAuthenticationPlugin(clientAuthPlugin)
                            .clientAuthenticationParameters(clientAuthParams).useTls(useTls)
                            .tlsAllowInsecureConnection(tlsAllowInsecureConnection)
                            .tlsHostnameVerificationEnable(tlsHostNameVerificationEnabled)
                            .tlsTrustCertsFilePath(tlsTrustCertFilePath).build(),
                    sourceConfig.getArchive(), admin);
        }

        @Override
        protected String validateSourceType(String sourceType) throws IOException {
            // Validate the connector source type from the locally available connectors
            String pulsarHome = System.getenv("PULSAR_HOME");
            if (pulsarHome == null) {
                pulsarHome = Paths.get("").toAbsolutePath().toString();
            }
            String connectorsDir = Paths.get(pulsarHome, "connectors").toString();
            Connectors connectors = ConnectorUtils.searchForConnectors(connectorsDir);

            if (!connectors.getSources().containsKey(sourceType)) {
                throw new ParameterException("Invalid source type '" + sourceType + "' -- Available sources are: "
                        + connectors.getSources().keySet());
            }

            // Source type is a valid built-in connector type. For local-run we'll fill it up with its own archive path
            return connectors.getSources().get(sourceType).toString();
        }
    }

    @Parameters(commandDescription = "Submit a Pulsar IO source connector to run in a Pulsar cluster")
    protected class CreateSource extends SourceCommand {
        @Override
        void runCmd() throws Exception {
            if (Utils.isFunctionPackageUrlSupported(this.sourceConfig.getArchive())) {
                admin.functions().createFunctionWithUrl(SourceConfigUtils.convert(sourceConfig), sourceConfig.getArchive());
            } else {
                admin.functions().createFunction(SourceConfigUtils.convert(sourceConfig), sourceConfig.getArchive());
            }
            print("Created successfully");
        }
    }

    @Parameters(commandDescription = "Update a Pulsar IO source connector")
    protected class UpdateSource extends SourceCommand {
        @Override
        void runCmd() throws Exception {
            if (Utils.isFunctionPackageUrlSupported(sourceConfig.getArchive())) {
                admin.functions().updateFunctionWithUrl(SourceConfigUtils.convert(sourceConfig), sourceConfig.getArchive());
            } else {
                admin.functions().updateFunction(SourceConfigUtils.convert(sourceConfig), sourceConfig.getArchive());
            }
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

        @Parameter(names = { "-t", "--source-type" }, description = "The source's connector provider")
        protected String sourceType;

        @Parameter(names = "--processingGuarantees", description = "The processing guarantees (aka delivery semantics) applied to the Source", hidden = true)
        protected FunctionConfig.ProcessingGuarantees DEPRECATED_processingGuarantees;
        @Parameter(names = "--processing-guarantees", description = "The processing guarantees (aka delivery semantics) applied to the source")
        protected FunctionConfig.ProcessingGuarantees processingGuarantees;

        @Parameter(names = { "-o", "--destinationTopicName" }, description = "The Pulsar topic to which data is sent", hidden = true)
        protected String DEPRECATED_destinationTopicName;
        @Parameter(names = "--destination-topic-name", description = "The Pulsar topic to which data is sent")
        protected String destinationTopicName;

        @Parameter(names = "--deserializationClassName", description = "The SerDe classname for the source", hidden = true)
        protected String DEPRECATED_deserializationClassName;
        @Parameter(names = "--deserialization-classname", description = "The SerDe classname for the source")
        protected String deserializationClassName;

        @Parameter(names = { "-st",
                "--schema-type" }, description = "The schema type (either a builtin schema like 'avro', 'json', etc.."
                        + " or custom Schema class name to be used to encode messages emitted from the source")
        protected String schemaType;

        @Parameter(names = "--parallelism", description = "The source's parallelism factor (i.e. the number of source instances to run)")
        protected Integer parallelism;
        @Parameter(names = { "-a", "--archive" },
                description = "The path to the NAR archive for the Source. It also supports url-path [http/https/file (file protocol assumes that file already exists on worker host)] from which worker can download the package.", listConverter = StringConverter.class)
        protected String archive;
        @Parameter(names = "--className", description = "The source's class name if archive is file-url-path (file://)", hidden = true)
        protected String DEPRECATED_className;
        @Parameter(names = "--classname", description = "The source's class name if archive is file-url-path (file://)")
        protected String className;
        @Parameter(names = "--sourceConfigFile", description = "The path to a YAML config file specifying the "
                + "source's configuration", hidden = true)
        protected String DEPRECATED_sourceConfigFile;
        @Parameter(names = "--source-config-file", description = "The path to a YAML config file specifying the "
                + "source's configuration")
        protected String sourceConfigFile;
        @Parameter(names = "--cpu", description = "The CPU (in cores) that needs to be allocated per source instance (applicable only to Docker runtime)")
        protected Double cpu;
        @Parameter(names = "--ram", description = "The RAM (in bytes) that need to be allocated per source instance (applicable only to the process and Docker runtimes)")
        protected Long ram;
        @Parameter(names = "--disk", description = "The disk (in bytes) that need to be allocated per source instance (applicable only to Docker runtime)")
        protected Long disk;
        @Parameter(names = "--sourceConfig", description = "Source config key/values", hidden = true)
        protected String DEPRECATED_sourceConfigString;
        @Parameter(names = "--source-config", description = "Source config key/values")
        protected String sourceConfigString;

        protected SourceConfig sourceConfig;

        private void mergeArgs() {
            if (DEPRECATED_processingGuarantees != null) processingGuarantees = DEPRECATED_processingGuarantees;
            if (!StringUtils.isBlank(DEPRECATED_destinationTopicName)) destinationTopicName = DEPRECATED_destinationTopicName;
            if (!StringUtils.isBlank(DEPRECATED_deserializationClassName)) deserializationClassName = DEPRECATED_deserializationClassName;
            if (!StringUtils.isBlank(DEPRECATED_className)) className = DEPRECATED_className;
            if (!StringUtils.isBlank(DEPRECATED_sourceConfigFile)) sourceConfigFile = DEPRECATED_sourceConfigFile;
            if (!StringUtils.isBlank(DEPRECATED_sourceConfigString)) sourceConfigString = DEPRECATED_sourceConfigString;
        }

        @Override
        void processArguments() throws Exception {
            super.processArguments();
            // merge deprecated args with new args
            mergeArgs();

            if (null != sourceConfigFile) {
                this.sourceConfig = CmdUtils.loadConfig(sourceConfigFile, SourceConfig.class);
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
            if (null != schemaType) {
                sourceConfig.setSchemaType(schemaType);
            }

            if (null != processingGuarantees) {
                sourceConfig.setProcessingGuarantees(processingGuarantees);
            }
            if (parallelism != null) {
                sourceConfig.setParallelism(parallelism);
            }

            if (archive != null && sourceType != null) {
                throw new ParameterException("Cannot specify both archive and source-type");
            }

            if (archive != null) {
                sourceConfig.setArchive(archive);
            }

            if (sourceType != null) {
                sourceConfig.setArchive(validateSourceType(sourceType));
            }

            org.apache.pulsar.functions.utils.Resources resources = sourceConfig.getResources();
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
            sourceConfig.setResources(resources);

            if (null != sourceConfigString) {
                sourceConfig.setConfigs(parseConfigs(sourceConfigString));
            }

            inferMissingArguments(sourceConfig);

            // check if source configs are valid
            validateSourceConfigs(sourceConfig);
        }

        protected Map<String, Object> parseConfigs(String str) {
            Type type = new TypeToken<Map<String, String>>() {
            }.getType();
            return new Gson().fromJson(str, type);
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
            if (StringUtils.isBlank(sourceConfig.getArchive())) {
                throw new ParameterException("Source archive not specfied");
            }

            boolean isConnectorBuiltin = sourceConfig.getArchive().startsWith(Utils.BUILTIN);
            boolean isArchivePathUrl = Utils.isFunctionPackageUrlSupported(sourceConfig.getArchive());

            String archivePath = null;
            if (isArchivePathUrl) {
                // download archive file if url is http
                if(sourceConfig.getArchive().startsWith(Utils.HTTP)) {
                    File tempPkgFile = null;
                    try {
                        tempPkgFile = downloadFromHttpUrl(sourceConfig.getArchive(), sourceConfig.getName());
                        archivePath = tempPkgFile.getAbsolutePath();
                    } catch(Exception e) {
                        if(tempPkgFile!=null ) {
                            tempPkgFile.deleteOnExit();
                        }
                        throw new ParameterException("Failed to download archive from " + sourceConfig.getArchive()
                                + ", due to =" + e.getMessage());
                    }
                }
            } else if (isConnectorBuiltin) {
                // Ignore local checks when submitting built-in connector
                archivePath = null;
            } else {
                archivePath = sourceConfig.getArchive();
            }


            // if jar file is present locally then load jar and validate SinkClass in it
            ClassLoader classLoader = null;
            if (archivePath != null) {
                if (!fileExists(archivePath)) {
                    throw new ParameterException("Archive file " + archivePath + " does not exist");
                }

                try {
                    ConnectorDefinition connector = ConnectorUtils.getConnectorDefinition(archivePath);
                    log.info("Connector: {}", connector);
                } catch (IOException e) {
                    throw new ParameterException("Connector from " + archivePath + " has error: " + e.getMessage());
                }

                try {
                    classLoader = NarClassLoader.getFromArchive(new File(archivePath),
                            Collections.emptySet());
                } catch (IOException e) {
                    throw new IllegalArgumentException(e);
                }
            }

            try {
             // Need to load jar and set context class loader before calling
                ConfigValidation.validateConfig(sourceConfig, FunctionConfig.Runtime.JAVA.name(), classLoader);
            } catch (Exception e) {
                throw new ParameterException(e.getMessage());
            }
        }

        protected org.apache.pulsar.functions.proto.Function.FunctionDetails createSourceConfigProto2(SourceConfig sourceConfig)
                throws IOException {
            org.apache.pulsar.functions.proto.Function.FunctionDetails.Builder functionDetailsBuilder
                    = org.apache.pulsar.functions.proto.Function.FunctionDetails.newBuilder();
            Utils.mergeJson(FunctionsImpl.printJson(SourceConfigUtils.convert(sourceConfig)), functionDetailsBuilder);
            return functionDetailsBuilder.build();
        }

        protected String validateSourceType(String sourceType) throws IOException {
            Set<String> availableSources;
            try {
                availableSources = admin.functions().getSources();
            } catch (PulsarAdminException e) {
                throw new IOException(e);
            }

            if (!availableSources.contains(sourceType)) {
                throw new ParameterException(
                        "Invalid source type '" + sourceType + "' -- Available sources are: " + availableSources);
            }

            // Source type is a valid built-in connector type
            return "builtin://" + sourceType;
        }
    }

    @Parameters(commandDescription = "Stops a Pulsar IO source connector")
    protected class DeleteSource extends BaseCommand {

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
                throw new ParameterException(
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

    @Parameters(commandDescription = "Get the list of Pulsar IO connector sources supported by Pulsar cluster")
    public class ListSources extends BaseCommand {
        @Override
        void runCmd() throws Exception {
            admin.functions().getConnectorsList().stream().filter(x -> !StringUtils.isEmpty(x.getSourceClass()))
                    .forEach(connector -> {
                        System.out.println(connector.getName());
                        System.out.println(WordUtils.wrap(connector.getDescription(), 80));
                        System.out.println("----------------------------------------");
                    });
        }
    }
}
