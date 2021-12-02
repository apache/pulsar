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

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.pulsar.common.naming.TopicName.DEFAULT_NAMESPACE;
import static org.apache.pulsar.common.naming.TopicName.PUBLIC_TENANT;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.StringConverter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.WordUtils;
import org.apache.pulsar.admin.cli.utils.CmdUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.functions.ProducerConfig;
import org.apache.pulsar.common.functions.Resources;
import org.apache.pulsar.common.functions.UpdateOptionsImpl;
import org.apache.pulsar.common.io.BatchSourceConfig;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.common.functions.Utils;
import org.apache.pulsar.common.util.ObjectMapperFactory;

@Getter
@Parameters(commandDescription = "Interface for managing Pulsar IO Sources (ingress data into Pulsar)")
@Slf4j
public class CmdSources extends CmdBase {

    private final CreateSource createSource;
    private final DeleteSource deleteSource;
    private final GetSource getSource;
    private final GetSourceStatus getSourceStatus;
    private final ListSources listSources;
    private final UpdateSource updateSource;
    private final RestartSource restartSource;
    private final StopSource stopSource;
    private final StartSource startSource;
    private final LocalSourceRunner localSourceRunner;

    public CmdSources(Supplier<PulsarAdmin> admin) {
        super("sources", admin);
        createSource = new CreateSource();
        updateSource = new UpdateSource();
        deleteSource = new DeleteSource();
        listSources = new ListSources();
        getSource = new GetSource();
        getSourceStatus = new GetSourceStatus();
        restartSource = new RestartSource();
        stopSource = new StopSource();
        startSource = new StartSource();
        localSourceRunner = new LocalSourceRunner();

        jcommander.addCommand("create", createSource);
        jcommander.addCommand("update", updateSource);
        jcommander.addCommand("delete", deleteSource);
        jcommander.addCommand("get", getSource);
        // TODO depecreate getstatus
        jcommander.addCommand("status", getSourceStatus, "getstatus");
        jcommander.addCommand("list", listSources);
        jcommander.addCommand("stop", stopSource);
        jcommander.addCommand("start", startSource);
        jcommander.addCommand("restart", restartSource);
        jcommander.addCommand("localrun", localSourceRunner);
        jcommander.addCommand("available-sources", new ListBuiltInSources());
        jcommander.addCommand("reload", new ReloadBuiltInSources());
    }

    /**
     * Base command
     */
    @Getter
    abstract class BaseCommand extends CliCommand {
        @Override
        void run() throws Exception {
            try {
                processArguments();
            } catch (Exception e) {
                System.err.println(e.getMessage());
                System.err.println();
                String chosenCommand = jcommander.getParsedCommand();
                getUsageFormatter().usage(chosenCommand);
                return;
            }
            runCmd();
        }

        void processArguments() throws Exception {
        }

        abstract void runCmd() throws Exception;
    }

    @Parameters(commandDescription = "Run a Pulsar IO source connector locally (rather than deploying it to the Pulsar cluster)")
    protected class LocalSourceRunner extends CreateSource {

        @Parameter(names = "--state-storage-service-url", description = "The URL for the state storage service (the default is Apache BookKeeper)")
        protected String stateStorageServiceUrl;
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

        @Parameter(names = "--secrets-provider-classname", description = "Whats the classname for secrets provider")
        protected String secretsProviderClassName;
        @Parameter(names = "--secrets-provider-config", description = "Config that needs to be passed to secrets provider")
        protected String secretsProviderConfig;
        @Parameter(names = "--metrics-port-start", description = "The starting port range for metrics server")
        protected String metricsPortStart;

        private void mergeArgs() {
            if (isBlank(brokerServiceUrl) && !isBlank(DEPRECATED_brokerServiceUrl)) {
                brokerServiceUrl = DEPRECATED_brokerServiceUrl;
            }
            if (isBlank(clientAuthPlugin) && !isBlank(DEPRECATED_clientAuthPlugin)) {
                clientAuthPlugin = DEPRECATED_clientAuthPlugin;
            }
            if (isBlank(clientAuthParams) && !isBlank(DEPRECATED_clientAuthParams)) {
                clientAuthParams = DEPRECATED_clientAuthParams;
            }
            if (!useTls && DEPRECATED_useTls != null) {
                useTls = DEPRECATED_useTls;
            }
            if (!tlsAllowInsecureConnection && DEPRECATED_tlsAllowInsecureConnection != null) {
                tlsAllowInsecureConnection = DEPRECATED_tlsAllowInsecureConnection;
            }
            if (!tlsHostNameVerificationEnabled && DEPRECATED_tlsHostNameVerificationEnabled != null) {
                tlsHostNameVerificationEnabled = DEPRECATED_tlsHostNameVerificationEnabled;
            }
            if (isBlank(tlsTrustCertFilePath) && !isBlank(DEPRECATED_tlsTrustCertFilePath)) {
                tlsTrustCertFilePath = DEPRECATED_tlsTrustCertFilePath;
            }
        }

        @Override
        public void runCmd() throws Exception {
            // merge deprecated args with new args
            mergeArgs();

            List<String> localRunArgs = new LinkedList<>();
            localRunArgs.add(System.getenv("PULSAR_HOME") + "/bin/function-localrunner");
            localRunArgs.add("--sourceConfig");
            localRunArgs.add(new Gson().toJson(sourceConfig));
            for (Field field : this.getClass().getDeclaredFields()) {
                if (field.getName().startsWith("DEPRECATED")) continue;
                if(field.getName().contains("$")) continue;
                Object value = field.get(this);
                if (value != null) {
                    localRunArgs.add("--" + field.getName());
                    localRunArgs.add(value.toString());
                }
            }
            ProcessBuilder processBuilder = new ProcessBuilder(localRunArgs).inheritIO();
            Process process = processBuilder.start();
            process.waitFor();
        }

        @Override
        protected String validateSourceType(String sourceType) {
            return sourceType;
        }
    }

    @Parameters(commandDescription = "Submit a Pulsar IO source connector to run in a Pulsar cluster")
    protected class CreateSource extends SourceDetailsCommand {
        @Override
        void runCmd() throws Exception {
            if (Utils.isFunctionPackageUrlSupported(this.sourceConfig.getArchive())) {
                getAdmin().sources().createSourceWithUrl(sourceConfig, sourceConfig.getArchive());
            } else {
                getAdmin().sources().createSource(sourceConfig, sourceConfig.getArchive());
            }
            print("Created successfully");
        }
    }

    @Parameters(commandDescription = "Update a Pulsar IO source connector")
    protected class UpdateSource extends SourceDetailsCommand {

        @Parameter(names = "--update-auth-data", description = "Whether or not to update the auth data")
        protected boolean updateAuthData;

        @Override
        void runCmd() throws Exception {
            UpdateOptionsImpl updateOptions = new UpdateOptionsImpl();
            updateOptions.setUpdateAuthData(updateAuthData);
            if (Utils.isFunctionPackageUrlSupported(sourceConfig.getArchive())) {
                getAdmin().sources().updateSourceWithUrl(sourceConfig, sourceConfig.getArchive(), updateOptions);
            } else {
                getAdmin().sources().updateSource(sourceConfig, sourceConfig.getArchive(), updateOptions);
            }
            print("Updated successfully");
        }

        protected void validateSourceConfigs(SourceConfig sourceConfig) {
            if (sourceConfig.getTenant() == null) {
                sourceConfig.setTenant(PUBLIC_TENANT);
            }
            if (sourceConfig.getNamespace() == null) {
                sourceConfig.setNamespace(DEFAULT_NAMESPACE);
            }
        }
    }

    abstract class SourceDetailsCommand extends BaseCommand {
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
        @Parameter(names = "--producer-config", description = "The custom producer configuration (as a JSON string)")
        protected String producerConfig;

        @Parameter(names = "--batch-builder", description = "BatchBuilder provides two types of batch construction methods, DEFAULT and KEY_BASED. The default value is: DEFAULT")
        protected String batchBuilder;

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
        @Parameter(names = "--batch-source-config", description = "Batch source config key/values")
        protected String batchSourceConfigString;
        @Parameter(names = "--custom-runtime-options", description = "A string that encodes options to customize the runtime, see docs for configured runtime for details")
        protected String customRuntimeOptions;
        @Parameter(names = "--secrets", description = "The map of secretName to an object that encapsulates how the secret is fetched by the underlying secrets provider")
        protected String secretsString;

        protected SourceConfig sourceConfig;

        private void mergeArgs() {
            if (processingGuarantees == null && DEPRECATED_processingGuarantees != null) {
                processingGuarantees = DEPRECATED_processingGuarantees;
            }
            if (isBlank(destinationTopicName) && !isBlank(DEPRECATED_destinationTopicName)) {
                destinationTopicName = DEPRECATED_destinationTopicName;
            }
            if (isBlank(deserializationClassName) && !isBlank(DEPRECATED_deserializationClassName)) {
                deserializationClassName = DEPRECATED_deserializationClassName;
            }
            if (isBlank(className) && !isBlank(DEPRECATED_className)) {
                className = DEPRECATED_className;
            }
            if (isBlank(sourceConfigFile) && !isBlank(DEPRECATED_sourceConfigFile)) {
                sourceConfigFile = DEPRECATED_sourceConfigFile;
            }
            if (isBlank(sourceConfigString) && !isBlank(DEPRECATED_sourceConfigString)) {
                sourceConfigString = DEPRECATED_sourceConfigString;
            }
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
            if (null != producerConfig) {
                Type type = new TypeToken<ProducerConfig>() {}.getType();
                sourceConfig.setProducerConfig(new Gson().fromJson(producerConfig, type));
            }
            if (null != deserializationClassName) {
                sourceConfig.setSerdeClassName(deserializationClassName);
            }
            if (null != schemaType) {
                sourceConfig.setSchemaType(schemaType);
            }

            if (null != batchBuilder) {
                sourceConfig.setBatchBuilder(batchBuilder);
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

            Resources resources = sourceConfig.getResources();
            if (cpu != null) {
                if (resources == null) {
                    resources = new Resources();
                }
                resources.setCpu(cpu);
            }

            if (ram != null) {
                if (resources == null) {
                    resources = new Resources();
                }
                resources.setRam(ram);
            }

            if (disk != null) {
                if (resources == null) {
                    resources = new Resources();
                }
                resources.setDisk(disk);
            }
            if (resources != null) {
                sourceConfig.setResources(resources);
            }

            try {
                if (null != sourceConfigString) {
                    sourceConfig.setConfigs(parseConfigs(sourceConfigString));
                }
            } catch (Exception ex) {
                throw new ParameterException("Cannot parse source-config", ex);
            }
            
            if (null != batchSourceConfigString) {
            	sourceConfig.setBatchSourceConfig(parseBatchSourceConfigs(batchSourceConfigString));
            }

            if (customRuntimeOptions != null) {
                sourceConfig.setCustomRuntimeOptions(customRuntimeOptions);
            }

            if (secretsString != null) {
                Type type = new TypeToken<Map<String, Object>>() {}.getType();
                Map<String, Object> secretsMap = new Gson().fromJson(secretsString, type);
                if (secretsMap == null) {
                    secretsMap = Collections.emptyMap();
                }
                sourceConfig.setSecrets(secretsMap);
            }
            
            // check if source configs are valid
            validateSourceConfigs(sourceConfig);
        }

        protected Map<String, Object> parseConfigs(String str) throws JsonProcessingException {
            ObjectMapper mapper = ObjectMapperFactory.getThreadLocal();
            TypeReference<HashMap<String,Object>> typeRef
                    = new TypeReference<HashMap<String,Object>>() {};

            return mapper.readValue(str, typeRef);
        }

            protected BatchSourceConfig parseBatchSourceConfigs(String str) {
        	return new Gson().fromJson(str, BatchSourceConfig.class);
        }

        protected void validateSourceConfigs(SourceConfig sourceConfig) {
            if (isBlank(sourceConfig.getArchive())) {
                throw new ParameterException("Source archive not specified");
            }
            org.apache.pulsar.common.functions.Utils.inferMissingArguments(sourceConfig);
            if (!Utils.isFunctionPackageUrlSupported(sourceConfig.getArchive()) &&
                !sourceConfig.getArchive().startsWith(Utils.BUILTIN)) {
                if (!new File(sourceConfig.getArchive()).exists()) {
                    throw new IllegalArgumentException(String.format("Source Archive %s does not exist", sourceConfig.getArchive()));
                }
            }
            if (isBlank(sourceConfig.getName())) {
                throw new IllegalArgumentException("Source name not specified");
            }
            
            if (sourceConfig.getBatchSourceConfig() != null) {
            	validateBatchSourceConfigs(sourceConfig.getBatchSourceConfig());
            }
        }
        
        protected void validateBatchSourceConfigs(BatchSourceConfig batchSourceConfig) {
           if (isBlank(batchSourceConfig.getDiscoveryTriggererClassName())) {
             throw new IllegalArgumentException("Discovery Triggerer not specified");
           } 
        }

        protected String validateSourceType(String sourceType) throws IOException {
            Set<String> availableSources;
            try {
                availableSources = getAdmin().sources().getBuiltInSources().stream().map(ConnectorDefinition::getName).collect(Collectors.toSet());
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

    /**
     * Function level command
     */
    @Getter
    abstract class SourceCommand extends BaseCommand {
        @Parameter(names = "--tenant", description = "The source's tenant")
        protected String tenant;

        @Parameter(names = "--namespace", description = "The source's namespace")
        protected String namespace;

        @Parameter(names = "--name", description = "The source's name")
        protected String sourceName;

        @Override
        void processArguments() throws Exception {
            super.processArguments();
            if (tenant == null) {
                tenant = PUBLIC_TENANT;
            }
            if (namespace == null) {
                namespace = DEFAULT_NAMESPACE;
            }
            if (null == sourceName) {
                throw new RuntimeException(
                            "You must specify a name for the source");
            }
        }
    }

    @Parameters(commandDescription = "Stops a Pulsar IO source connector")
    protected class DeleteSource extends SourceCommand {

        @Override
        void runCmd() throws Exception {
            getAdmin().sources().deleteSource(tenant, namespace, sourceName);
            print("Delete source successfully");
        }
    }

    @Parameters(commandDescription = "Gets the information about a Pulsar IO source connector")
    protected class GetSource extends SourceCommand {

        @Override
        void runCmd() throws Exception {
            SourceConfig sourceConfig = getAdmin().sources().getSource(tenant, namespace, sourceName);
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            System.out.println(gson.toJson(sourceConfig));
        }
    }

    /**
     * List Sources command
     */
    @Parameters(commandDescription = "List all running Pulsar IO source connectors")
    protected class ListSources extends BaseCommand {
        @Parameter(names = "--tenant", description = "The sink's tenant")
        protected String tenant;

        @Parameter(names = "--namespace", description = "The sink's namespace")
        protected String namespace;

        @Override
        public void processArguments() {
            if (tenant == null) {
                tenant = PUBLIC_TENANT;
            }
            if (namespace == null) {
                namespace = DEFAULT_NAMESPACE;
            }
        }

        @Override
        void runCmd() throws Exception {
            List<String> sources = getAdmin().sources().listSources(tenant, namespace);
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            System.out.println(gson.toJson(sources));
        }
    }

    @Parameters(commandDescription = "Check the current status of a Pulsar Source")
    class GetSourceStatus extends SourceCommand {

        @Parameter(names = "--instance-id", description = "The source instanceId (Get-status of all instances if instance-id is not provided")
        protected String instanceId;

        @Override
        void runCmd() throws Exception {
            if (isBlank(instanceId)) {
                print(getAdmin().sources().getSourceStatus(tenant, namespace, sourceName));
            } else {
                print(getAdmin().sources().getSourceStatus(tenant, namespace, sourceName, Integer.parseInt(instanceId)));
            }
        }
    }

    @Parameters(commandDescription = "Restart source instance")
    class RestartSource extends SourceCommand {

        @Parameter(names = "--instance-id", description = "The source instanceId (restart all instances if instance-id is not provided")
        protected String instanceId;

        @Override
        void runCmd() throws Exception {
            if (isNotBlank(instanceId)) {
                try {
                    getAdmin().sources().restartSource(tenant, namespace, sourceName, Integer.parseInt(instanceId));
                } catch (NumberFormatException e) {
                    System.err.println("instance-id must be a number");
                }
            } else {
                getAdmin().sources().restartSource(tenant, namespace, sourceName);
            }
            System.out.println("Restarted successfully");
        }
    }

    @Parameters(commandDescription = "Stop source instance")
    class StopSource extends SourceCommand {

        @Parameter(names = "--instance-id", description = "The source instanceId (stop all instances if instance-id is not provided")
        protected String instanceId;

        @Override
        void runCmd() throws Exception {
            if (isNotBlank(instanceId)) {
                try {
                    getAdmin().sources().stopSource(tenant, namespace, sourceName, Integer.parseInt(instanceId));
                } catch (NumberFormatException e) {
                    System.err.println("instance-id must be a number");
                }
            } else {
                getAdmin().sources().stopSource(tenant, namespace, sourceName);
            }
            System.out.println("Stopped successfully");
        }
    }

    @Parameters(commandDescription = "Start source instance")
    class StartSource extends SourceCommand {

        @Parameter(names = "--instance-id", description = "The source instanceId (start all instances if instance-id is not provided")
        protected String instanceId;

        @Override
        void runCmd() throws Exception {
            if (isNotBlank(instanceId)) {
                try {
                    getAdmin().sources().startSource(tenant, namespace, sourceName, Integer.parseInt(instanceId));
                } catch (NumberFormatException e) {
                    System.err.println("instance-id must be a number");
                }
            } else {
                getAdmin().sources().startSource(tenant, namespace, sourceName);
            }
            System.out.println("Started successfully");
        }
    }

    @Parameters(commandDescription = "Get the list of Pulsar IO connector sources supported by Pulsar cluster")
    public class ListBuiltInSources extends BaseCommand {
        @Override
        void runCmd() throws Exception {
            getAdmin().sources().getBuiltInSources().stream().filter(x -> !StringUtils.isEmpty(x.getSourceClass()))
                    .forEach(connector -> {
                        System.out.println(connector.getName());
                        System.out.println(WordUtils.wrap(connector.getDescription(), 80));
                        System.out.println("----------------------------------------");
                    });
        }
    }

    @Parameters(commandDescription = "Reload the available built-in connectors")
    public class ReloadBuiltInSources extends BaseCommand {

        @Override
        void runCmd() throws Exception {
            getAdmin().sources().reloadBuiltInSources();
        }
    }
}
