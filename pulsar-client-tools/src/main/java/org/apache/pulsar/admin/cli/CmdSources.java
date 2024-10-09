/*
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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
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
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.ProducerConfig;
import org.apache.pulsar.common.functions.Resources;
import org.apache.pulsar.common.functions.UpdateOptionsImpl;
import org.apache.pulsar.common.functions.Utils;
import org.apache.pulsar.common.io.BatchSourceConfig;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Getter
@Command(description = "Interface for managing Pulsar IO Sources (ingress data into Pulsar)", aliases = "source")
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

        addCommand("create", createSource);
        addCommand("update", updateSource);
        addCommand("delete", deleteSource);
        addCommand("get", getSource);
        // TODO depecreate getstatus
        addCommand("status", getSourceStatus, "getstatus");
        addCommand("list", listSources);
        addCommand("stop", stopSource);
        addCommand("start", startSource);
        addCommand("restart", restartSource);
        addCommand("localrun", localSourceRunner);
        addCommand("available-sources", new ListBuiltInSources());
        addCommand("reload", new ReloadBuiltInSources());
    }

    /**
     * Base command.
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

    @Command(description = "Run a Pulsar IO source connector locally "
            + "(rather than deploying it to the Pulsar cluster)")
    protected class LocalSourceRunner extends CreateSource {

        @Option(names = "--state-storage-service-url",
                description = "The URL for the state storage service (the default is Apache BookKeeper)")
        protected String stateStorageServiceUrl;
        @Option(names = "--brokerServiceUrl", description = "The URL for the Pulsar broker", hidden = true)
        protected String deprecatedBrokerServiceUrl;
        @Option(names = "--broker-service-url", description = "The URL for the Pulsar broker")
        protected String brokerServiceUrl;

        @Option(names = "--clientAuthPlugin",
                description = "Client authentication plugin using which function-process can connect to broker",
                hidden = true)
        protected String deprecatedClientAuthPlugin;
        @Option(names = "--client-auth-plugin",
                description = "Client authentication plugin using which function-process can connect to broker")
        protected String clientAuthPlugin;

        @Option(names = "--clientAuthParams", description = "Client authentication param", hidden = true)
        protected String deprecatedClientAuthParams;
        @Option(names = "--client-auth-params", description = "Client authentication param")
        protected String clientAuthParams;

        @Option(names = "--use_tls", description = "Use tls connection", hidden = true)
        protected Boolean deprecatedUseTls;
        @Option(names = "--use-tls", description = "Use tls connection")
        protected boolean useTls;

        @Option(names = "--tls_allow_insecure", description = "Allow insecure tls connection", hidden = true)
        protected Boolean deprecatedTlsAllowInsecureConnection;
        @Option(names = "--tls-allow-insecure", description = "Allow insecure tls connection")
        protected boolean tlsAllowInsecureConnection;

        @Option(names = "--hostname_verification_enabled",
                description = "Enable hostname verification", hidden = true)
        protected Boolean deprecatedTlsHostNameVerificationEnabled;
        @Option(names = "--hostname-verification-enabled", description = "Enable hostname verification")
        protected boolean tlsHostNameVerificationEnabled;

        @Option(names = "--tls_trust_cert_path", description = "tls trust cert file path", hidden = true)
        protected String deprecatedTlsTrustCertFilePath;
        @Option(names = "--tls-trust-cert-path", description = "tls trust cert file path")
        protected String tlsTrustCertFilePath;

        @Option(names = "--secrets-provider-classname", description = "Whats the classname for secrets provider")
        protected String secretsProviderClassName;
        @Option(names = "--secrets-provider-config",
                description = "Config that needs to be passed to secrets provider")
        protected String secretsProviderConfig;
        @Option(names = "--metrics-port-start", description = "The starting port range for metrics server")
        protected String metricsPortStart;

        private void mergeArgs() {
            if (isBlank(brokerServiceUrl) && !isBlank(deprecatedBrokerServiceUrl)) {
                brokerServiceUrl = deprecatedBrokerServiceUrl;
            }
            if (isBlank(clientAuthPlugin) && !isBlank(deprecatedClientAuthPlugin)) {
                clientAuthPlugin = deprecatedClientAuthPlugin;
            }
            if (isBlank(clientAuthParams) && !isBlank(deprecatedClientAuthParams)) {
                clientAuthParams = deprecatedClientAuthParams;
            }
            if (!useTls && deprecatedUseTls != null) {
                useTls = deprecatedUseTls;
            }
            if (!tlsAllowInsecureConnection && deprecatedTlsAllowInsecureConnection != null) {
                tlsAllowInsecureConnection = deprecatedTlsAllowInsecureConnection;
            }
            if (!tlsHostNameVerificationEnabled && deprecatedTlsHostNameVerificationEnabled != null) {
                tlsHostNameVerificationEnabled = deprecatedTlsHostNameVerificationEnabled;
            }
            if (isBlank(tlsTrustCertFilePath) && !isBlank(deprecatedTlsTrustCertFilePath)) {
                tlsTrustCertFilePath = deprecatedTlsTrustCertFilePath;
            }
        }

        @VisibleForTesting
        List<String> getLocalRunArgs() throws Exception {
            // merge deprecated args with new args
            mergeArgs();
            List<String> localRunArgs = new LinkedList<>();
            localRunArgs.add(System.getenv("PULSAR_HOME") + "/bin/function-localrunner");
            localRunArgs.add("--sourceConfig");
            localRunArgs.add(new Gson().toJson(sourceConfig));
            for (Field field : this.getClass().getDeclaredFields()) {
                if (field.getName().toUpperCase().startsWith("DEPRECATED")) {
                    continue;
                }
                if (field.getName().contains("$")) {
                    continue;
                }
                Object value = field.get(this);
                if (value != null) {
                    localRunArgs.add("--" + field.getName());
                    localRunArgs.add(value.toString());
                }
            }
            return localRunArgs;
        }

        @Override
        public void runCmd() throws Exception {
            ProcessBuilder processBuilder = new ProcessBuilder(getLocalRunArgs()).inheritIO();
            Process process = processBuilder.start();
            process.waitFor();
        }

        @Override
        protected String validateSourceType(String sourceType) {
            return sourceType;
        }
    }

    @Command(description = "Submit a Pulsar IO source connector to run in a Pulsar cluster")
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

    @Command(description = "Update a Pulsar IO source connector")
    protected class UpdateSource extends SourceDetailsCommand {

        @Option(names = "--update-auth-data", description = "Whether or not to update the auth data")
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
        @Option(names = "--tenant", description = "The source's tenant")
        protected String tenant;
        @Option(names = "--namespace", description = "The source's namespace")
        protected String namespace;
        @Option(names = "--name", description = "The source's name")
        protected String name;

        @Option(names = {"-t", "--source-type"}, description = "The source's connector provider")
        protected String sourceType;

        @Option(names = "--processingGuarantees",
                description = "The processing guarantees (aka delivery semantics) applied to the Source", hidden = true)
        protected FunctionConfig.ProcessingGuarantees deprecatedProcessingGuarantees;
        @Option(names = "--processing-guarantees",
                description = "The processing guarantees (as known as delivery semantics) applied to the source."
                    + " A source connector receives messages from external system and writes messages to a Pulsar"
                    + " topic. The '--processing-guarantees' is used to ensure the processing guarantees for writing"
                    + " messages to the Pulsar topic. The available values are `ATLEAST_ONCE`, `ATMOST_ONCE`,"
                    + " `EFFECTIVELY_ONCE`. If it is not specified, `ATLEAST_ONCE` delivery guarantee is used.")
        protected FunctionConfig.ProcessingGuarantees processingGuarantees;

        @Option(names = { "-o", "--destinationTopicName" },
                description = "The Pulsar topic to which data is sent", hidden = true)
        protected String deprecatedDestinationTopicName;
        @Option(names = "--destination-topic-name", description = "The Pulsar topic to which data is sent")
        protected String destinationTopicName;
        @Option(names = "--producer-config", description = "The custom producer configuration (as a JSON string)")
        protected String producerConfig;

        @Option(names = "--batch-builder", description = "BatchBuilder provides two types of "
                + "batch construction methods, DEFAULT and KEY_BASED. The default value is: DEFAULT")
        protected String batchBuilder;

        @Option(names = "--deserializationClassName",
                description = "The SerDe classname for the source", hidden = true)
        protected String deprecatedDeserializationClassName;
        @Option(names = "--deserialization-classname", description = "The SerDe classname for the source")
        protected String deserializationClassName;

        @Option(names = { "-st",
                "--schema-type" }, description = "The schema type (either a builtin schema like 'avro', 'json', etc.."
                + " or custom Schema class name to be used to encode messages emitted from the source")
        protected String schemaType;

        @Option(names = "--parallelism",
                description = "The source's parallelism factor (i.e. the number of source instances to run)")
        protected Integer parallelism;
        @Option(names = { "-a", "--archive" },
                description = "The path to the NAR archive for the Source. It also supports url-path "
                        + "[http/https/file (file protocol assumes that file already exists on worker host)] "
                        + "from which worker can download the package.")
        protected String archive;
        @Option(names = "--className",
                description = "The source's class name if archive is file-url-path (file://)", hidden = true)
        protected String deprecatedClassName;
        @Option(names = "--classname", description = "The source's class name if archive is file-url-path (file://)")
        protected String className;
        @Option(names = "--sourceConfigFile", description = "The path to a YAML config file specifying the "
                + "source's configuration", hidden = true)
        protected String deprecatedSourceConfigFile;
        @Option(names = "--source-config-file", description = "The path to a YAML config file specifying the "
                + "source's configuration")
        protected String sourceConfigFile;
        @Option(names = "--cpu", description = "The CPU (in cores) that needs to be allocated "
                + "per source instance (applicable only to Docker runtime)")
        protected Double cpu;
        @Option(names = "--ram", description = "The RAM (in bytes) that need to be allocated "
                + "per source instance (applicable only to the process and Docker runtimes)")
        protected Long ram;
        @Option(names = "--disk", description = "The disk (in bytes) that need to be allocated "
                + "per source instance (applicable only to Docker runtime)")
        protected Long disk;
        @Option(names = "--sourceConfig", description = "Source config key/values", hidden = true)
        protected String deprecatedSourceConfigString;
        @Option(names = "--source-config", description = "Source config key/values")
        protected String sourceConfigString;
        @Option(names = "--batch-source-config", description = "Batch source config key/values")
        protected String batchSourceConfigString;
        @Option(names = "--custom-runtime-options", description = "A string that encodes options to "
                + "customize the runtime, see docs for configured runtime for details")
        protected String customRuntimeOptions;
        @Option(names = "--secrets", description = "The map of secretName to an object that encapsulates "
                + "how the secret is fetched by the underlying secrets provider")
        protected String secretsString;
        @Option(names = "--log-topic", description = "The topic to which the logs of a Pulsar Sink are produced")
        protected String logTopic;
        @Option(names = "--runtime-flags", description = "Any flags that you want to pass to a runtime"
                + " (for process & Kubernetes runtime only).")
        protected String runtimeFlags;

        protected SourceConfig sourceConfig;

        private void mergeArgs() {
            if (processingGuarantees == null && deprecatedProcessingGuarantees != null) {
                processingGuarantees = deprecatedProcessingGuarantees;
            }
            if (isBlank(destinationTopicName) && !isBlank(deprecatedDestinationTopicName)) {
                destinationTopicName = deprecatedDestinationTopicName;
            }
            if (isBlank(deserializationClassName) && !isBlank(deprecatedDeserializationClassName)) {
                deserializationClassName = deprecatedDeserializationClassName;
            }
            if (isBlank(className) && !isBlank(deprecatedClassName)) {
                className = deprecatedClassName;
            }
            if (isBlank(sourceConfigFile) && !isBlank(deprecatedSourceConfigFile)) {
                sourceConfigFile = deprecatedSourceConfigFile;
            }
            if (isBlank(sourceConfigString) && !isBlank(deprecatedSourceConfigString)) {
                sourceConfigString = deprecatedSourceConfigString;
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

            if (archive != null && (sourceType != null || sourceConfig.getSourceType() != null)) {
                throw new ParameterException("Cannot specify both archive and source-type");
            }

            if (archive != null) {
                sourceConfig.setArchive(archive);
            }

            if (sourceType != null) {
                sourceConfig.setArchive(validateSourceType(sourceType));
            } else if (sourceConfig.getSourceType() != null) {
                sourceConfig.setArchive(validateSourceType(sourceConfig.getSourceType()));
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
            if (null != logTopic) {
                sourceConfig.setLogTopic(logTopic);
            }
            if (null != runtimeFlags) {
                sourceConfig.setRuntimeFlags(runtimeFlags);
            }

            // check if source configs are valid
            validateSourceConfigs(sourceConfig);
        }

        protected Map<String, Object> parseConfigs(String str) throws JsonProcessingException {
            ObjectMapper mapper = ObjectMapperFactory.getMapper().getObjectMapper();
            TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {};

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
            if (!Utils.isFunctionPackageUrlSupported(sourceConfig.getArchive())
                    && !sourceConfig.getArchive().startsWith(Utils.BUILTIN)) {
                if (!new File(sourceConfig.getArchive()).exists()) {
                    throw new IllegalArgumentException(String.format("Source Archive %s does not exist",
                            sourceConfig.getArchive()));
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
                availableSources = getAdmin().sources().getBuiltInSources().stream()
                        .map(ConnectorDefinition::getName).collect(Collectors.toSet());
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
     * Function level command.
     */
    @Getter
    abstract class SourceCommand extends BaseCommand {
        @Option(names = "--tenant", description = "The source's tenant")
        protected String tenant;

        @Option(names = "--namespace", description = "The source's namespace")
        protected String namespace;

        @Option(names = "--name", description = "The source's name")
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

    @Command(description = "Stops a Pulsar IO source connector")
    protected class DeleteSource extends SourceCommand {

        @Override
        void runCmd() throws Exception {
            getAdmin().sources().deleteSource(tenant, namespace, sourceName);
            print("Delete source successfully");
        }
    }

    @Command(description = "Gets the information about a Pulsar IO source connector")
    protected class GetSource extends SourceCommand {

        @Override
        void runCmd() throws Exception {
            SourceConfig sourceConfig = getAdmin().sources().getSource(tenant, namespace, sourceName);
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            System.out.println(gson.toJson(sourceConfig));
        }
    }

    /**
     * List Sources command.
     */
    @Command(description = "List all running Pulsar IO source connectors")
    protected class ListSources extends BaseCommand {
        @Option(names = "--tenant", description = "The source's tenant")
        protected String tenant;

        @Option(names = "--namespace", description = "The source's namespace")
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

    @Command(description = "Check the current status of a Pulsar Source")
    class GetSourceStatus extends SourceCommand {

        @Option(names = "--instance-id",
                description = "The source instanceId (Get-status of all instances if instance-id is not provided")
        protected String instanceId;

        @Override
        void runCmd() throws Exception {
            if (isBlank(instanceId)) {
                print(getAdmin().sources().getSourceStatus(tenant, namespace, sourceName));
            } else {
                print(getAdmin().sources()
                        .getSourceStatus(tenant, namespace, sourceName, Integer.parseInt(instanceId)));
            }
        }
    }

    @Command(description = "Restart source instance")
    class RestartSource extends SourceCommand {

        @Option(names = "--instance-id",
                description = "The source instanceId (restart all instances if instance-id is not provided")
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

    @Command(description = "Stop source instance")
    class StopSource extends SourceCommand {

        @Option(names = "--instance-id",
                description = "The source instanceId (stop all instances if instance-id is not provided")
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

    @Command(description = "Start source instance")
    class StartSource extends SourceCommand {

        @Option(names = "--instance-id",
                description = "The source instanceId (start all instances if instance-id is not provided")
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

    @Command(description = "Get the list of Pulsar IO connector sources supported by Pulsar cluster")
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

    @Command(description = "Reload the available built-in connectors")
    public class ReloadBuiltInSources extends BaseCommand {

        @Override
        void runCmd() throws Exception {
            getAdmin().sources().reloadBuiltInSources();
        }
    }
}
