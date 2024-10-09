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
import java.util.Arrays;
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
import org.apache.commons.lang3.text.WordUtils;
import org.apache.pulsar.admin.cli.utils.CmdUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.functions.ConsumerConfig;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.Resources;
import org.apache.pulsar.common.functions.UpdateOptionsImpl;
import org.apache.pulsar.common.functions.Utils;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Getter
@Command(description = "Interface for managing Pulsar IO sinks (egress data from Pulsar)", aliases = "sink")
@Slf4j
public class CmdSinks extends CmdBase {

    private final CreateSink createSink;
    private final UpdateSink updateSink;
    private final DeleteSink deleteSink;
    private final ListSinks listSinks;
    private final GetSink getSink;
    private final GetSinkStatus getSinkStatus;
    private final StopSink stopSink;
    private final StartSink startSink;
    private final RestartSink restartSink;
    private final LocalSinkRunner localSinkRunner;

    public CmdSinks(Supplier<PulsarAdmin> admin) {
        super("sinks", admin);
        createSink = new CreateSink();
        updateSink = new UpdateSink();
        deleteSink = new DeleteSink();
        listSinks = new ListSinks();
        getSink = new GetSink();
        getSinkStatus = new GetSinkStatus();
        stopSink = new StopSink();
        startSink = new StartSink();
        restartSink = new RestartSink();
        localSinkRunner = new LocalSinkRunner();

        addCommand("create", createSink);
        addCommand("update", updateSink);
        addCommand("delete", deleteSink);
        addCommand("list", listSinks);
        addCommand("get", getSink);
        // TODO deprecate getstatus
        addCommand("status", getSinkStatus, "getstatus");
        addCommand("stop", stopSink);
        addCommand("start", startSink);
        addCommand("restart", restartSink);
        addCommand("localrun", localSinkRunner);
        addCommand("available-sinks", new ListBuiltInSinks());
        addCommand("reload", new ReloadBuiltInSinks());
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

    @Command(description = "Run a Pulsar IO sink connector locally "
            + "(rather than deploying it to the Pulsar cluster)")
    protected class LocalSinkRunner extends CreateSink {

        @Option(names = "--state-storage-service-url",
                description = "The URL for the state storage service (the default is Apache BookKeeper)")
        protected String stateStorageServiceUrl;
        @Option(names = "--brokerServiceUrl", description = "The URL for the Pulsar broker", hidden = true)
        protected String deprecatedBrokerServiceUrl;
        @Option(names = "--broker-service-url", description = "The URL for the Pulsar broker")
        protected String brokerServiceUrl;

        @Option(names = "--clientAuthPlugin", description = "Client authentication plugin using "
                + "which function-process can connect to broker", hidden = true)
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
            localRunArgs.add("--sinkConfig");
            localRunArgs.add(new Gson().toJson(sinkConfig));
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
        protected String validateSinkType(String sinkType) {
            return sinkType;
        }
    }

    @Command(description = "Submit a Pulsar IO sink connector to run in a Pulsar cluster")
    protected class CreateSink extends SinkDetailsCommand {
        @Override
        void runCmd() throws Exception {
            if (Utils.isFunctionPackageUrlSupported(archive)) {
                getAdmin().sinks().createSinkWithUrl(sinkConfig, sinkConfig.getArchive());
            } else {
                getAdmin().sinks().createSink(sinkConfig, sinkConfig.getArchive());
            }
            print("Created successfully");
        }
    }

    @Command(description = "Update a Pulsar IO sink connector")
    protected class UpdateSink extends SinkDetailsCommand {

        @Option(names = "--update-auth-data", description = "Whether or not to update the auth data")
        protected boolean updateAuthData;

        @Override
        void runCmd() throws Exception {
            UpdateOptionsImpl updateOptions = new UpdateOptionsImpl();
            updateOptions.setUpdateAuthData(updateAuthData);
            if (Utils.isFunctionPackageUrlSupported(archive)) {
                getAdmin().sinks().updateSinkWithUrl(sinkConfig, sinkConfig.getArchive(), updateOptions);
            } else {
                getAdmin().sinks().updateSink(sinkConfig, sinkConfig.getArchive(), updateOptions);
            }
            print("Updated successfully");
        }

        protected void validateSinkConfigs(SinkConfig sinkConfig) {
            if (sinkConfig.getTenant() == null) {
                sinkConfig.setTenant(PUBLIC_TENANT);
            }
            if (sinkConfig.getNamespace() == null) {
                sinkConfig.setNamespace(DEFAULT_NAMESPACE);
            }
        }
    }

    abstract class SinkDetailsCommand extends BaseCommand {
        @Option(names = "--tenant", description = "The sink's tenant")
        protected String tenant;
        @Option(names = "--namespace", description = "The sink's namespace")
        protected String namespace;
        @Option(names = "--name", description = "The sink's name")
        protected String name;

        @Option(names = { "-t", "--sink-type" }, description = "The sinks's connector provider")
        protected String sinkType;

        @Option(names = "--cleanup-subscription", description = "Whether delete the subscription "
                + "when sink is deleted")
        protected Boolean cleanupSubscription;

        @Option(names = { "-i",
                "--inputs" }, description = "The sink's input topic or topics "
                + "(multiple topics can be specified as a comma-separated list)")
        protected String inputs;

        @Option(names = "--topicsPattern", description = "TopicsPattern to consume from list of topics "
                + "under a namespace that match the pattern. [--input] and [--topicsPattern] are mutually exclusive. "
                + "Add SerDe class name for a pattern in --customSerdeInputs  (supported for java fun only)",
                hidden = true)
        protected String deprecatedTopicsPattern;
        @Option(names = "--topics-pattern", description = "The topic pattern to consume from a list of topics "
                + "under a namespace that matches the pattern. [--input] and [--topics-pattern] are mutually "
                + "exclusive. Add SerDe class name for a pattern in --custom-serde-inputs")
        protected String topicsPattern;

        @Option(names = "--subsName", description = "Pulsar source subscription name "
                + "if user wants a specific subscription-name for input-topic consumer", hidden = true)
        protected String deprecatedSubsName;
        @Option(names = "--subs-name", description = "Pulsar source subscription name "
                + "if user wants a specific subscription-name for input-topic consumer")
        protected String subsName;

        @Option(names = "--subs-position", description = "Pulsar source subscription position "
                + "if user wants to consume messages from the specified location")
        protected SubscriptionInitialPosition subsPosition;

        @Option(names = "--customSerdeInputs",
                description = "The map of input topics to SerDe class names (as a JSON string)", hidden = true)
        protected String deprecatedCustomSerdeInputString;
        @Option(names = "--custom-serde-inputs",
                description = "The map of input topics to SerDe class names (as a JSON string)")
        protected String customSerdeInputString;

        @Option(names = "--custom-schema-inputs",
                description = "The map of input topics to Schema types or class names (as a JSON string)")
        protected String customSchemaInputString;

        @Option(names = "--input-specs",
                description = "The map of inputs to custom configuration (as a JSON string)")
        protected String inputSpecs;

        @Option(names = "--max-redeliver-count", description = "Maximum number of times that a message "
                + "will be redelivered before being sent to the dead letter queue")
        protected Integer maxMessageRetries;
        @Option(names = "--dead-letter-topic",
                description = "Name of the dead topic where the failing messages will be sent.")
        protected String deadLetterTopic;

        @Option(names = "--processingGuarantees",
                description = "The processing guarantees (aka delivery semantics) applied to the sink", hidden = true)
        protected FunctionConfig.ProcessingGuarantees deprecatedProcessingGuarantees;
        @Option(names = "--processing-guarantees",
                description = "The processing guarantees (as known as delivery semantics) applied to the sink."
                    + " The '--processing-guarantees' implementation in Pulsar also relies on sink implementation."
                    + " The available values are `ATLEAST_ONCE`, `ATMOST_ONCE`, `EFFECTIVELY_ONCE`."
                    + " If it is not specified, `ATLEAST_ONCE` delivery guarantee is used.")
        protected FunctionConfig.ProcessingGuarantees processingGuarantees;
        @Option(names = "--retainOrdering", description = "Sink consumes and sinks messages in order", hidden = true)
        protected Boolean deprecatedRetainOrdering;
        @Option(names = "--retain-ordering", description = "Sink consumes and sinks messages in order")
        protected Boolean retainOrdering;
        @Option(names = "--parallelism",
                description = "The sink's parallelism factor (i.e. the number of sink instances to run)")
        protected Integer parallelism;
        @Option(names = "--retain-key-ordering",
                description = "Sink consumes and processes messages in key order")
        protected Boolean retainKeyOrdering;
        @Option(names = {"-a", "--archive"}, description = "Path to the archive file for the sink. It also supports "
                + "url-path [http/https/file (file protocol assumes that file already exists on worker host)] from "
                + "which worker can download the package.")
        protected String archive;
        @Option(names = "--className",
                description = "The sink's class name if archive is file-url-path (file://)", hidden = true)
        protected String deprecatedClassName;
        @Option(names = "--classname", description = "The sink's class name if archive is file-url-path (file://)")
        protected String className;

        @Option(names = "--sinkConfigFile", description = "The path to a YAML config file specifying the "
                + "sink's configuration", hidden = true)
        protected String deprecatedSinkConfigFile;
        @Option(names = "--sink-config-file", description = "The path to a YAML config file specifying the "
                + "sink's configuration")
        protected String sinkConfigFile;
        @Option(names = "--cpu", description = "The CPU (in cores) that needs to be allocated "
                + "per sink instance (applicable only to Docker runtime)")
        protected Double cpu;
        @Option(names = "--ram", description = "The RAM (in bytes) that need to be allocated "
                + "per sink instance (applicable only to the process and Docker runtimes)")
        protected Long ram;
        @Option(names = "--disk", description = "The disk (in bytes) that need to be allocated "
                + "per sink instance (applicable only to Docker runtime)")
        protected Long disk;
        @Option(names = "--sinkConfig", description = "User defined configs key/values", hidden = true)
        protected String deprecatedSinkConfigString;
        @Option(names = "--sink-config", description = "User defined configs key/values")
        protected String sinkConfigString;
        @Option(names = "--auto-ack",
                description = "Whether or not the framework will automatically acknowledge messages", arity = "1")
        protected Boolean autoAck;
        @Option(names = "--timeout-ms", description = "The message timeout in milliseconds")
        protected Long timeoutMs;
        @Option(names = "--negative-ack-redelivery-delay-ms",
                description = "The negative ack message redelivery delay in milliseconds")
        protected Long negativeAckRedeliveryDelayMs;
        @Option(names = "--custom-runtime-options", description = "A string that encodes options to "
                + "customize the runtime, see docs for configured runtime for details")
        protected String customRuntimeOptions;
        @Option(names = "--secrets", description = "The map of secretName to an object that encapsulates "
                + "how the secret is fetched by the underlying secrets provider")
        protected String secretsString;
        @Option(names = "--transform-function", description = "Transform function applied before the Sink")
        protected String transformFunction;
        @Option(names = "--transform-function-classname", description = "The transform function class name")
        protected String transformFunctionClassName;
        @Option(names = "--transform-function-config", description = "Configuration of the transform function "
                + "applied before the Sink")
        protected String transformFunctionConfig;
        @Option(names = "--log-topic", description = "The topic to which the logs of a Pulsar Sink are produced")
        protected String logTopic;
        @Option(names = "--runtime-flags", description = "Any flags that you want to pass to a runtime"
                + " (for process & Kubernetes runtime only).")
        protected String runtimeFlags;

        protected SinkConfig sinkConfig;

        private void mergeArgs() {
            if (isBlank(subsName) && !isBlank(deprecatedSubsName)) {
                subsName = deprecatedSubsName;
            }
            if (isBlank(topicsPattern) && !isBlank(deprecatedTopicsPattern)) {
                topicsPattern = deprecatedTopicsPattern;
            }
            if (isBlank(customSerdeInputString) && !isBlank(deprecatedCustomSerdeInputString)) {
                customSerdeInputString = deprecatedCustomSerdeInputString;
            }
            if (processingGuarantees == null && deprecatedProcessingGuarantees != null) {
                processingGuarantees = deprecatedProcessingGuarantees;
            }
            if (retainOrdering == null && deprecatedRetainOrdering != null) {
                retainOrdering = deprecatedRetainOrdering;
            }
            if (isBlank(className) && !isBlank(deprecatedClassName)) {
                className = deprecatedClassName;
            }
            if (isBlank(sinkConfigFile) && !isBlank(deprecatedSinkConfigFile)) {
                sinkConfigFile = deprecatedSinkConfigFile;
            }
            if (isBlank(sinkConfigString) && !isBlank(deprecatedSinkConfigString)) {
                sinkConfigString = deprecatedSinkConfigString;
            }
        }

        @Override
        void processArguments() throws Exception {
            super.processArguments();
            // merge deprecated args with new args
            mergeArgs();

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

            if (null != className) {
                sinkConfig.setClassName(className);
            }

            if (null != name) {
                sinkConfig.setName(name);
            }
            if (null != processingGuarantees) {
                sinkConfig.setProcessingGuarantees(processingGuarantees);
            }

            if (null != cleanupSubscription) {
                sinkConfig.setCleanupSubscription(cleanupSubscription);
            }

            if (retainOrdering != null) {
                sinkConfig.setRetainOrdering(retainOrdering);
            }

            if (retainKeyOrdering != null) {
                sinkConfig.setRetainKeyOrdering(retainKeyOrdering);
            }

            if (null != inputs) {
                sinkConfig.setInputs(Arrays.asList(inputs.split(",")));
            }
            if (null != customSerdeInputString) {
                Type type = new TypeToken<Map<String, String>>(){}.getType();
                Map<String, String> customSerdeInputMap = new Gson().fromJson(customSerdeInputString, type);
                sinkConfig.setTopicToSerdeClassName(customSerdeInputMap);
            }

            if (null != customSchemaInputString) {
                Type type = new TypeToken<Map<String, String>>(){}.getType();
                Map<String, String> customSchemaInputMap = new Gson().fromJson(customSchemaInputString, type);
                sinkConfig.setTopicToSchemaType(customSchemaInputMap);
            }

            if (null != inputSpecs) {
                Type type = new TypeToken<Map<String, ConsumerConfig>>(){}.getType();
                sinkConfig.setInputSpecs(new Gson().fromJson(inputSpecs, type));
            }

            sinkConfig.setMaxMessageRetries(maxMessageRetries);
            if (null != deadLetterTopic) {
                sinkConfig.setDeadLetterTopic(deadLetterTopic);
            }

            if (isNotBlank(subsName)) {
                sinkConfig.setSourceSubscriptionName(subsName);
            }

            if (null != subsPosition) {
                sinkConfig.setSourceSubscriptionPosition(subsPosition);
            }

            if (null != topicsPattern) {
                sinkConfig.setTopicsPattern(topicsPattern);
            }

            if (parallelism != null) {
                sinkConfig.setParallelism(parallelism);
            }

            if (archive != null && (sinkType != null || sinkConfig.getSinkType() != null)) {
                throw new ParameterException("Cannot specify both archive and sink-type");
            }

            if (null != archive) {
                sinkConfig.setArchive(archive);
            }

            if (sinkType != null) {
                sinkConfig.setArchive(validateSinkType(sinkType));
            } else if (sinkConfig.getSinkType() != null) {
                sinkConfig.setArchive(validateSinkType(sinkConfig.getSinkType()));
            }

            Resources resources = sinkConfig.getResources();
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
                sinkConfig.setResources(resources);
            }

            try {
                if (null != sinkConfigString) {
                    sinkConfig.setConfigs(parseConfigs(sinkConfigString));
                }
            } catch (Exception ex) {
                throw new IllegalArgumentException("Cannot parse sink-config", ex);
            }

            if (autoAck != null) {
                sinkConfig.setAutoAck(autoAck);
            }
            if (timeoutMs != null) {
                sinkConfig.setTimeoutMs(timeoutMs);
            }
            if (negativeAckRedeliveryDelayMs != null && negativeAckRedeliveryDelayMs > 0) {
                sinkConfig.setNegativeAckRedeliveryDelayMs(negativeAckRedeliveryDelayMs);
            }

            if (customRuntimeOptions != null) {
                sinkConfig.setCustomRuntimeOptions(customRuntimeOptions);
            }

            if (secretsString != null) {
                Type type = new TypeToken<Map<String, Object>>() {}.getType();
                Map<String, Object> secretsMap = new Gson().fromJson(secretsString, type);
                if (secretsMap == null) {
                    secretsMap = Collections.emptyMap();
                }
                sinkConfig.setSecrets(secretsMap);
            }

            if (transformFunction != null) {
                sinkConfig.setTransformFunction(transformFunction);
            }

            if (transformFunctionClassName != null) {
                sinkConfig.setTransformFunctionClassName(transformFunctionClassName);
            }

            if (transformFunctionConfig != null) {
                sinkConfig.setTransformFunctionConfig(transformFunctionConfig);
            }
            if (null != logTopic) {
                sinkConfig.setLogTopic(logTopic);
            }
            if (null != runtimeFlags) {
                sinkConfig.setRuntimeFlags(runtimeFlags);
            }

            // check if configs are valid
            validateSinkConfigs(sinkConfig);
        }

        protected Map<String, Object> parseConfigs(String str) throws JsonProcessingException {
            ObjectMapper mapper = ObjectMapperFactory.getMapper().getObjectMapper();
            TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {};

            return mapper.readValue(str, typeRef);
        }

        protected void validateSinkConfigs(SinkConfig sinkConfig) {

            if (isBlank(sinkConfig.getArchive())) {
                throw new ParameterException("Sink archive not specified");
            }

            org.apache.pulsar.common.functions.Utils.inferMissingArguments(sinkConfig);

            if (!Utils.isFunctionPackageUrlSupported(sinkConfig.getArchive())
                    && !sinkConfig.getArchive().startsWith(Utils.BUILTIN)) {
                if (!new File(sinkConfig.getArchive()).exists()) {
                    throw new IllegalArgumentException(String.format("Sink Archive file %s does not exist",
                            sinkConfig.getArchive()));
                }
            }
        }

        protected String validateSinkType(String sinkType) throws IOException {
            Set<String> availableSinks;
            try {
                availableSinks = getAdmin().sinks().getBuiltInSinks().stream()
                        .map(ConnectorDefinition::getName).collect(Collectors.toSet());
            } catch (PulsarAdminException e) {
                throw new IOException(e);
            }

            if (!availableSinks.contains(sinkType)) {
                throw new ParameterException(
                        "Invalid sink type '" + sinkType + "' -- Available sinks are: " + availableSinks);
            }

            // Source type is a valid built-in connector type
            return "builtin://" + sinkType;
        }
    }

    /**
     * Sink level command.
     */
    @Getter
    abstract class SinkCommand extends BaseCommand {
        @Option(names = "--tenant", description = "The sink's tenant")
        protected String tenant;

        @Option(names = "--namespace", description = "The sink's namespace")
        protected String namespace;

        @Option(names = "--name", description = "The sink's name")
        protected String sinkName;

        @Override
        void processArguments() throws Exception {
            super.processArguments();
            if (tenant == null) {
                tenant = PUBLIC_TENANT;
            }
            if (namespace == null) {
                namespace = DEFAULT_NAMESPACE;
            }
            if (null == sinkName) {
                throw new RuntimeException(
                        "You must specify a name for the sink");
            }
        }
    }

    @Command(description = "Stops a Pulsar IO sink connector")
    protected class DeleteSink extends SinkCommand {

        @Override
        void runCmd() throws Exception {
            getAdmin().sinks().deleteSink(tenant, namespace, sinkName);
            print("Deleted successfully");
        }
    }

    @Command(description = "Gets the information about a Pulsar IO sink connector")
    protected class GetSink extends SinkCommand {

        @Override
        void runCmd() throws Exception {
            SinkConfig sinkConfig = getAdmin().sinks().getSink(tenant, namespace, sinkName);
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            System.out.println(gson.toJson(sinkConfig));
        }
    }

    /**
     * List Sources command.
     */
    @Command(description = "List all running Pulsar IO sink connectors")
    protected class ListSinks extends BaseCommand {
        @Option(names = "--tenant", description = "The sink's tenant")
        protected String tenant;

        @Option(names = "--namespace", description = "The sink's namespace")
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
            List<String> sinks = getAdmin().sinks().listSinks(tenant, namespace);
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            System.out.println(gson.toJson(sinks));
        }
    }

    @Command(description = "Check the current status of a Pulsar Sink")
    class GetSinkStatus extends SinkCommand {

        @Option(names = "--instance-id",
                description = "The sink instanceId (Get-status of all instances if instance-id is not provided")
        protected String instanceId;

        @Override
        void runCmd() throws Exception {
            if (isBlank(instanceId)) {
                print(getAdmin().sinks().getSinkStatus(tenant, namespace, sinkName));
            } else {
                print(getAdmin().sinks().getSinkStatus(tenant, namespace, sinkName, Integer.parseInt(instanceId)));
            }
        }
    }

    @Command(description = "Restart sink instance")
    class RestartSink extends SinkCommand {

        @Option(names = "--instance-id",
                description = "The sink instanceId (restart all instances if instance-id is not provided")
        protected String instanceId;

        @Override
        void runCmd() throws Exception {
            if (isNotBlank(instanceId)) {
                try {
                    getAdmin().sinks().restartSink(tenant, namespace, sinkName, Integer.parseInt(instanceId));
                } catch (NumberFormatException e) {
                    System.err.println("instance-id must be a number");
                }
            } else {
                getAdmin().sinks().restartSink(tenant, namespace, sinkName);
            }
            System.out.println("Restarted successfully");
        }
    }

    @Command(description = "Stops sink instance")
    class StopSink extends SinkCommand {

        @Option(names = "--instance-id",
                description = "The sink instanceId (stop all instances if instance-id is not provided")
        protected String instanceId;

        @Override
        void runCmd() throws Exception {
            if (isNotBlank(instanceId)) {
                try {
                    getAdmin().sinks().stopSink(tenant, namespace, sinkName, Integer.parseInt(instanceId));
                } catch (NumberFormatException e) {
                    System.err.println("instance-id must be a number");
                }
            } else {
                getAdmin().sinks().stopSink(tenant, namespace, sinkName);
            }
            System.out.println("Stopped successfully");
        }
    }

    @Command(description = "Starts sink instance")
    class StartSink extends SinkCommand {

        @Option(names = "--instance-id",
                description = "The sink instanceId (start all instances if instance-id is not provided")
        protected String instanceId;

        @Override
        void runCmd() throws Exception {
            if (isNotBlank(instanceId)) {
                try {
                    getAdmin().sinks().startSink(tenant, namespace, sinkName, Integer.parseInt(instanceId));
                } catch (NumberFormatException e) {
                    System.err.println("instance-id must be a number");
                }
            } else {
                getAdmin().sinks().startSink(tenant, namespace, sinkName);
            }
            System.out.println("Started successfully");
        }
    }

    @Command(description = "Get the list of Pulsar IO connector sinks supported by Pulsar cluster")
    public class ListBuiltInSinks extends BaseCommand {
        @Override
        void runCmd() throws Exception {
            getAdmin().sinks().getBuiltInSinks().stream().filter(x -> isNotBlank(x.getSinkClass()))
                    .forEach(connector -> {
                        System.out.println(connector.getName());
                        System.out.println(WordUtils.wrap(connector.getDescription(), 80));
                        System.out.println("----------------------------------------");
                    });
        }
    }

    @Command(description = "Reload the available built-in connectors")
    public class ReloadBuiltInSinks extends BaseCommand {

        @Override
        void runCmd() throws Exception {
            getAdmin().sinks().reloadBuiltInSinks();
        }
    }
}
