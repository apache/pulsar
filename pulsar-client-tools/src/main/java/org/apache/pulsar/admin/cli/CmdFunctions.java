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

import static org.apache.commons.lang.StringUtils.isBlank;
import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.apache.pulsar.common.naming.TopicName.DEFAULT_NAMESPACE;
import static org.apache.pulsar.common.naming.TopicName.PUBLIC_TENANT;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.StringConverter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.admin.cli.utils.CmdUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.functions.ConsumerConfig;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.FunctionState;
import org.apache.pulsar.common.functions.ProducerConfig;
import org.apache.pulsar.common.functions.Resources;
import org.apache.pulsar.common.functions.UpdateOptionsImpl;
import org.apache.pulsar.common.functions.Utils;
import org.apache.pulsar.common.functions.WindowConfig;
import org.apache.pulsar.common.util.ObjectMapperFactory;

@Slf4j
@Parameters(commandDescription = "Interface for managing Pulsar Functions "
        + "(lightweight, Lambda-style compute processes that work with Pulsar)")
public class CmdFunctions extends CmdBase {
    private final LocalRunner localRunner;
    private final CreateFunction creater;
    private final DeleteFunction deleter;
    private final UpdateFunction updater;
    private final GetFunction getter;
    private final GetFunctionStatus functionStatus;
    @Getter
    private final GetFunctionStats functionStats;
    private final RestartFunction restart;
    private final StopFunction stop;
    private final StartFunction start;
    private final ListFunctions lister;
    private final StateGetter stateGetter;
    private final StatePutter statePutter;
    private final TriggerFunction triggerer;
    private final UploadFunction uploader;
    private final DownloadFunction downloader;

    /**
     * Base command.
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

        void processArguments() throws Exception {}

        abstract void runCmd() throws Exception;
    }

    /**
     * Namespace level command.
     */
    @Getter
    abstract class NamespaceCommand extends BaseCommand {
        @Parameter(names = "--tenant", description = "The tenant of a Pulsar Function")
        protected String tenant;

        @Parameter(names = "--namespace", description = "The namespace of a Pulsar Function")
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
    }

    /**
     * Function level command.
     */
    @Getter
    abstract class FunctionCommand extends BaseCommand {
        @Parameter(names = "--fqfn", description = "The Fully Qualified Function Name (FQFN) for the function")
        protected String fqfn;

        @Parameter(names = "--tenant", description = "The tenant of a Pulsar Function")
        protected String tenant;

        @Parameter(names = "--namespace", description = "The namespace of a Pulsar Function")
        protected String namespace;

        @Parameter(names = "--name", description = "The name of a Pulsar Function")
        protected String functionName;

        @Override
        void processArguments() throws Exception {
            super.processArguments();

            boolean usesSetters = (null != tenant || null != namespace || null != functionName);
            boolean usesFqfn = (null != fqfn);

            // Throw an exception if --fqfn is set alongside any combination of --tenant, --namespace, and --name
            if (usesFqfn && usesSetters) {
                throw new RuntimeException("You must specify either a Fully Qualified Function Name (FQFN) "
                        + "or tenant, namespace, and function name");
            } else if (usesFqfn) {
                // If the --fqfn flag is used, parse tenant, namespace, and name using that flag
                String[] fqfnParts = fqfn.split("/");
                if (fqfnParts.length != 3) {
                    throw new RuntimeException(
                            "Fully qualified function names (FQFNs) must be of the form tenant/namespace/name");
                }
                tenant = fqfnParts[0];
                namespace = fqfnParts[1];
                functionName = fqfnParts[2];
            } else {
                if (tenant == null) {
                    tenant = PUBLIC_TENANT;
                }
                if (namespace == null) {
                    namespace = DEFAULT_NAMESPACE;
                }
                if (null == functionName) {
                    throw new RuntimeException(
                            "You must specify a name for the function or a Fully Qualified Function Name (FQFN)");
                }
            }
        }
    }

    /**
     * Commands that require a function config.
     */
    @Getter
    abstract class FunctionDetailsCommand extends BaseCommand {
        @Parameter(names = "--fqfn", description = "The Fully Qualified Function Name (FQFN) for the function")
        protected String fqfn;
        @Parameter(names = "--tenant", description = "The tenant of a Pulsar Function")
        protected String tenant;
        @Parameter(names = "--namespace", description = "The namespace of a Pulsar Function")
        protected String namespace;
        @Parameter(names = "--name", description = "The name of a Pulsar Function")
        protected String functionName;
        // for backwards compatibility purposes
        @Parameter(names = "--className", description = "The class name of a Pulsar Function", hidden = true)
        protected String deprecatedClassName;
        @Parameter(names = "--classname", description = "The class name of a Pulsar Function")
        protected String className;
        @Parameter(names = "--jar", description = "Path to the JAR file for the function "
                + "(if the function is written in Java). It also supports URL path [http/https/file "
                + "(file protocol assumes that file already exists on worker host)/function "
                + "(package URL from packages management service)] from which worker can download the package.",
                listConverter = StringConverter.class)
        protected String jarFile;
        @Parameter(names = "--py", description = "Path to the main Python file/Python Wheel file for the function "
                + "(if the function is written in Python). It also supports URL path [http/https/file "
                + "(file protocol assumes that file already exists on worker host)/function "
                + "(package URL from packages management service)] from which worker can download the package.",
                listConverter = StringConverter.class)
        protected String pyFile;
        @Parameter(names = "--go", description = "Path to the main Go executable binary for the function "
                + "(if the function is written in Go). It also supports URL path [http/https/file "
                + "(file protocol assumes that file already exists on worker host)/function "
                + "(package URL from packages management service)] from which worker can download the package.")
        protected String goFile;
        @Parameter(names = {"-i", "--inputs"}, description = "The input topic or "
                + "topics (multiple topics can be specified as a comma-separated list) of a Pulsar Function")
        protected String inputs;
        // for backwards compatibility purposes
        @Parameter(names = "--topicsPattern", description = "TopicsPattern to consume from list of topics "
                + "under a namespace that match the pattern. [--input] and [--topic-pattern] are mutually exclusive. "
                + "Add SerDe class name for a pattern in --custom-serde-inputs (supported for java fun only)",
                hidden = true)
        protected String deprecatedTopicsPattern;
        @Parameter(names = "--topics-pattern", description = "The topic pattern to consume from list of topics "
                + "under a namespace that match the pattern. [--input] and [--topic-pattern] are mutually exclusive. "
                + "Add SerDe class name for a pattern in --custom-serde-inputs (supported for java fun only)")
        protected String topicsPattern;

        @Parameter(names = {"-o", "--output"},
                description = "The output topic of a Pulsar Function (If none is specified, no output is written)")
        protected String output;
        @Parameter(names = "--producer-config", description = "The custom producer configuration (as a JSON string)")
        protected String producerConfig;
        // for backwards compatibility purposes
        @Parameter(names = "--logTopic",
                description = "The topic to which the logs of a Pulsar Function are produced", hidden = true)
        protected String deprecatedLogTopic;
        @Parameter(names = "--log-topic", description = "The topic to which the logs of a Pulsar Function are produced")
        protected String logTopic;

        @Parameter(names = {"-st", "--schema-type"}, description = "The builtin schema type or "
                + "custom schema class name to be used for messages output by the function")
        protected String schemaType = "";

        // for backwards compatibility purposes
        @Parameter(names = "--customSerdeInputs",
                description = "The map of input topics to SerDe class names (as a JSON string)", hidden = true)
        protected String deprecatedCustomSerdeInputString;
        @Parameter(names = "--custom-serde-inputs",
                description = "The map of input topics to SerDe class names (as a JSON string)")
        protected String customSerdeInputString;
        @Parameter(names = "--custom-schema-inputs",
                description = "The map of input topics to Schema properties (as a JSON string)")
        protected String customSchemaInputString;
        @Parameter(names = "--custom-schema-outputs",
                description = "The map of input topics to Schema properties (as a JSON string)")
        protected String customSchemaOutputString;
        @Parameter(names = "--input-specs",
                description = "The map of inputs to custom configuration (as a JSON string)")
        protected String inputSpecs;
        // for backwards compatibility purposes
        @Parameter(names = "--outputSerdeClassName",
                description = "The SerDe class to be used for messages output by the function", hidden = true)
        protected String deprecatedOutputSerdeClassName;
        @Parameter(names = "--output-serde-classname",
                description = "The SerDe class to be used for messages output by the function")
        protected String outputSerdeClassName;
        // for backwards compatibility purposes
        @Parameter(names = "--functionConfigFile", description = "The path to a YAML config file that specifies "
                + "the configuration of a Pulsar Function", hidden = true)
        protected String deprecatedFnConfigFile;
        @Parameter(names = "--function-config-file",
                description = "The path to a YAML config file that specifies the configuration of a Pulsar Function")
        protected String fnConfigFile;
        // for backwards compatibility purposes
        @Parameter(names = "--processingGuarantees", description = "The processing guarantees (aka delivery semantics) "
                + "applied to the function", hidden = true)
        protected FunctionConfig.ProcessingGuarantees deprecatedProcessingGuarantees;
        @Parameter(names = "--processing-guarantees",
                description = "The processing guarantees (aka delivery semantics) applied to the function")
        protected FunctionConfig.ProcessingGuarantees processingGuarantees;
        // for backwards compatibility purposes
        @Parameter(names = "--userConfig", description = "User-defined config key/values", hidden = true)
        protected String deprecatedUserConfigString;
        @Parameter(names = "--user-config", description = "User-defined config key/values")
        protected String userConfigString;
        @Parameter(names = "--retainOrdering",
                description = "Function consumes and processes messages in order", hidden = true)
        protected Boolean deprecatedRetainOrdering;
        @Parameter(names = "--retain-ordering", description = "Function consumes and processes messages in order")
        protected Boolean retainOrdering;
        @Parameter(names = "--retain-key-ordering",
                description = "Function consumes and processes messages in key order")
        protected Boolean retainKeyOrdering;
        @Parameter(names = "--batch-builder", description = "BatcherBuilder provides two types of "
                + "batch construction methods, DEFAULT and KEY_BASED. The default value is: DEFAULT")
        protected String batchBuilder;
        @Parameter(names = "--forward-source-message-property", description = "Forwarding input message's properties "
                + "to output topic when processing (use false to disable it)", arity = 1)
        protected Boolean forwardSourceMessageProperty = true;
        @Parameter(names = "--subs-name", description = "Pulsar source subscription name if user wants a specific "
                + "subscription-name for input-topic consumer")
        protected String subsName;
        @Parameter(names = "--subs-position", description = "Pulsar source subscription position if user wants to "
                + "consume messages from the specified location")
        protected SubscriptionInitialPosition subsPosition;
        @Parameter(names = "--parallelism", description = "The parallelism factor of a Pulsar Function "
                + "(i.e. the number of function instances to run)")
        protected Integer parallelism;
        @Parameter(names = "--cpu", description = "The cpu in cores that need to be allocated "
                + "per function instance(applicable only to docker runtime)")
        protected Double cpu;
        @Parameter(names = "--ram", description = "The ram in bytes that need to be allocated "
                + "per function instance(applicable only to process/docker runtime)")
        protected Long ram;
        @Parameter(names = "--disk", description = "The disk in bytes that need to be allocated "
                + "per function instance(applicable only to docker runtime)")
        protected Long disk;
        // for backwards compatibility purposes
        @Parameter(names = "--windowLengthCount", description = "The number of messages per window", hidden = true)
        protected Integer deprecatedWindowLengthCount;
        @Parameter(names = "--window-length-count", description = "The number of messages per window")
        protected Integer windowLengthCount;
        // for backwards compatibility purposes
        @Parameter(names = "--windowLengthDurationMs",
                description = "The time duration of the window in milliseconds", hidden = true)
        protected Long deprecatedWindowLengthDurationMs;
        @Parameter(names = "--window-length-duration-ms",
                description = "The time duration of the window in milliseconds")
        protected Long windowLengthDurationMs;
        // for backwards compatibility purposes
        @Parameter(names = "--slidingIntervalCount",
                description = "The number of messages after which the window slides", hidden = true)
        protected Integer deprecatedSlidingIntervalCount;
        @Parameter(names = "--sliding-interval-count",
                description = "The number of messages after which the window slides")
        protected Integer slidingIntervalCount;
        // for backwards compatibility purposes
        @Parameter(names = "--slidingIntervalDurationMs",
                description = "The time duration after which the window slides", hidden = true)
        protected Long deprecatedSlidingIntervalDurationMs;
        @Parameter(names = "--sliding-interval-duration-ms",
                description = "The time duration after which the window slides")
        protected Long slidingIntervalDurationMs;
        // for backwards compatibility purposes
        @Parameter(names = "--autoAck",
                description = "Whether or not the framework acknowledges messages automatically", hidden = true)
        protected Boolean deprecatedAutoAck = null;
        @Parameter(names = "--auto-ack",
                description = "Whether or not the framework acknowledges messages automatically", arity = 1)
        protected Boolean autoAck;
        // for backwards compatibility purposes
        @Parameter(names = "--timeoutMs", description = "The message timeout in milliseconds", hidden = true)
        protected Long deprecatedTimeoutMs;
        @Parameter(names = "--timeout-ms", description = "The message timeout in milliseconds")
        protected Long timeoutMs;
        @Parameter(names = "--max-message-retries",
                description = "How many times should we try to process a message before giving up")
        protected Integer maxMessageRetries;
        @Parameter(names = "--custom-runtime-options", description = "A string that encodes options to "
                + "customize the runtime, see docs for configured runtime for details")
        protected String customRuntimeOptions;
        @Parameter(names = "--secrets", description = "The map of secretName to an object that encapsulates "
                + "how the secret is fetched by the underlying secrets provider")
        protected String secretsString;
        @Parameter(names = "--dead-letter-topic",
                description = "The topic where messages that are not processed successfully are sent to")
        protected String deadLetterTopic;
        protected FunctionConfig functionConfig;
        protected String userCodeFile;

        private void mergeArgs() {
            if (isBlank(className) && !isBlank(deprecatedClassName)) {
                className = deprecatedClassName;
            }
            if (isBlank(topicsPattern) && !isBlank(deprecatedTopicsPattern)) {
                topicsPattern = deprecatedTopicsPattern;
            }
            if (isBlank(logTopic) && !isBlank(deprecatedLogTopic)) {
                logTopic = deprecatedLogTopic;
            }
            if (isBlank(outputSerdeClassName) && !isBlank(deprecatedOutputSerdeClassName)) {
                outputSerdeClassName = deprecatedOutputSerdeClassName;
            }
            if (isBlank(customSerdeInputString) && !isBlank(deprecatedCustomSerdeInputString)) {
                customSerdeInputString = deprecatedCustomSerdeInputString;
            }

            if (isBlank(fnConfigFile) && !isBlank(deprecatedFnConfigFile)) {
                fnConfigFile = deprecatedFnConfigFile;
            }
            if (processingGuarantees == null && deprecatedProcessingGuarantees != null) {
                processingGuarantees = deprecatedProcessingGuarantees;
            }
            if (isBlank(userConfigString) && !isBlank(deprecatedUserConfigString)) {
                userConfigString = deprecatedUserConfigString;
            }
            if (retainOrdering == null && deprecatedRetainOrdering != null) {
                retainOrdering = deprecatedRetainOrdering;
            }
            if (windowLengthCount == null && deprecatedWindowLengthCount != null) {
                windowLengthCount = deprecatedWindowLengthCount;
            }
            if (windowLengthDurationMs == null && deprecatedWindowLengthDurationMs != null) {
                windowLengthDurationMs = deprecatedWindowLengthDurationMs;
            }
            if (slidingIntervalCount == null && deprecatedSlidingIntervalCount != null) {
                slidingIntervalCount = deprecatedSlidingIntervalCount;
            }
            if (slidingIntervalDurationMs == null && deprecatedSlidingIntervalDurationMs != null) {
                slidingIntervalDurationMs = deprecatedSlidingIntervalDurationMs;
            }
            if (autoAck == null && deprecatedAutoAck != null) {
                autoAck = deprecatedAutoAck;
            }
            if (timeoutMs == null && deprecatedTimeoutMs != null) {
                timeoutMs = deprecatedTimeoutMs;
            }
        }

        @Override
        void processArguments() throws Exception {
            super.processArguments();
            // merge deprecated args with new args
            mergeArgs();

            // Initialize config builder either from a supplied YAML config file or from scratch
            if (null != fnConfigFile) {
                functionConfig = CmdUtils.loadConfig(fnConfigFile, FunctionConfig.class);
            } else {
                functionConfig = new FunctionConfig();
            }

            if (null != fqfn) {
                parseFullyQualifiedFunctionName(fqfn, functionConfig);
            } else {
                if (null != tenant) {
                    functionConfig.setTenant(tenant);
                }
                if (null != namespace) {
                    functionConfig.setNamespace(namespace);
                }
                if (null != functionName) {
                    functionConfig.setName(functionName);
                }
            }

            if (null != inputs) {
                List<String> inputTopics = Arrays.asList(inputs.split(","));
                functionConfig.setInputs(inputTopics);
            }
            if (null != customSerdeInputString) {
                Type type = new TypeToken<Map<String, String>>() {}.getType();
                Map<String, String> customSerdeInputMap = new Gson().fromJson(customSerdeInputString, type);
                functionConfig.setCustomSerdeInputs(customSerdeInputMap);
            }
            if (null != customSchemaInputString) {
                Type type = new TypeToken<Map<String, String>>() {}.getType();
                Map<String, String> customschemaInputMap = new Gson().fromJson(customSchemaInputString, type);
                functionConfig.setCustomSchemaInputs(customschemaInputMap);
            }
            if (null != customSchemaOutputString) {
                Type type = new TypeToken<Map<String, String>>() {}.getType();
                Map<String, String> customSchemaOutputMap = new Gson().fromJson(customSchemaOutputString, type);
                functionConfig.setCustomSchemaOutputs(customSchemaOutputMap);
            }
            if (null != inputSpecs) {
                Type type = new TypeToken<Map<String, ConsumerConfig>>() {}.getType();
                functionConfig.setInputSpecs(new Gson().fromJson(inputSpecs, type));
            }
            if (null != topicsPattern) {
                functionConfig.setTopicsPattern(topicsPattern);
            }
            if (null != output) {
                functionConfig.setOutput(output);
            }
            if (null != producerConfig) {
                Type type = new TypeToken<ProducerConfig>() {}.getType();
                functionConfig.setProducerConfig(new Gson().fromJson(producerConfig, type));
            }
            if (null != logTopic) {
                functionConfig.setLogTopic(logTopic);
            }
            if (null != className) {
                functionConfig.setClassName(className);
            }
            if (null != outputSerdeClassName) {
                functionConfig.setOutputSerdeClassName(outputSerdeClassName);
            }

            if (null != schemaType) {
                functionConfig.setOutputSchemaType(schemaType);
            }
            if (null != processingGuarantees) {
                functionConfig.setProcessingGuarantees(processingGuarantees);
            }

            if (null != retainOrdering) {
                functionConfig.setRetainOrdering(retainOrdering);
            }

            if (null != retainKeyOrdering) {
                functionConfig.setRetainKeyOrdering(retainKeyOrdering);
            }

            if (isNotBlank(batchBuilder)) {
                functionConfig.setBatchBuilder(batchBuilder);
            }

            if (null != forwardSourceMessageProperty) {
                functionConfig.setForwardSourceMessageProperty(forwardSourceMessageProperty);
            }

            if (isNotBlank(subsName)) {
                functionConfig.setSubName(subsName);
            }

            if (null != subsPosition) {
                functionConfig.setSubscriptionPosition(subsPosition);
            }

            if (null != userConfigString) {
                Type type = new TypeToken<Map<String, String>>() {}.getType();
                Map<String, Object> userConfigMap = new Gson().fromJson(userConfigString, type);
                if (userConfigMap == null) {
                    userConfigMap = new HashMap<>();
                }
                functionConfig.setUserConfig(userConfigMap);
            }

            if (parallelism != null) {
                functionConfig.setParallelism(parallelism);
            }

            Resources resources = functionConfig.getResources();
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
                functionConfig.setResources(resources);
            }

            if (timeoutMs != null) {
                functionConfig.setTimeoutMs(timeoutMs);
            }

            if (customRuntimeOptions != null) {
                functionConfig.setCustomRuntimeOptions(customRuntimeOptions);
            }

            if (secretsString != null) {
                Type type = new TypeToken<Map<String, Object>>() {}.getType();
                Map<String, Object> secretsMap = new Gson().fromJson(secretsString, type);
                if (secretsMap == null) {
                    secretsMap = Collections.emptyMap();
                }
                functionConfig.setSecrets(secretsMap);
            }

            // window configs
            WindowConfig windowConfig = functionConfig.getWindowConfig();
            if (null != windowLengthCount) {
                if (windowConfig == null) {
                    windowConfig = new WindowConfig();
                }
                windowConfig.setWindowLengthCount(windowLengthCount);
            }
            if (null != windowLengthDurationMs) {
                if (windowConfig == null) {
                    windowConfig = new WindowConfig();
                }
                windowConfig.setWindowLengthDurationMs(windowLengthDurationMs);
            }
            if (null != slidingIntervalCount) {
                if (windowConfig == null) {
                    windowConfig = new WindowConfig();
                }
                windowConfig.setSlidingIntervalCount(slidingIntervalCount);
            }
            if (null != slidingIntervalDurationMs) {
                if (windowConfig == null) {
                    windowConfig = new WindowConfig();
                }
                windowConfig.setSlidingIntervalDurationMs(slidingIntervalDurationMs);
            }

            functionConfig.setWindowConfig(windowConfig);

            if (autoAck != null) {
                functionConfig.setAutoAck(autoAck);
            }

            if (null != maxMessageRetries) {
                functionConfig.setMaxMessageRetries(maxMessageRetries);
            }
            if (null != deadLetterTopic) {
                functionConfig.setDeadLetterTopic(deadLetterTopic);
            }

            if (null != jarFile) {
                functionConfig.setJar(jarFile);
            }

            if (null != pyFile) {
                functionConfig.setPy(pyFile);
            }

            if (null != goFile) {
                functionConfig.setGo(goFile);
            }

            if (functionConfig.getJar() != null) {
                userCodeFile = functionConfig.getJar();
            } else if (functionConfig.getPy() != null) {
                userCodeFile = functionConfig.getPy();
            } else if (functionConfig.getGo() != null) {
                userCodeFile = functionConfig.getGo();
            }

            // check if configs are valid
            validateFunctionConfigs(functionConfig);
        }

        protected void validateFunctionConfigs(FunctionConfig functionConfig) {
            // go doesn't need className
            if (functionConfig.getRuntime() == FunctionConfig.Runtime.PYTHON
                    || functionConfig.getRuntime() == FunctionConfig.Runtime.JAVA){
                if (StringUtils.isEmpty(functionConfig.getClassName())) {
                    throw new IllegalArgumentException("No Function Classname specified");
                }
            }
            if (StringUtils.isEmpty(functionConfig.getName())) {
                org.apache.pulsar.common.functions.Utils.inferMissingFunctionName(functionConfig);
            }
            if (StringUtils.isEmpty(functionConfig.getTenant())) {
                org.apache.pulsar.common.functions.Utils.inferMissingTenant(functionConfig);
            }
            if (StringUtils.isEmpty(functionConfig.getNamespace())) {
                org.apache.pulsar.common.functions.Utils.inferMissingNamespace(functionConfig);
            }

            if (isNotBlank(functionConfig.getJar()) && isNotBlank(functionConfig.getPy())
                    && isNotBlank(functionConfig.getGo())) {
                throw new ParameterException("Either a Java jar or a Python file or a Go executable binary needs to"
                        + " be specified for the function. Cannot specify both.");
            }

            if (isBlank(functionConfig.getJar()) && isBlank(functionConfig.getPy())
                    && isBlank(functionConfig.getGo())) {
                throw new ParameterException("Either a Java jar or a Python file or a Go executable binary needs to"
                        + " be specified for the function. Please specify one.");
            }

            if (!isBlank(functionConfig.getJar()) && !Utils.isFunctionPackageUrlSupported(functionConfig.getJar())
                    && !new File(functionConfig.getJar()).exists()) {
                throw new ParameterException("The specified jar file does not exist");
            }
            if (!isBlank(functionConfig.getPy()) && !Utils.isFunctionPackageUrlSupported(functionConfig.getPy())
                    && !new File(functionConfig.getPy()).exists()) {
                throw new ParameterException("The specified python file does not exist");
            }
            if (!isBlank(functionConfig.getGo()) && !Utils.isFunctionPackageUrlSupported(functionConfig.getGo())
                    && !new File(functionConfig.getGo()).exists()) {
                throw new ParameterException("The specified go executable binary does not exist");
            }
        }
    }

    @Parameters(commandDescription = "Run a Pulsar Function locally, rather than deploy to a Pulsar cluster)")
    class LocalRunner extends FunctionDetailsCommand {

        // TODO: this should become BookKeeper URL and it should be fetched from Pulsar client.
        // for backwards compatibility purposes
        @Parameter(names = "--stateStorageServiceUrl", description = "The URL for the state storage service "
                + "(the default is Apache BookKeeper)", hidden = true)
        protected String deprecatedStateStorageServiceUrl;
        @Parameter(names = "--state-storage-service-url", description = "The URL for the state storage service "
                + "(the default is Apache BookKeeper)")
        protected String stateStorageServiceUrl;
        // for backwards compatibility purposes
        @Parameter(names = "--brokerServiceUrl", description = "The URL for Pulsar broker", hidden = true)
        protected String deprecatedBrokerServiceUrl;
        @Parameter(names = "--broker-service-url", description = "The URL for Pulsar broker")
        protected String brokerServiceUrl;
        @Parameter(names = "--web-service-url", description = "The URL for Pulsar web service")
        protected String webServiceUrl = null;
        // for backwards compatibility purposes
        @Parameter(names = "--clientAuthPlugin", description = "Client authentication plugin using "
                + "which function-process can connect to broker", hidden = true)
        protected String deprecatedClientAuthPlugin;
        @Parameter(names = "--client-auth-plugin",
                description = "Client authentication plugin using which function-process can connect to broker")
        protected String clientAuthPlugin;
        // for backwards compatibility purposes
        @Parameter(names = "--clientAuthParams", description = "Client authentication param", hidden = true)
        protected String deprecatedClientAuthParams;
        @Parameter(names = "--client-auth-params", description = "Client authentication param")
        protected String clientAuthParams;
        // for backwards compatibility purposes
        @Parameter(names = "--use_tls", description = "Use tls connection", hidden = true)
        protected Boolean deprecatedUseTls = null;
        @Parameter(names = "--use-tls", description = "Use tls connection")
        protected boolean useTls;
        // for backwards compatibility purposes
        @Parameter(names = "--tls_allow_insecure", description = "Allow insecure tls connection", hidden = true)
        protected Boolean deprecatedTlsAllowInsecureConnection = null;
        @Parameter(names = "--tls-allow-insecure", description = "Allow insecure tls connection")
        protected boolean tlsAllowInsecureConnection;
        // for backwards compatibility purposes
        @Parameter(names = "--hostname_verification_enabled",
                description = "Enable hostname verification", hidden = true)
        protected Boolean deprecatedTlsHostNameVerificationEnabled = null;
        @Parameter(names = "--hostname-verification-enabled", description = "Enable hostname verification")
        protected boolean tlsHostNameVerificationEnabled;
        // for backwards compatibility purposes
        @Parameter(names = "--tls_trust_cert_path", description = "tls trust cert file path", hidden = true)
        protected String deprecatedTlsTrustCertFilePath;
        @Parameter(names = "--tls-trust-cert-path", description = "tls trust cert file path")
        protected String tlsTrustCertFilePath;
        // for backwards compatibility purposes
        @Parameter(names = "--instanceIdOffset", description = "Start the instanceIds from this offset", hidden = true)
        protected Integer deprecatedInstanceIdOffset = null;
        @Parameter(names = "--instance-id-offset", description = "Start the instanceIds from this offset")
        protected Integer instanceIdOffset = 0;
        @Parameter(names = "--runtime", description = "either THREAD or PROCESS. Only applies for Java functions")
        protected String runtime;
        @Parameter(names = "--secrets-provider-classname", description = "Whats the classname for secrets provider")
        protected String secretsProviderClassName;
        @Parameter(names = "--secrets-provider-config",
                description = "Config that needs to be passed to secrets provider")
        protected String secretsProviderConfig;
        @Parameter(names = "--metrics-port-start", description = "The starting port range for metrics server")
        protected String metricsPortStart;

        private void mergeArgs() {
            if (isBlank(stateStorageServiceUrl) && !isBlank(deprecatedStateStorageServiceUrl)) {
                stateStorageServiceUrl = deprecatedStateStorageServiceUrl;
            }
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
            if (instanceIdOffset == null && deprecatedInstanceIdOffset != null) {
                instanceIdOffset = deprecatedInstanceIdOffset;
            }
        }

        @Override
        void runCmd() throws Exception {
            // merge deprecated args with new args
            mergeArgs();
            List<String> localRunArgs = new LinkedList<>();
            localRunArgs.add(System.getenv("PULSAR_HOME") + "/bin/function-localrunner");
            localRunArgs.add("--functionConfig");
            localRunArgs.add(new Gson().toJson(functionConfig));
            for (Field field : this.getClass().getDeclaredFields()) {
                if (field.getName().startsWith("DEPRECATED")) {
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
            ProcessBuilder processBuilder = new ProcessBuilder(localRunArgs).inheritIO();
            Process process = processBuilder.start();
            process.waitFor();
        }
    }

    @Parameters(commandDescription = "Create a Pulsar Function in cluster mode (deploy it on a Pulsar cluster)")
    class CreateFunction extends FunctionDetailsCommand {
        @Override
        void runCmd() throws Exception {
            if (Utils.isFunctionPackageUrlSupported(functionConfig.getJar())) {
                getAdmin().functions().createFunctionWithUrl(functionConfig, functionConfig.getJar());
            } else if (Utils.isFunctionPackageUrlSupported(functionConfig.getPy())) {
                getAdmin().functions().createFunctionWithUrl(functionConfig, functionConfig.getPy());
            } else if (Utils.isFunctionPackageUrlSupported(functionConfig.getGo())) {
                getAdmin().functions().createFunctionWithUrl(functionConfig, functionConfig.getGo());
            } else {
                getAdmin().functions().createFunction(functionConfig, userCodeFile);
            }

            print("Created successfully");
        }
    }

    @Parameters(commandDescription = "Fetch information about a Pulsar Function")
    class GetFunction extends FunctionCommand {
        @Override
        void runCmd() throws Exception {
            FunctionConfig functionConfig = getAdmin().functions().getFunction(tenant, namespace, functionName);
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            System.out.println(gson.toJson(functionConfig));
        }
    }

    @Parameters(commandDescription = "Check the current status of a Pulsar Function")
    class GetFunctionStatus extends FunctionCommand {

        @Parameter(names = "--instance-id", description = "The function instanceId "
                + "(Get-status of all instances if instance-id is not provided)")
        protected String instanceId;

        @Override
        void runCmd() throws Exception {
            if (isBlank(instanceId)) {
                print(getAdmin().functions().getFunctionStatus(tenant, namespace, functionName));
            } else {
                print(getAdmin().functions()
                        .getFunctionStatus(tenant, namespace, functionName, Integer.parseInt(instanceId)));
            }
        }
    }

    @Parameters(commandDescription = "Get the current stats of a Pulsar Function")
    class GetFunctionStats extends FunctionCommand {

        @Parameter(names = "--instance-id", description = "The function instanceId "
                + "(Get-stats of all instances if instance-id is not provided)")
        protected String instanceId;

        @Override
        void runCmd() throws Exception {

            if (isBlank(instanceId)) {
                print(getAdmin().functions().getFunctionStats(tenant, namespace, functionName));
            } else {
               print(getAdmin().functions()
                       .getFunctionStats(tenant, namespace, functionName, Integer.parseInt(instanceId)));
            }
        }
    }

    @Parameters(commandDescription = "Restart function instance")
    class RestartFunction extends FunctionCommand {

        @Parameter(names = "--instance-id", description = "The function instanceId "
                + "(restart all instances if instance-id is not provided)")
        protected String instanceId;

        @Override
        void runCmd() throws Exception {
            if (isNotBlank(instanceId)) {
                try {
                    getAdmin().functions()
                            .restartFunction(tenant, namespace, functionName, Integer.parseInt(instanceId));
                } catch (NumberFormatException e) {
                    System.err.println("instance-id must be a number");
                }
            } else {
                getAdmin().functions().restartFunction(tenant, namespace, functionName);
            }
            System.out.println("Restarted successfully");
        }
    }

    @Parameters(commandDescription = "Stops function instance")
    class StopFunction extends FunctionCommand {

        @Parameter(names = "--instance-id", description = "The function instanceId "
                + "(stop all instances if instance-id is not provided)")
        protected String instanceId;

        @Override
        void runCmd() throws Exception {
            if (isNotBlank(instanceId)) {
                try {
                    getAdmin().functions().stopFunction(tenant, namespace, functionName, Integer.parseInt(instanceId));
                } catch (NumberFormatException e) {
                    System.err.println("instance-id must be a number");
                }
            } else {
                getAdmin().functions().stopFunction(tenant, namespace, functionName);
            }
            System.out.println("Stopped successfully");
        }
    }

    @Parameters(commandDescription = "Starts a stopped function instance")
    class StartFunction extends FunctionCommand {

        @Parameter(names = "--instance-id", description = "The function instanceId "
                + "(start all instances if instance-id is not provided)")
        protected String instanceId;

        @Override
        void runCmd() throws Exception {
            if (isNotBlank(instanceId)) {
                try {
                    getAdmin().functions().startFunction(tenant, namespace, functionName, Integer.parseInt(instanceId));
                } catch (NumberFormatException e) {
                    System.err.println("instance-id must be a number");
                }
            } else {
                getAdmin().functions().startFunction(tenant, namespace, functionName);
            }
            System.out.println("Started successfully");
        }
    }

    @Parameters(commandDescription = "Delete a Pulsar Function that is running on a Pulsar cluster")
    class DeleteFunction extends FunctionCommand {
        @Override
        void runCmd() throws Exception {
            getAdmin().functions().deleteFunction(tenant, namespace, functionName);
            print("Deleted successfully");
        }
    }

    @Parameters(commandDescription = "Update a Pulsar Function that has been deployed to a Pulsar cluster")
    class UpdateFunction extends FunctionDetailsCommand {

        @Parameter(names = "--update-auth-data", description = "Whether or not to update the auth data")
        protected boolean updateAuthData;

        @Override
        protected void validateFunctionConfigs(FunctionConfig functionConfig) {
            if (StringUtils.isEmpty(functionConfig.getClassName())) {
                if (StringUtils.isEmpty(functionConfig.getName())) {
                    throw new IllegalArgumentException("Function Name not provided");
                }
            } else if (StringUtils.isEmpty(functionConfig.getName())) {
                org.apache.pulsar.common.functions.Utils.inferMissingFunctionName(functionConfig);
            }
            if (StringUtils.isEmpty(functionConfig.getTenant())) {
                org.apache.pulsar.common.functions.Utils.inferMissingTenant(functionConfig);
            }
            if (StringUtils.isEmpty(functionConfig.getNamespace())) {
                org.apache.pulsar.common.functions.Utils.inferMissingNamespace(functionConfig);
            }
        }

        @Override
        void runCmd() throws Exception {

            UpdateOptionsImpl updateOptions = new UpdateOptionsImpl();
            updateOptions.setUpdateAuthData(updateAuthData);
            if (Utils.isFunctionPackageUrlSupported(functionConfig.getJar())) {
                getAdmin().functions().updateFunctionWithUrl(functionConfig, functionConfig.getJar(), updateOptions);
            } else if (Utils.isFunctionPackageUrlSupported(functionConfig.getPy())) {
                getAdmin().functions().updateFunctionWithUrl(functionConfig, functionConfig.getPy(), updateOptions);
            } else if (Utils.isFunctionPackageUrlSupported(functionConfig.getGo())) {
                getAdmin().functions().updateFunctionWithUrl(functionConfig, functionConfig.getGo(), updateOptions);
            } else {
                getAdmin().functions().updateFunction(functionConfig, userCodeFile, updateOptions);
            }
            print("Updated successfully");
        }
    }

    @Parameters(commandDescription = "List all Pulsar Functions running under a specific tenant and namespace")
    class ListFunctions extends NamespaceCommand {
        @Override
        void runCmd() throws Exception {
            print(getAdmin().functions().getFunctions(tenant, namespace));
        }
    }

    @Parameters(commandDescription = "Fetch the current state associated with a Pulsar Function")
    class StateGetter extends FunctionCommand {

        @Parameter(names = { "-k", "--key" }, description = "Key name of State")
        private String key = null;

        @Parameter(names = { "-w", "--watch" }, description = "Watch for changes in the value associated with a key "
                + "for a Pulsar Function")
        private boolean watch = false;

        @Override
        void runCmd() throws Exception {
            if (isBlank(key)) {
                throw new ParameterException("State key needs to be specified");
            }
            do {
                try {
                    FunctionState functionState = getAdmin().functions()
                                                       .getFunctionState(tenant, namespace, functionName, key);
                    Gson gson = new GsonBuilder().setPrettyPrinting().create();
                    System.out.println(gson.toJson(functionState));
                } catch (PulsarAdminException pae) {
                    if (pae.getStatusCode() == 404 && watch) {
                        System.err.println(pae.getMessage());
                    } else {
                        throw pae;
                    }
                }
                if (watch) {
                    Thread.sleep(1000);
                }
            } while (watch);
        }
    }

    @Parameters(commandDescription = "Put the state associated with a Pulsar Function")
    class StatePutter extends FunctionCommand {

        @Parameter(names = { "-s", "--state" }, description = "The FunctionState that needs to be put", required = true)
        private String state = null;

        @Override
        void runCmd() throws Exception {
            TypeReference<FunctionState> typeRef = new TypeReference<FunctionState>() {};
            FunctionState stateRepr = ObjectMapperFactory.getThreadLocal().readValue(state, typeRef);
            getAdmin().functions()
                    .putFunctionState(tenant, namespace, functionName, stateRepr);
        }
    }

    @Parameters(commandDescription = "Trigger the specified Pulsar Function with a supplied value")
    class TriggerFunction extends FunctionCommand {
        // for backward compatibility purposes
        @Parameter(names = "--triggerValue",
                description = "The value with which you want to trigger the function", hidden = true)
        protected String deprecatedTriggerValue;
        @Parameter(names = "--trigger-value", description = "The value with which you want to trigger the function")
        protected String triggerValue;
        // for backward compatibility purposes
        @Parameter(names = "--triggerFile", description = "The path to the file that contains the data with which "
                + "you want to trigger the function", hidden = true)
        protected String deprecatedTriggerFile;
        @Parameter(names = "--trigger-file", description = "The path to the file that contains the data with which "
                + "you want to trigger the function")
        protected String triggerFile;
        @Parameter(names = "--topic", description = "The specific topic name that the function consumes from that"
                + " you want to inject the data to")
        protected String topic;

        public void mergeArgs() {
            if (isBlank(triggerValue) && !isBlank(deprecatedTriggerValue)) {
                triggerValue = deprecatedTriggerValue;
            }
            if (isBlank(triggerFile) && !isBlank(deprecatedTriggerFile)) {
                triggerFile = deprecatedTriggerFile;
            }
        }

        @Override
        void runCmd() throws Exception {
            // merge deprecated args with new args
            mergeArgs();
            if (triggerFile == null && triggerValue == null) {
                throw new ParameterException("Either a trigger value or a trigger filepath needs to be specified");
            }
            String retval = getAdmin().functions()
                    .triggerFunction(tenant, namespace, functionName, topic, triggerValue, triggerFile);
            System.out.println(retval);
        }
    }

    @Parameters(commandDescription = "Upload File Data to Pulsar", hidden = true)
    class UploadFunction extends BaseCommand {
        // for backward compatibility purposes
        @Parameter(
                names = "--sourceFile",
                description = "The file whose contents need to be uploaded",
                listConverter = StringConverter.class, hidden = true)
        protected String deprecatedSourceFile;
        @Parameter(
                names = "--source-file",
                description = "The file whose contents need to be uploaded",
                listConverter = StringConverter.class)
        protected String sourceFile;
        @Parameter(
                names = "--path",
                description = "Path where the contents need to be stored",
                listConverter = StringConverter.class, required = true)
        protected String path;

        private void mergeArgs() {
            if (isBlank(sourceFile) && !isBlank(deprecatedSourceFile)) {
                sourceFile = deprecatedSourceFile;
            }
        }

        @Override
        void runCmd() throws Exception {
            // merge deprecated args with new args
            mergeArgs();
            if (StringUtils.isBlank(sourceFile)) {
                throw new ParameterException("--source-file needs to be specified");
            }
            getAdmin().functions().uploadFunction(sourceFile, path);
            print("Uploaded successfully");
        }
    }

    @Parameters(commandDescription = "Download File Data from Pulsar", hidden = true)
    class DownloadFunction extends FunctionCommand {
        // for backward compatibility purposes
        @Parameter(
                names = "--destinationFile",
                description = "The file to store downloaded content",
                listConverter = StringConverter.class, hidden = true)
        protected String deprecatedDestinationFile;
        @Parameter(
                names = "--destination-file",
                description = "The file to store downloaded content",
                listConverter = StringConverter.class)
        protected String destinationFile;
        @Parameter(
                names = "--path",
                description = "Path to store the content",
                listConverter = StringConverter.class, required = false, hidden = true)
        protected String path;

        private void mergeArgs() {
            if (isBlank(destinationFile) && !isBlank(deprecatedDestinationFile)) {
                destinationFile = deprecatedDestinationFile;
            }
        }

        @Override
        void processArguments() throws Exception {
            if (path == null) {
                super.processArguments();
            }
        }

        @Override
        void runCmd() throws Exception {
            // merge deprecated args with new args
            mergeArgs();
            if (StringUtils.isBlank(destinationFile)) {
                throw new ParameterException("--destination-file needs to be specified");
            }
            if (path != null) {
                getAdmin().functions().downloadFunction(destinationFile, path);
            } else {
                getAdmin().functions().downloadFunction(destinationFile, tenant, namespace, functionName);
            }
            print("Downloaded successfully");
        }
    }

    public CmdFunctions(Supplier<PulsarAdmin> admin) throws PulsarClientException {
        super("functions", admin);
        localRunner = new LocalRunner();
        creater = new CreateFunction();
        deleter = new DeleteFunction();
        updater = new UpdateFunction();
        getter = new GetFunction();
        functionStatus = new GetFunctionStatus();
        functionStats = new GetFunctionStats();
        lister = new ListFunctions();
        stateGetter = new StateGetter();
        statePutter = new StatePutter();
        triggerer = new TriggerFunction();
        uploader = new UploadFunction();
        downloader = new DownloadFunction();
        restart = new RestartFunction();
        stop = new StopFunction();
        start = new StartFunction();
        jcommander.addCommand("localrun", getLocalRunner());
        jcommander.addCommand("create", getCreater());
        jcommander.addCommand("delete", getDeleter());
        jcommander.addCommand("update", getUpdater());
        jcommander.addCommand("get", getGetter());
        jcommander.addCommand("restart", getRestarter());
        jcommander.addCommand("stop", getStopper());
        jcommander.addCommand("start", getStarter());
        // TODO depecreate getstatus
        jcommander.addCommand("status", getStatuser(), "getstatus");
        jcommander.addCommand("stats", getFunctionStats());
        jcommander.addCommand("list", getLister());
        jcommander.addCommand("querystate", getStateGetter());
        jcommander.addCommand("putstate", getStatePutter());
        jcommander.addCommand("trigger", getTriggerer());
        jcommander.addCommand("upload", getUploader());
        jcommander.addCommand("download", getDownloader());
    }

    @VisibleForTesting
    LocalRunner getLocalRunner() {
        return localRunner;
    }

    @VisibleForTesting
    CreateFunction getCreater() {
        return creater;
    }

    @VisibleForTesting
    DeleteFunction getDeleter() {
        return deleter;
    }

    @VisibleForTesting
    UpdateFunction getUpdater() {
        return updater;
    }

    @VisibleForTesting
    GetFunction getGetter() {
        return getter;
    }

    @VisibleForTesting
    GetFunctionStatus getStatuser() {
        return functionStatus;
    }

    @VisibleForTesting
    ListFunctions getLister() {
        return lister;
    }

    @VisibleForTesting
    StatePutter getStatePutter() {
        return statePutter;
    }

    @VisibleForTesting
    StateGetter getStateGetter() {
        return stateGetter;
    }

    @VisibleForTesting
    TriggerFunction getTriggerer() {
        return triggerer;
    }

    @VisibleForTesting
    UploadFunction getUploader() {
        return uploader;
    }

    @VisibleForTesting
    DownloadFunction getDownloader() {
        return downloader;
    }

    @VisibleForTesting
    RestartFunction getRestarter() {
        return restart;
    }

    @VisibleForTesting
    StopFunction getStopper() {
        return stop;
    }

    @VisibleForTesting
    StartFunction getStarter() {
        return start;
    }

    private void parseFullyQualifiedFunctionName(String fqfn, FunctionConfig functionConfig) {
        String[] args = fqfn.split("/");
        if (args.length != 3) {
            throw new ParameterException("Fully qualified function names (FQFNs) must "
                    + "be of the form tenant/namespace/name");
        } else {
            functionConfig.setTenant(args[0]);
            functionConfig.setNamespace(args[1]);
            functionConfig.setName(args[2]);
        }
    }

}
