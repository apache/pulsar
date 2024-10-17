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

import static org.apache.commons.lang.StringUtils.isBlank;
import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.apache.pulsar.common.naming.TopicName.DEFAULT_NAMESPACE;
import static org.apache.pulsar.common.naming.TopicName.PUBLIC_TENANT;
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
import org.apache.commons.text.WordUtils;
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
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Slf4j
@Command(description = "Interface for managing Pulsar Functions "
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
            processArguments();
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
        @Option(names = "--tenant", description = "The tenant of a Pulsar Function")
        protected String tenant;

        @Option(names = "--namespace", description = "The namespace of a Pulsar Function")
        protected String namespace;
    }

    /**
     * Function level command.
     */
    @Getter
    abstract class FunctionCommand extends BaseCommand {
        @Option(names = "--fqfn", description = "The Fully Qualified Function Name (FQFN) for the function")
        protected String fqfn;

        @Option(names = "--tenant", description = "The tenant of a Pulsar Function")
        protected String tenant;

        @Option(names = "--namespace", description = "The namespace of a Pulsar Function")
        protected String namespace;

        @Option(names = "--name", description = "The name of a Pulsar Function")
        protected String functionName;

        @Override
        void processArguments() throws Exception {
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
        @Option(names = "--fqfn", description = "The Fully Qualified Function Name (FQFN) for the function"
                + " #Java, Python")
        protected String fqfn;
        @Option(names = "--tenant", description = "The tenant of a Pulsar Function #Java, Python, Go")
        protected String tenant;
        @Option(names = "--namespace", description = "The namespace of a Pulsar Function #Java, Python, Go")
        protected String namespace;
        @Option(names = "--name", description = "The name of a Pulsar Function #Java, Python, Go")
        protected String functionName;
        // for backwards compatibility purposes
        @Option(names = "--className", description = "The class name of a Pulsar Function", hidden = true)
        protected String deprecatedClassName;
        @Option(names = "--classname", description = "The class name of a Pulsar Function #Java, Python")
        protected String className;
        @Option(names = { "-t", "--function-type" }, description = "The built-in Pulsar Function type")
        protected String functionType;
        @Option(names = "--cleanup-subscription", description = "Whether delete the subscription "
                + "when function is deleted")
        protected Boolean cleanupSubscription;
        @Option(names = "--jar", description = "Path to the JAR file for the function "
                + "(if the function is written in Java). It also supports URL path [http/https/file "
                + "(file protocol assumes that file already exists on worker host)/function "
                + "(package URL from packages management service)] from which worker can download the package. #Java")
        protected String jarFile;
        @Option(names = "--py", description = "Path to the main Python file/Python Wheel file for the function "
                + "(if the function is written in Python). It also supports URL path [http/https/file "
                + "(file protocol assumes that file already exists on worker host)/function "
                + "(package URL from packages management service)] from which worker can download the package. #Python")
        protected String pyFile;
        @Option(names = "--go", description = "Path to the main Go executable binary for the function "
                + "(if the function is written in Go). It also supports URL path [http/https/file "
                + "(file protocol assumes that file already exists on worker host)/function "
                + "(package URL from packages management service)] from which worker can download the package. #Go")
        protected String goFile;
        @Option(names = {"-i", "--inputs"}, description = "The input topic or "
                + "topics (multiple topics can be specified as a comma-separated list) of a Pulsar Function"
                + " #Java, Python, Go")
        protected String inputs;
        // for backwards compatibility purposes
        @Option(names = "--topicsPattern", description = "TopicsPattern to consume from list of topics "
                + "under a namespace that match the pattern. [--input] and [--topic-pattern] are mutually exclusive. "
                + "Add SerDe class name for a pattern in --custom-serde-inputs (supported for java fun only)",
                hidden = true)
        protected String deprecatedTopicsPattern;
        @Option(names = "--topics-pattern", description = "The topic pattern to consume from a list of topics "
                + "under a namespace that matches the pattern. [--input] and [--topics-pattern] are mutually "
                + "exclusive. Add SerDe class name for a pattern in --custom-serde-inputs (supported for java "
                + "functions only) #Java, Python")
        protected String topicsPattern;

        @Option(names = {"-o", "--output"},
                description = "The output topic of a Pulsar Function (If none is specified, no output is written)"
                        + " #Java, Python, Go")
        protected String output;
        @Option(names = "--producer-config", description = "The custom producer configuration (as a JSON string)"
                + " #Java")
        protected String producerConfig;
        // for backwards compatibility purposes
        @Option(names = "--logTopic",
                description = "The topic to which the logs of a Pulsar Function are produced", hidden = true)
        protected String deprecatedLogTopic;
        @Option(names = "--log-topic", description = "The topic to which the logs of a Pulsar Function are produced"
                + " #Java, Python, Go")
        protected String logTopic;

        @Option(names = {"-st", "--schema-type"}, description = "The builtin schema type or "
                + "custom schema class name to be used for messages output by the function #Java")
        protected String schemaType = "";

        // for backwards compatibility purposes
        @Option(names = "--customSerdeInputs",
                description = "The map of input topics to SerDe class names (as a JSON string)", hidden = true)
        protected String deprecatedCustomSerdeInputString;
        @Option(names = "--custom-serde-inputs",
                description = "The map of input topics to SerDe class names (as a JSON string) #Java, Python")
        protected String customSerdeInputString;
        @Option(names = "--custom-schema-inputs",
                description = "The map of input topics to Schema properties (as a JSON string) #Java, Python")
        protected String customSchemaInputString;
        @Option(names = "--custom-schema-outputs",
                description = "The map of input topics to Schema properties (as a JSON string) #Java")
        protected String customSchemaOutputString;
        @Option(names = "--input-specs",
                description = "The map of inputs to custom configuration (as a JSON string) #Java, Python, Go")
        protected String inputSpecs;
        @Option(names = "--input-type-class-name",
                description = "The class name of input type class #Java, Python, Go")
        protected String inputTypeClassName;
        // for backwards compatibility purposes
        @Option(names = "--outputSerdeClassName",
                description = "The SerDe class to be used for messages output by the function", hidden = true)
        protected String deprecatedOutputSerdeClassName;
        @Option(names = "--output-serde-classname",
                description = "The SerDe class to be used for messages output by the function #Java, Python")
        protected String outputSerdeClassName;
        @Option(names = "--output-type-class-name",
                description = "The class name of output type class #Java, Python, Go")
        protected String outputTypeClassName;
        // for backwards compatibility purposes
        @Option(names = "--functionConfigFile", description = "The path to a YAML config file that specifies "
                + "the configuration of a Pulsar Function", hidden = true)
        protected String deprecatedFnConfigFile;
        @Option(names = "--function-config-file",
                description = "The path to a YAML config file that specifies the configuration of a Pulsar Function"
                        + " #Java, Python, Go")
        protected String fnConfigFile;
        // for backwards compatibility purposes
        @Option(names = "--processingGuarantees", description = "The processing guarantees (aka delivery semantics) "
                + "applied to the function", hidden = true)
        protected FunctionConfig.ProcessingGuarantees deprecatedProcessingGuarantees;
        @Option(names = "--processing-guarantees",
                description = "The processing guarantees (as known as delivery semantics) applied to the function."
                    + " Available values are: `ATLEAST_ONCE`, `ATMOST_ONCE`, `EFFECTIVELY_ONCE`."
                    + " If it is not specified, the `ATLEAST_ONCE` delivery guarantee is used."
                    + " #Java, Python, Go")
        protected FunctionConfig.ProcessingGuarantees processingGuarantees;
        // for backwards compatibility purposes
        @Option(names = "--userConfig", description = "User-defined config key/values", hidden = true)
        protected String deprecatedUserConfigString;
        @Option(names = "--user-config", description = "User-defined config key/values #Java, Python, Go")
        protected String userConfigString;
        @Option(names = "--retainOrdering",
                description = "Function consumes and processes messages in order", hidden = true)
        protected Boolean deprecatedRetainOrdering;
        @Option(names = "--retain-ordering", description = "Function consumes and processes messages in order #Java")
        protected Boolean retainOrdering;
        @Option(names = "--retain-key-ordering",
                description = "Function consumes and processes messages in key order #Java")
        protected Boolean retainKeyOrdering;
        @Option(names = "--batch-builder", description = "BatcherBuilder provides two types of "
                + "batch construction methods, DEFAULT and KEY_BASED. The default value is: DEFAULT")
        protected String batchBuilder;
        @Option(names = "--forward-source-message-property", description = "Forwarding input message's properties "
                + "to output topic when processing (use false to disable it) #Java", arity = "1")
        protected Boolean forwardSourceMessageProperty = true;
        @Option(names = "--subs-name", description = "Pulsar source subscription name if user wants a specific "
                + "subscription-name for input-topic consumer #Java, Python, Go")
        protected String subsName;
        @Option(names = "--subs-position", description = "Pulsar source subscription position if user wants to "
                + "consume messages from the specified location #Java")
        protected SubscriptionInitialPosition subsPosition;
        @Option(names = "--skip-to-latest", description = "Whether or not the consumer skip to latest message "
            + "upon function instance restart", arity = "1")
        protected Boolean skipToLatest;
        @Option(names = "--parallelism", description = "The parallelism factor of a Pulsar Function "
                + "(i.e. the number of function instances to run) #Java")
        protected Integer parallelism;
        @Option(names = "--cpu", description = "The cpu in cores that need to be allocated "
                + "per function instance(applicable only to docker runtime) #Java(Process & K8s),Python(K8s),Go(K8s)")
        protected Double cpu;
        @Option(names = "--ram", description = "The ram in bytes that need to be allocated "
                + "per function instance(applicable only to process/docker runtime)"
                + " #Java(Process & K8s),Python(K8s),Go(K8s)")
        protected Long ram;
        @Option(names = "--disk", description = "The disk in bytes that need to be allocated "
                + "per function instance(applicable only to docker runtime) #Java(Process & K8s),Python(K8s),Go(K8s)")
        protected Long disk;
        // for backwards compatibility purposes
        @Option(names = "--windowLengthCount", description = "The number of messages per window", hidden = true)
        protected Integer deprecatedWindowLengthCount;
        @Option(names = "--window-length-count", description = "The number of messages per window #Java")
        protected Integer windowLengthCount;
        // for backwards compatibility purposes
        @Option(names = "--windowLengthDurationMs",
                description = "The time duration of the window in milliseconds", hidden = true)
        protected Long deprecatedWindowLengthDurationMs;
        @Option(names = "--window-length-duration-ms",
                description = "The time duration of the window in milliseconds #Java")
        protected Long windowLengthDurationMs;
        // for backwards compatibility purposes
        @Option(names = "--slidingIntervalCount",
                description = "The number of messages after which the window slides", hidden = true)
        protected Integer deprecatedSlidingIntervalCount;
        @Option(names = "--sliding-interval-count",
                description = "The number of messages after which the window slides #Java")
        protected Integer slidingIntervalCount;
        // for backwards compatibility purposes
        @Option(names = "--slidingIntervalDurationMs",
                description = "The time duration after which the window slides", hidden = true)
        protected Long deprecatedSlidingIntervalDurationMs;
        @Option(names = "--sliding-interval-duration-ms",
                description = "The time duration after which the window slides #Java")
        protected Long slidingIntervalDurationMs;
        // for backwards compatibility purposes
        @Option(names = "--autoAck",
                description = "Whether or not the framework acknowledges messages automatically", hidden = true)
        protected Boolean deprecatedAutoAck = null;
        @Option(names = "--auto-ack",
                description = "Whether or not the framework acknowledges messages automatically"
                        + " #Java, Python, Go", arity = "1")
        protected Boolean autoAck;
        // for backwards compatibility purposes
        @Option(names = "--timeoutMs", description = "The message timeout in milliseconds", hidden = true)
        protected Long deprecatedTimeoutMs;
        @Option(names = "--timeout-ms", description = "The message timeout in milliseconds #Java, Python")
        protected Long timeoutMs;
        @Option(names = "--max-message-retries",
                description = "How many times should we try to process a message before giving up #Java")
        protected Integer maxMessageRetries;
        @Option(names = "--custom-runtime-options", description = "A string that encodes options to "
                + "customize the runtime, see docs for configured runtime for details #Java")
        protected String customRuntimeOptions;
        @Option(names = "--secrets", description = "The map of secretName to an object that encapsulates "
                + "how the secret is fetched by the underlying secrets provider #Java, Python")
        protected String secretsString;
        @Option(names = "--dead-letter-topic",
                description = "The topic where messages that are not processed successfully are sent to #Java")
        protected String deadLetterTopic;
        @Option(names = "--runtime-flags", description = "Any flags that you want to pass to a runtime"
                + " (for process & Kubernetes runtime only).")
        protected String runtimeFlags;
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
            // merge deprecated args with new args
            mergeArgs();

            // Initialize config builder either from a supplied YAML config file or from scratch
            if (null != fnConfigFile) {
                functionConfig = CmdUtils.loadConfig(fnConfigFile, FunctionConfig.class);
            } else {
                functionConfig = new FunctionConfig();
            }

            if (null != fqfn) {
                String[] args = fqfn.split("/");
                if (args.length != 3) {
                    throw new ParameterException("Fully qualified function names (FQFNs) must "
                            + "be of the form tenant/namespace/name");
                } else {
                    functionConfig.setTenant(args[0]);
                    functionConfig.setNamespace(args[1]);
                    functionConfig.setName(args[2]);
                }
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

            if (null != cleanupSubscription) {
                functionConfig.setCleanupSubscription(cleanupSubscription);
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
            if (null != inputTypeClassName) {
                functionConfig.setInputTypeClassName(inputTypeClassName);
            }
            if (null != topicsPattern) {
                functionConfig.setTopicsPattern(topicsPattern);
            }
            if (null != output) {
                functionConfig.setOutput(output);
            }
            if (null != outputTypeClassName) {
                functionConfig.setOutputTypeClassName(outputTypeClassName);
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

            if (null != skipToLatest) {
                functionConfig.setSkipToLatest(skipToLatest);
            }

            if (null != userConfigString) {
                Type type = new TypeToken<Map<String, Object>>() {}.getType();
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

            if (jarFile != null && functionType != null) {
                throw new ParameterException("Cannot specify both jar and function-type");
            }

            if (null != jarFile) {
                functionConfig.setJar(jarFile);
            }

            if (functionType != null) {
                functionConfig.setJar("builtin://" + functionType);
            } else if (functionConfig.getFunctionType() != null) {
                functionConfig.setJar("builtin://" + functionConfig.getFunctionType());
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

            if (null != runtimeFlags) {
                functionConfig.setRuntimeFlags(runtimeFlags);
            }

            // check if configs are valid
            validateFunctionConfigs(functionConfig);
        }

        protected void validateFunctionConfigs(FunctionConfig functionConfig) {
            // go doesn't need className
            if (functionConfig.getPy() != null
                    || (functionConfig.getJar() != null && !functionConfig.getJar().startsWith("builtin://"))) {
                if (StringUtils.isEmpty(functionConfig.getClassName())) {
                    throw new ParameterException("No Function Classname specified");
                }
            }
            if (StringUtils.isEmpty(functionConfig.getName())) {
                org.apache.pulsar.common.functions.Utils.inferMissingFunctionName(functionConfig);
            }
            if (StringUtils.isEmpty(functionConfig.getName())) {
                throw new IllegalArgumentException("No Function name specified");
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

            if (!isBlank(functionConfig.getJar()) && !functionConfig.getJar().startsWith("builtin://")
                    && !Utils.isFunctionPackageUrlSupported(functionConfig.getJar())
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

    @Command(description = "Run a Pulsar Function locally, rather than deploy to a Pulsar cluster)")
    class LocalRunner extends FunctionDetailsCommand {

        // TODO: this should become BookKeeper URL and it should be fetched from Pulsar client.
        // for backwards compatibility purposes
        @Option(names = "--stateStorageServiceUrl", description = "The URL for the state storage service "
                + "(the default is Apache BookKeeper)", hidden = true)
        protected String deprecatedStateStorageServiceUrl;
        @Option(names = "--state-storage-service-url", description = "The URL for the state storage service "
                + "(the default is Apache BookKeeper) #Java, Python")
        protected String stateStorageServiceUrl;
        // for backwards compatibility purposes
        @Option(names = "--brokerServiceUrl", description = "The URL for Pulsar broker", hidden = true)
        protected String deprecatedBrokerServiceUrl;
        @Option(names = "--broker-service-url", description = "The URL for Pulsar broker #Java, Python, Go")
        protected String brokerServiceUrl;
        @Option(names = "--web-service-url", description = "The URL for Pulsar web service #Java, Python")
        protected String webServiceUrl = null;
        // for backwards compatibility purposes
        @Option(names = "--clientAuthPlugin", description = "Client authentication plugin using "
                + "which function-process can connect to broker", hidden = true)
        protected String deprecatedClientAuthPlugin;
        @Option(names = "--client-auth-plugin",
                description = "Client authentication plugin using which function-process can connect to broker"
                        + " #Java, Python")
        protected String clientAuthPlugin;
        // for backwards compatibility purposes
        @Option(names = "--clientAuthParams", description = "Client authentication param", hidden = true)
        protected String deprecatedClientAuthParams;
        @Option(names = "--client-auth-params", description = "Client authentication param #Java, Python")
        protected String clientAuthParams;
        // for backwards compatibility purposes
        @Option(names = "--use_tls", description = "Use tls connection", hidden = true)
        protected Boolean deprecatedUseTls = null;
        @Option(names = "--use-tls", description = "Use tls connection #Java, Python")
        protected boolean useTls;
        // for backwards compatibility purposes
        @Option(names = "--tls_allow_insecure", description = "Allow insecure tls connection", hidden = true)
        protected Boolean deprecatedTlsAllowInsecureConnection = null;
        @Option(names = "--tls-allow-insecure", description = "Allow insecure tls connection #Java, Python")
        protected boolean tlsAllowInsecureConnection;
        // for backwards compatibility purposes
        @Option(names = "--hostname_verification_enabled",
                description = "Enable hostname verification", hidden = true)
        protected Boolean deprecatedTlsHostNameVerificationEnabled = null;
        @Option(names = "--hostname-verification-enabled", description = "Enable hostname verification"
                + " #Java, Python")
        protected boolean tlsHostNameVerificationEnabled;
        // for backwards compatibility purposes
        @Option(names = "--tls_trust_cert_path", description = "tls trust cert file path", hidden = true)
        protected String deprecatedTlsTrustCertFilePath;
        @Option(names = "--tls-trust-cert-path", description = "tls trust cert file path #Java, Python")
        protected String tlsTrustCertFilePath;
        // for backwards compatibility purposes
        @Option(names = "--instanceIdOffset", description = "Start the instanceIds from this offset", hidden = true)
        protected Integer deprecatedInstanceIdOffset = null;
        @Option(names = "--instance-id-offset", description = "Start the instanceIds from this offset #Java, Python")
        protected Integer instanceIdOffset = 0;
        @Option(names = "--runtime", description = "either THREAD or PROCESS. Only applies for Java functions #Java")
        protected String runtime;
        @Option(names = "--secrets-provider-classname", description = "Whats the classname for secrets provider"
                + " #Java, Python")
        protected String secretsProviderClassName;
        @Option(names = "--secrets-provider-config",
                description = "Config that needs to be passed to secrets provider #Java, Python")
        protected String secretsProviderConfig;
        @Option(names = "--metrics-port-start", description = "The starting port range for metrics server"
                + " #Java, Python, Go")
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
            ProcessBuilder processBuilder = new ProcessBuilder(localRunArgs).inheritIO();
            Process process = processBuilder.start();
            process.waitFor();
        }
    }

    @Command(description = "Create a Pulsar Function in cluster mode (deploy it on a Pulsar cluster)")
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

    @Command(description = "Fetch information about a Pulsar Function")
    class GetFunction extends FunctionCommand {
        @Override
        void runCmd() throws Exception {
            FunctionConfig functionConfig = getAdmin().functions().getFunction(tenant, namespace, functionName);
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            System.out.println(gson.toJson(functionConfig));
        }
    }

    @Command(aliases = "getstatus", description = "Check the current status of a Pulsar Function")
    class GetFunctionStatus extends FunctionCommand {

        @Option(names = "--instance-id", description = "The function instanceId "
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

    @Command(description = "Get the current stats of a Pulsar Function")
    class GetFunctionStats extends FunctionCommand {

        @Option(names = "--instance-id", description = "The function instanceId "
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

    @Command(description = "Restart function instance")
    class RestartFunction extends FunctionCommand {

        @Option(names = "--instance-id", description = "The function instanceId "
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

    @Command(description = "Stops function instance")
    class StopFunction extends FunctionCommand {

        @Option(names = "--instance-id", description = "The function instanceId "
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

    @Command(description = "Starts a stopped function instance")
    class StartFunction extends FunctionCommand {

        @Option(names = "--instance-id", description = "The function instanceId "
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

    @Command(description = "Delete a Pulsar Function that is running on a Pulsar cluster")
    class DeleteFunction extends FunctionCommand {
        @Override
        void runCmd() throws Exception {
            getAdmin().functions().deleteFunction(tenant, namespace, functionName);
            print("Deleted successfully");
        }
    }

    @Command(description = "Update a Pulsar Function that has been deployed to a Pulsar cluster")
    class UpdateFunction extends FunctionDetailsCommand {

        @Option(names = "--update-auth-data", description = "Whether or not to update the auth data #Java, Python")
        protected boolean updateAuthData;

        @Override
        protected void validateFunctionConfigs(FunctionConfig functionConfig) {
            if (StringUtils.isEmpty(functionConfig.getName())) {
                org.apache.pulsar.common.functions.Utils.inferMissingFunctionName(functionConfig);
            }
            if (StringUtils.isEmpty(functionConfig.getName())) {
                throw new ParameterException("Function Name not provided");
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

    @Command(description = "List all Pulsar Functions running under a specific tenant and namespace")
    class ListFunctions extends NamespaceCommand {
        @Override
        void runCmd() throws Exception {
            print(getAdmin().functions().getFunctions(tenant, namespace));
        }
    }

    @Command(description = "Fetch the current state associated with a Pulsar Function")
    class StateGetter extends FunctionCommand {

        @Option(names = {"-k", "--key"}, description = "Key name of State")
        private String key = null;

        @Option(names = {"-w", "--watch"}, description = "Watch for changes in the value associated with a key "
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

    @Command(description = "Put the state associated with a Pulsar Function")
    class StatePutter extends FunctionCommand {

        @Option(names = {"-s", "--state"}, description = "The FunctionState that needs to be put", required = true)
        private String state = null;

        @Override
        void runCmd() throws Exception {
            FunctionState stateRepr =
                    ObjectMapperFactory.getMapper().reader().readValue(state, FunctionState.class);
            getAdmin().functions()
                    .putFunctionState(tenant, namespace, functionName, stateRepr);
        }
    }

    @Command(description = "Trigger the specified Pulsar Function with a supplied value")
    class TriggerFunction extends FunctionCommand {
        // for backward compatibility purposes
        @Option(names = "--triggerValue",
                description = "The value with which you want to trigger the function", hidden = true)
        protected String deprecatedTriggerValue;
        @Option(names = "--trigger-value", description = "The value with which you want to trigger the function")
        protected String triggerValue;
        // for backward compatibility purposes
        @Option(names = "--triggerFile", description = "The path to the file that contains the data with which "
                + "you want to trigger the function", hidden = true)
        protected String deprecatedTriggerFile;
        @Option(names = "--trigger-file", description = "The path to the file that contains the data with which "
                + "you want to trigger the function")
        protected String triggerFile;
        @Option(names = "--topic", description = "The specific topic name that the function consumes from that"
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

    @Command(description = "Upload File Data to Pulsar")
    class UploadFunction extends BaseCommand {
        // for backward compatibility purposes
        @Option(
                names = "--sourceFile",
                description = "The file whose contents need to be uploaded", hidden = true)
        protected String deprecatedSourceFile;
        @Option(
                names = "--source-file",
                description = "The file whose contents need to be uploaded")
        protected String sourceFile;
        @Option(
                names = "--path",
                description = "Path or functionPkgUrl where the contents need to be stored", required = true)
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

    @Command(description = "Download File Data from Pulsar")
    class DownloadFunction extends FunctionCommand {
        // for backward compatibility purposes
        @Option(
                names = "--destinationFile",
                description = "The file to store downloaded content", hidden = true)
        protected String deprecatedDestinationFile;
        @Option(
                names = "--destination-file",
                description = "The file to store downloaded content")
        protected String destinationFile;
        @Option(
                names = "--path",
                description = "Path or functionPkgUrl to store the content", required = false, hidden = true)
        protected String path;
        @Option(
                names = "--transform-function",
                description = "Download the transform Function of the connector")
        protected Boolean transformFunction = false;

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
                getAdmin().functions()
                        .downloadFunction(destinationFile, tenant, namespace, functionName, transformFunction);
            }
            print("Downloaded successfully");
        }
    }

    @Command(description = "Reload the available built-in functions")
    public class ReloadBuiltInFunctions extends CmdFunctions.BaseCommand {

        @Override
        void runCmd() throws Exception {
            getAdmin().functions().reloadBuiltInFunctions();
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
        addCommand("localrun", getLocalRunner());
        addCommand("create", getCreater());
        addCommand("delete", getDeleter());
        addCommand("update", getUpdater());
        addCommand("get", getGetter());
        addCommand("restart", getRestarter());
        addCommand("stop", getStopper());
        addCommand("start", getStarter());
        // TODO depecreate getstatus
        addCommand("status", getStatuser(), "getstatus");
        addCommand("stats", getFunctionStats());
        addCommand("list", getLister());
        addCommand("querystate", getStateGetter());
        addCommand("putstate", getStatePutter());
        addCommand("trigger", getTriggerer());
        addCommand("upload", getUploader());
        addCommand("download", getDownloader());
        addCommand("reload", new ReloadBuiltInFunctions());
        addCommand("available-functions", new ListBuiltInFunctions());
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

    @Command(description = "Get the list of Pulsar Functions supported by Pulsar cluster")
    public class ListBuiltInFunctions extends BaseCommand {
        @Override
        void runCmd() throws Exception {
            getAdmin().functions().getBuiltInFunctions()
                    .forEach(function -> {
                        print(function.getName());
                        print(WordUtils.wrap(function.getDescription(), 80));
                        print("----------------------------------------");
                    });
        }
    }

}
