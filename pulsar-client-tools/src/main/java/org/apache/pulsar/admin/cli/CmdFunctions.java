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

import static com.google.common.base.Preconditions.checkNotNull;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.StringConverter;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import org.apache.pulsar.functions.shaded.io.netty.buffer.ByteBuf;
import org.apache.pulsar.functions.shaded.io.netty.buffer.ByteBufUtil;
import org.apache.pulsar.functions.shaded.io.netty.buffer.Unpooled;
import java.net.MalformedURLException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.jodah.typetools.TypeResolver;
import org.apache.bookkeeper.api.StorageClient;
import org.apache.bookkeeper.api.kv.Table;
import org.apache.bookkeeper.api.kv.result.KeyValue;
import org.apache.bookkeeper.clients.StorageClientBuilder;
import org.apache.bookkeeper.clients.config.StorageClientSettings;
import org.apache.bookkeeper.clients.utils.NetUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminWithFunctions;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.utils.DefaultSerDe;
import org.apache.pulsar.functions.proto.Function.FunctionConfig;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.runtime.ProcessRuntimeFactory;
import org.apache.pulsar.functions.api.SerDe;
import org.apache.pulsar.functions.runtime.RuntimeSpawner;
import org.apache.pulsar.functions.utils.FunctionConfigUtils;
import org.apache.pulsar.functions.utils.Reflections;

import java.io.File;
import java.lang.reflect.Type;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.pulsar.functions.utils.Utils;

@Slf4j
@Parameters(commandDescription = "Interface for managing Pulsar Functions (lightweight, Lambda-style compute processes that work with Pulsar)")
public class CmdFunctions extends CmdBase {

    private final PulsarAdminWithFunctions fnAdmin;
    private final LocalRunner localRunner;
    private final CreateFunction creater;
    private final DeleteFunction deleter;
    private final UpdateFunction updater;
    private final GetFunction getter;
    private final GetFunctionStatus statuser;
    private final ListFunctions lister;
    private final StateGetter stateGetter;
    private final TriggerFunction triggerer;

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

        void processArguments() throws Exception {}

        abstract void runCmd() throws Exception;
    }

    /**
     * Namespace level command
     */
    @Getter
    abstract class NamespaceCommand extends BaseCommand {
        @Parameter(names = "--tenant", description = "The function's tenant", required = true)
        protected String tenant;

        @Parameter(names = "--namespace", description = "The function's namespace", required = true)
        protected String namespace;
    }

    /**
     * Function level command
     */
    @Getter
    abstract class FunctionCommand extends NamespaceCommand {
        @Parameter(names = "--name", description = "The function's name", required = true)
        protected String functionName;
    }

    /**
     * Commands that require a function config
     */
    @Getter
    abstract class FunctionConfigCommand extends BaseCommand {
        @Parameter(names = "--tenant", description = "The function's tenant")
        protected String tenant;
        @Parameter(names = "--namespace", description = "The function's namespace")
        protected String namespace;
        @Parameter(names = "--name", description = "The function's name")
        protected String functionName;
        @Parameter(names = "--className", description = "The function's class name", required = true)
        protected String className;
        @Parameter(
                names = "--jar",
                description = "Path to the jar file for the function (if the function is written in Java)",
                listConverter = StringConverter.class)
        protected String jarFile;
        @Parameter(
                names = "--py",
                description = "Path to the main Python file for the function (if the function is written in Python)",
                listConverter = StringConverter.class)
        protected String pyFile;
        @Parameter(names = "--inputs", description = "The function's input topic or topics (multiple topics can be specified as a comma-separated list)")
        protected String inputs;
        @Parameter(names = "--output", description = "The function's output topic")
        protected String output;
        @Parameter(names = "--logTopic", description = "The topic to which the function's logs are produced")
        protected String logTopic;
        @Parameter(names = "--customSerdeInputs", description = "The map of input topics to SerDe class names (as a JSON string)")
        protected String customSerdeInputString;
        @Parameter(names = "--outputSerdeClassName", description = "The SerDe class to be used for messages output by the function")
        protected String outputSerdeClassName;
        @Parameter(names = "--functionConfigFile", description = "The path to a YAML config file specifying the function's configuration")
        protected String fnConfigFile;
        @Parameter(names = "--processingGuarantees", description = "The processing guarantees (aka delivery semantics) applied to the function")
        protected FunctionConfig.ProcessingGuarantees processingGuarantees;
        @Parameter(names = "--subscriptionType", description = "The type of subscription used by the function when consuming messages from the input topic(s)")
        protected FunctionConfig.SubscriptionType subscriptionType;
        @Parameter(names = "--userConfig", description = "User-defined config key/values")
        protected String userConfigString;
        @Parameter(names = "--parallelism", description = "The function's parallelism factor (i.e. the number of function instances to run)")
        protected String parallelism;

        protected FunctionConfig functionConfig;
        protected String userCodeFile;

        @Override
        void processArguments() throws Exception {

            FunctionConfig.Builder functionConfigBuilder;
            if (null != fnConfigFile) {
                functionConfigBuilder = FunctionConfigUtils.loadConfig(new File(fnConfigFile));
            } else {
                functionConfigBuilder = FunctionConfig.newBuilder();
            }
            if (null != inputs) {
                String[] topicNames = inputs.split(",");
                for (int i = 0; i < topicNames.length; ++i) {
                    functionConfigBuilder.addInputs(topicNames[i]);
                }
            }
            if (null != customSerdeInputString) {
                Type type = new TypeToken<Map<String, String>>(){}.getType();
                Map<String, String> customSerdeInputMap = new Gson().fromJson(customSerdeInputString, type);
                functionConfigBuilder.putAllCustomSerdeInputs(customSerdeInputMap);
            }
            if (null != output) {
                functionConfigBuilder.setOutput(output);
            }
            if (null != logTopic) {
                functionConfigBuilder.setLogTopic(logTopic);
            }
            if (null != tenant) {
                functionConfigBuilder.setTenant(tenant);
            }
            if (null != namespace) {
                functionConfigBuilder.setNamespace(namespace);
            }
            if (null != functionName) {
                functionConfigBuilder.setName(functionName);
            }
            if (null != className) {
                functionConfigBuilder.setClassName(className);
            }
            if (null != outputSerdeClassName) {
                functionConfigBuilder.setOutputSerdeClassName(outputSerdeClassName);
            }
            if (null != processingGuarantees) {
                functionConfigBuilder.setProcessingGuarantees(processingGuarantees);
            }
            if (null != subscriptionType) {
                functionConfigBuilder.setSubscriptionType(subscriptionType);
            }
            if (null != userConfigString) {
                Type type = new TypeToken<Map<String, String>>(){}.getType();
                Map<String, String> userConfigMap = new Gson().fromJson(userConfigString, type);
                functionConfigBuilder.putAllUserConfig(userConfigMap);
            }
            if (null != jarFile) {
                doJavaSubmitChecks(functionConfigBuilder);
                functionConfigBuilder.setRuntime(FunctionConfig.Runtime.JAVA);
                userCodeFile = jarFile;
            } else if (null != pyFile) {
                doPythonSubmitChecks(functionConfigBuilder);
                functionConfigBuilder.setRuntime(FunctionConfig.Runtime.PYTHON);
                userCodeFile = pyFile;
            } else {
                throw new RuntimeException("Either a Java jar or a Python file needs to be specified for the function");
            }

            if (functionConfigBuilder.getInputsCount() == 0 && functionConfigBuilder.getCustomSerdeInputsCount() == 0) {
                throw new RuntimeException("No input topic(s) specified for the function");
            }

            if (parallelism == null) {
                if (functionConfigBuilder.getParallelism() == 0) {
                    functionConfigBuilder.setParallelism(1);
                }
            } else {
                int num = Integer.parseInt(parallelism);
                if (num <= 0) {
                    throw new IllegalArgumentException("The parallelism factor (the number of instances) for the function must be positive");
                }
                functionConfigBuilder.setParallelism(num);
            }

            functionConfigBuilder.setAutoAck(true);
            inferMissingArguments(functionConfigBuilder);
            functionConfig = functionConfigBuilder.build();
        }

        private void doJavaSubmitChecks(FunctionConfig.Builder functionConfigBuilder) {
            File file = new File(jarFile);
            // check if the function class exists in Jar and it implements Function class
            if (!Reflections.classExistsInJar(file, functionConfigBuilder.getClassName())) {
                throw new IllegalArgumentException(String.format("Pulsar function class %s does not exist in jar %s",
                        functionConfigBuilder.getClassName(), jarFile));
            } else if (!Reflections.classInJarImplementsIface(file, functionConfigBuilder.getClassName(), Function.class)
                    && !Reflections.classInJarImplementsIface(file, functionConfigBuilder.getClassName(), java.util.function.Function.class)) {
                throw new IllegalArgumentException(String.format("The Pulsar function class %s in jar %s implements neither org.apache.pulsar.functions.api.Function nor java.util.function.Function",
                        functionConfigBuilder.getClassName(), jarFile));
            }

            ClassLoader userJarLoader;
            try {
                userJarLoader = Reflections.loadJar(file);
            } catch (MalformedURLException e) {
                throw new RuntimeException("Failed to load user jar " + file, e);
            }

            Object userClass = Reflections.createInstance(functionConfigBuilder.getClassName(), file);
            Class<?>[] typeArgs;
            if (userClass instanceof Function) {
                Function pulsarFunction = (Function) userClass;
                if (pulsarFunction == null) {
                    throw new IllegalArgumentException(String.format("The Pulsar function class %s could not be instantiated from jar %s",
                            functionConfigBuilder.getClassName(), jarFile));
                }
                typeArgs = TypeResolver.resolveRawArguments(Function.class, pulsarFunction.getClass());
            } else {
                java.util.function.Function function = (java.util.function.Function) userClass;
                if (function == null) {
                    throw new IllegalArgumentException(String.format("The Java util function class %s could not be instantiated from jar %s",
                            functionConfigBuilder.getClassName(), jarFile));
                }
                typeArgs = TypeResolver.resolveRawArguments(java.util.function.Function.class, function.getClass());
            }

            // Check if the Input serialization/deserialization class exists in jar or already loaded and that it
            // implements SerDe class
            functionConfigBuilder.getCustomSerdeInputsMap().forEach((topicName, inputSerializer) -> {
                if (!Reflections.classExists(inputSerializer)
                        && !Reflections.classExistsInJar(new File(jarFile), inputSerializer)) {
                    throw new IllegalArgumentException(
                            String.format("The input serialization/deserialization class %s does not exist",
                                    inputSerializer));
                } else if (Reflections.classExists(inputSerializer)) {
                    if (!Reflections.classImplementsIface(inputSerializer, SerDe.class)) {
                        throw new IllegalArgumentException(String.format("The input serialization/deserialization class %s does not not implement %s",
                                inputSerializer, SerDe.class.getCanonicalName()));
                    }
                } else if (Reflections.classExistsInJar(new File(jarFile), inputSerializer)) {
                    if (!Reflections.classInJarImplementsIface(new File(jarFile), inputSerializer, SerDe.class)) {
                        throw new IllegalArgumentException(String.format("The input serialization/deserialization class %s does not not implement %s",
                                inputSerializer, SerDe.class.getCanonicalName()));
                    }
                }
                if (inputSerializer.equals(DefaultSerDe.class.getName())) {
                    if (!DefaultSerDe.IsSupportedType(typeArgs[0])) {
                        throw new RuntimeException("The default Serializer does not support type " + typeArgs[0]);
                    }
                } else {
                    SerDe serDe = (SerDe) Reflections.createInstance(inputSerializer, file);
                    if (serDe == null) {
                        throw new IllegalArgumentException(String.format("The SerDe class %s does not exist in jar %s",
                                inputSerializer, jarFile));
                    }
                    Class<?>[] serDeTypes = TypeResolver.resolveRawArguments(SerDe.class, serDe.getClass());

                    // type inheritance information seems to be lost in generic type
                    // load the actual type class for verification
                    Class<?> fnInputClass;
                    Class<?> serdeInputClass;
                    try {
                        fnInputClass = Class.forName(typeArgs[0].getName(), true, userJarLoader);
                        serdeInputClass = Class.forName(serDeTypes[0].getName(), true, userJarLoader);
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException("Failed to load type class", e);
                    }

                    if (!fnInputClass.isAssignableFrom(serdeInputClass)) {
                        throw new RuntimeException("Serializer type mismatch " + typeArgs[0] + " vs " + serDeTypes[0]);
                    }
                }
            });
            functionConfigBuilder.getInputsList().forEach((topicName) -> {
                if (!DefaultSerDe.IsSupportedType(typeArgs[0])) {
                    throw new RuntimeException("Default Serializer does not support type " + typeArgs[0]);
                }
            });
            if (!Void.class.equals(typeArgs[1])) {
                if (functionConfigBuilder.getOutputSerdeClassName() == null
                        || functionConfigBuilder.getOutputSerdeClassName().isEmpty()
                        || functionConfigBuilder.getOutputSerdeClassName().equals(DefaultSerDe.class.getName())) {
                    if (!DefaultSerDe.IsSupportedType(typeArgs[1])) {
                        throw new RuntimeException("Default Serializer does not support type " + typeArgs[1]);
                    }
                } else {
                    SerDe serDe = (SerDe) Reflections.createInstance(functionConfigBuilder.getOutputSerdeClassName(), file);
                    if (serDe == null) {
                        throw new IllegalArgumentException(String.format("SerDe class %s does not exist in jar %s",
                                functionConfigBuilder.getOutputSerdeClassName(), jarFile));
                    }
                    Class<?>[] serDeTypes = TypeResolver.resolveRawArguments(SerDe.class, serDe.getClass());

                    // type inheritance information seems to be lost in generic type
                    // load the actual type class for verification
                    Class<?> fnOutputClass;
                    Class<?> serdeOutputClass;
                    try {
                        fnOutputClass = Class.forName(typeArgs[1].getName(), true, userJarLoader);
                        serdeOutputClass = Class.forName(serDeTypes[0].getName(), true, userJarLoader);
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException("Failed to load type class", e);
                    }

                    if (!serdeOutputClass.isAssignableFrom(fnOutputClass)) {
                        throw new RuntimeException("Serializer type mismatch " + typeArgs[1] + " vs " + serDeTypes[0]);
                    }
                }
            }
        }

        private void doPythonSubmitChecks(FunctionConfig.Builder functionConfigBuilder) {
            if (functionConfigBuilder.getProcessingGuarantees() == FunctionConfig.ProcessingGuarantees.EFFECTIVELY_ONCE) {
                throw new RuntimeException("Effectively-once processing guarantees not yet supported in Python");
            }
        }

        private void inferMissingArguments(FunctionConfig.Builder builder) {
            if (builder.getName() == null || builder.getName().isEmpty()) {
                inferMissingFunctionName(builder);
            }
            if (builder.getTenant() == null || builder.getTenant().isEmpty()) {
                inferMissingTenant(builder);
            }
            if (builder.getNamespace() == null || builder.getNamespace().isEmpty()) {
                inferMissingNamespace(builder);
            }
            if (builder.getOutput() == null || builder.getOutput().isEmpty()) {
                inferMissingOutput(builder);
            }
        }

        private void inferMissingFunctionName(FunctionConfig.Builder builder) {
            String [] domains = builder.getClassName().split("\\.");
            if (domains.length == 0) {
                builder.setName(builder.getClassName());
            } else {
                builder.setName(domains[domains.length - 1]);
            }
        }

        private void inferMissingTenant(FunctionConfig.Builder builder) {
            try {
                String inputTopic = getUniqueInput(builder);
                builder.setTenant(TopicName.get(inputTopic).getProperty());
            } catch (IllegalArgumentException ex) {
                throw new RuntimeException("You need to specify a tenant for the function", ex);
            }
        }

        private void inferMissingNamespace(FunctionConfig.Builder builder) {
            try {
                String inputTopic = getUniqueInput(builder);
                builder.setNamespace(TopicName.get(inputTopic).getNamespacePortion());
            } catch (IllegalArgumentException ex) {
                throw new RuntimeException("You need to specify a namespace for the function");
            }
        }

        private void inferMissingOutput(FunctionConfig.Builder builder) {
            try {
                String inputTopic = getUniqueInput(builder);
                builder.setOutput(inputTopic + "-" + builder.getName() + "-output");
            } catch (IllegalArgumentException ex) {
                // It might be that we really don't need an output topic
                // So we cannot really throw an exception
            }
        }

        private String getUniqueInput(FunctionConfig.Builder builder) {
            if (builder.getInputsCount() + builder.getCustomSerdeInputsCount() != 1) {
                throw new IllegalArgumentException();
            }
            if (builder.getInputsCount() == 1) {
                return builder.getInputs(0);
            } else {
                return builder.getCustomSerdeInputsMap().keySet().iterator().next();
            }
        }
    }

    @Parameters(commandDescription = "Run the Pulsar Function locally (rather than deploying it to the Pulsar cluster)")
    class LocalRunner extends FunctionConfigCommand {

        // TODO: this should become bookkeeper url and it should be fetched from pulsar client.
        @Parameter(names = "--stateStorageServiceUrl", description = "The URL for the state storage service (by default Apache BookKeeper)")
        protected String stateStorageServiceUrl;

        @Parameter(names = "--brokerServiceUrl", description = "The URL for the Pulsar broker")
        protected String brokerServiceUrl;

        @Override
        void runCmd() throws Exception {
            if (!FunctionConfigUtils.areAllRequiredFieldsPresent(functionConfig)) {
                throw new RuntimeException("Missing arguments");
            }

            String serviceUrl = admin.getServiceUrl();
            if (brokerServiceUrl != null) {
                serviceUrl = brokerServiceUrl;
            }
            if (serviceUrl == null) {
                serviceUrl = "pulsar://localhost:6650";
            }
            try (ProcessRuntimeFactory containerFactory = new ProcessRuntimeFactory(
                    serviceUrl, null, null, null)) {
                List<RuntimeSpawner> spawners = new LinkedList<>();
                for (int i = 0; i < functionConfig.getParallelism(); ++i) {
                    InstanceConfig instanceConfig = new InstanceConfig();
                    instanceConfig.setFunctionConfig(functionConfig);
                    // TODO: correctly implement function version and id
                    instanceConfig.setFunctionVersion(UUID.randomUUID().toString());
                    instanceConfig.setFunctionId(UUID.randomUUID().toString());
                    instanceConfig.setInstanceId(Integer.toString(i));
                    instanceConfig.setMaxBufferedTuples(1024);
                    RuntimeSpawner runtimeSpawner = new RuntimeSpawner(
                            instanceConfig,
                            userCodeFile,
                            containerFactory,
                            null);
                    spawners.add(runtimeSpawner);
                    runtimeSpawner.start();
                }
                Runtime.getRuntime().addShutdownHook(new Thread() {
                    public void run() {
                        log.info("Shutting down the localrun runtimeSpawner ...");
                        for (RuntimeSpawner spawner : spawners) {
                            spawner.close();
                        }
                    }
                });
                for (RuntimeSpawner spawner : spawners) {
                    spawner.join();
                    log.info("RuntimeSpawner quit because of {}", spawner.getRuntime().getDeathException());
                }

            }
        }
    }

    @Parameters(commandDescription = "Create a Pulsar Function in cluster mode (i.e. deploy it on a Pulsar cluster)")
    class CreateFunction extends FunctionConfigCommand {
        @Override
        void runCmd() throws Exception {
            if (!FunctionConfigUtils.areAllRequiredFieldsPresent(functionConfig)) {
                throw new RuntimeException("Missing arguments");
            }
            fnAdmin.functions().createFunction(functionConfig, userCodeFile);
            print("Created successfully");
        }
    }

    @Parameters(commandDescription = "Fetch information about a Pulsar Function")
    class GetFunction extends FunctionCommand {
        @Override
        void runCmd() throws Exception {
            String json = Utils.printJson(fnAdmin.functions().getFunction(tenant, namespace, functionName));
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            System.out.println(gson.toJson(new JsonParser().parse(json)));
        }
    }

    @Parameters(commandDescription = "Check the current status of a Pulsar Function")
    class GetFunctionStatus extends FunctionCommand {
        @Override
        void runCmd() throws Exception {
            String json = Utils.printJson(fnAdmin.functions().getFunctionStatus(tenant, namespace, functionName));
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            System.out.println(gson.toJson(new JsonParser().parse(json)));
        }
    }

    @Parameters(commandDescription = "Delete a Pulsar Function that's running on a Pulsar cluster")
    class DeleteFunction extends FunctionCommand {
        @Override
        void runCmd() throws Exception {
            fnAdmin.functions().deleteFunction(tenant, namespace, functionName);
            print("Deleted successfully");
        }
    }

    @Parameters(commandDescription = "Update a Pulsar Function that's been deployed to a Pulsar cluster")
    class UpdateFunction extends FunctionConfigCommand {
        @Override
        void runCmd() throws Exception {
            if (!FunctionConfigUtils.areAllRequiredFieldsPresent(functionConfig)) {
                throw new RuntimeException("Missing arguments");
            }
            fnAdmin.functions().updateFunction(functionConfig, userCodeFile);
            print("Updated successfully");
        }
    }

    @Parameters(commandDescription = "List all of the Pulsar Functions running under a specific tenant and namespace")
    class ListFunctions extends NamespaceCommand {
        @Override
        void runCmd() throws Exception {
            print(fnAdmin.functions().getFunctions(tenant, namespace));
        }
    }

    @Parameters(commandDescription = "Fetch the current state associated with a Pulsar Function running in cluster mode")
    class StateGetter extends FunctionCommand {

        @Parameter(names = { "-k", "--key" }, description = "key")
        private String key = null;

        // TODO: this url should be fetched along with bookkeeper location from pulsar admin
        @Parameter(names = { "-u", "--storage-service-url" }, description = "The URL for the storage service used by the function")
        private String stateStorageServiceUrl = null;

        @Parameter(names = { "-w", "--watch" }, description = "Watch for changes in the value associated with a key for a Pulsar Function")
        private boolean watch = false;

        @Override
        void runCmd() throws Exception {
            checkNotNull(stateStorageServiceUrl, "The state storage service URL is missing");

            String tableNs = String.format(
                "%s_%s",
                tenant,
                namespace);

            String tableName = getFunctionName();

            try (StorageClient client = StorageClientBuilder.newBuilder()
                 .withSettings(StorageClientSettings.newBuilder()
                     .addEndpoints(NetUtils.parseEndpoint(stateStorageServiceUrl))
                     .clientName("functions-admin")
                     .build())
                 .withNamespace(tableNs)
                 .build()) {
                try (Table<ByteBuf, ByteBuf> table = result(client.openTable(tableName))) {
                    long lastVersion = -1L;
                    do {
                        try (KeyValue<ByteBuf, ByteBuf> kv = result(table.getKv(Unpooled.wrappedBuffer(key.getBytes(UTF_8))))) {
                            if (null == kv) {
                                System.out.println("key '" + key + "' doesn't exist.");
                            } else {
                                if (kv.version() > lastVersion) {
                                    if (kv.isNumber()) {
                                        System.out.println("value = " + kv.numberValue());
                                    } else {
                                        System.out.println("value = " + new String(ByteBufUtil.getBytes(kv.value()), UTF_8));
                                    }
                                    lastVersion = kv.version();
                                }
                            }
                        }
                        if (watch) {
                            Thread.sleep(1000);
                        }
                    } while (watch);
                }
            }

        }
    }

    @Parameters(commandDescription = "Triggers the specified Pulsar Function with a supplied value")
    class TriggerFunction extends FunctionCommand {
        @Parameter(names = "--triggerValue", description = "The value with which you want to trigger the function")
        protected String triggerValue;
        @Parameter(names = "--triggerFile", description = "The path to the file that contains the data with which you'd like to trigger the function")
        protected String triggerFile;
        @Override
        void runCmd() throws Exception {
            if (triggerFile == null && triggerValue == null) {
                throw new RuntimeException("Either a trigger value or a trigger filepath needs to be specified");
            }
            String retval = fnAdmin.functions().triggerFunction(tenant, namespace, functionName, triggerValue, triggerFile);
            System.out.println(retval);
        }
    }

    public CmdFunctions(PulsarAdmin admin) throws PulsarClientException {
        super("functions", admin);
        if (admin instanceof PulsarAdminWithFunctions) {
            this.fnAdmin = (PulsarAdminWithFunctions) admin;
        } else {
            this.fnAdmin = new PulsarAdminWithFunctions(admin.getServiceUrl(), admin.getClientConfigData());
        }
        localRunner = new LocalRunner();
        creater = new CreateFunction();
        deleter = new DeleteFunction();
        updater = new UpdateFunction();
        getter = new GetFunction();
        statuser = new GetFunctionStatus();
        lister = new ListFunctions();
        stateGetter = new StateGetter();
        triggerer = new TriggerFunction();
        jcommander.addCommand("localrun", getLocalRunner());
        jcommander.addCommand("create", getCreater());
        jcommander.addCommand("delete", getDeleter());
        jcommander.addCommand("update", getUpdater());
        jcommander.addCommand("get", getGetter());
        jcommander.addCommand("getstatus", getStatuser());
        jcommander.addCommand("list", getLister());
        jcommander.addCommand("querystate", getStateGetter());
        jcommander.addCommand("trigger", getTriggerer());
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
    GetFunctionStatus getStatuser() { return statuser; }

    @VisibleForTesting
    ListFunctions getLister() {
        return lister;
    }

    @VisibleForTesting
    StateGetter getStateGetter() {
        return stateGetter;
    }

    @VisibleForTesting
    TriggerFunction getTriggerer() {
        return triggerer;
    }
}
