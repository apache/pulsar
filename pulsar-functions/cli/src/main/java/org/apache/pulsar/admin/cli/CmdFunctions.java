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
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;
import com.google.protobuf.util.JsonFormat;
import lombok.Getter;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarFunctionsAdmin;
import org.apache.pulsar.functions.api.PulsarFunction;
import org.apache.pulsar.functions.proto.Function.FunctionConfig;
import org.apache.pulsar.functions.runtime.container.ThreadFunctionContainerFactory;
import org.apache.pulsar.functions.api.SerDe;
import org.apache.pulsar.functions.fs.LimitsConfig;
import org.apache.pulsar.functions.runtime.spawner.Spawner;
import org.apache.pulsar.functions.utils.FunctionConfigUtils;
import org.apache.pulsar.functions.utils.Reflections;

import java.io.File;

@Parameters(commandDescription = "Operations about functions")
public class CmdFunctions extends CmdBase {

    private final PulsarFunctionsAdmin fnAdmin;
    private final LocalRunner localRunner;
    private final CreateFunction creater;
    private final DeleteFunction deleter;
    private final UpdateFunction updater;
    private final GetFunction getter;
    private final GetFunctionStatus statuser;
    private final ListFunctions lister;

    /**
     * Base command
     */
    @Getter
    abstract class BaseCommand extends CliCommand {
        @Parameter(names = "--tenant", description = "Tenant Name\n")
        protected String tenant;

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
        @Parameter(names = "--namespace", description = "Namespace Name\n")
        protected String namespace;
    }

    /**
     * Function level command
     */
    @Getter
    abstract class FunctionCommand extends NamespaceCommand {
        @Parameter(names = "--function-name", description = "Function Name\n")
        protected String functionName;
    }

    /**
     * Commands that require a function config
     */
    @Getter
    abstract class FunctionConfigCommand extends FunctionCommand {
        @Parameter(names = "--function-classname", description = "Function Class Name\n")
        protected String className;
        @Parameter(
                names = "--jar",
                description = "Path to Jar\n",
                listConverter = StringConverter.class)
        protected String jarFile;
        @Parameter(
                names = "--py",
                description = "Path to Python\n",
                listConverter = StringConverter.class)
        protected String pyFile;
        @Parameter(names = "--source-topics", description = "Input Topic Name\n")
        protected String sourceTopicNames;
        @Parameter(names = "--sink-topic", description = "Output Topic Name\n")
        protected String sinkTopicName;

        @Parameter(names = "--input-serde-classnames", description = "Input SerDe\n")
        protected String inputSerdeClassNames;

        @Parameter(names = "--output-serde-classname", description = "Output SerDe\n")
        protected String outputSerdeClassName;

        @Parameter(names = "--function-config", description = "Function Config\n")
        protected String fnConfigFile;

        @Parameter(names = "--processing-guarantees", description = "Processing Guarantees\n")
        protected FunctionConfig.ProcessingGuarantees processingGuarantees;
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
            if (null != sourceTopicNames && null != inputSerdeClassNames) {
                String[] sourceTopicName = sourceTopicNames.split(",");
                String[] inputSerdeClassName = inputSerdeClassNames.split(",");
                if (sourceTopicName.length != inputSerdeClassName.length) {
                    throw new IllegalArgumentException(String.format("SourceTopics and InputSerde should match"));
                }
                for (int i = 0; i < sourceTopicName.length; ++i) {
                    functionConfigBuilder.putInputs(sourceTopicName[i], inputSerdeClassName[i]);
                }
            }
            if (null != sinkTopicName) {
                functionConfigBuilder.setSinkTopic(sinkTopicName);
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
            if (null != jarFile) {
                doJavaSubmitChecks(functionConfigBuilder);
                functionConfigBuilder.setRuntime(FunctionConfig.Runtime.JAVA);
                userCodeFile = jarFile;
            } else if (null != pyFile) {
                // Can we do any checks here?
                functionConfigBuilder.setRuntime(FunctionConfig.Runtime.PYTHON);
                userCodeFile = pyFile;
            }

            functionConfig = functionConfigBuilder.build();
        }

        private void doJavaSubmitChecks(FunctionConfig.Builder functionConfigBuilder) {
            // check if the function class exists in Jar and it implements PulsarFunction class
            if (!Reflections.classExistsInJar(new File(jarFile), functionConfigBuilder.getClassName())) {
                throw new IllegalArgumentException(String.format("Pulsar function class %s does not exist in jar %s",
                        functionConfigBuilder.getClassName(), jarFile));
            } else if (!Reflections.classInJarImplementsIface(new File(jarFile), functionConfigBuilder.getClassName(), PulsarFunction.class)) {
                throw new IllegalArgumentException(String.format("Pulsar function class %s in jar %s does not implemement PulsarFunction.class",
                        functionConfigBuilder.getClassName(), jarFile));
            }

            // Check if the Input serialization/deserialization class exists in jar or already loaded and that it
            // implements SerDe class
            functionConfigBuilder.getInputsMap().forEach((topicName, inputSerializer) -> {
                if (!Reflections.classExists(inputSerializer)
                        && !Reflections.classExistsInJar(new File(jarFile), inputSerializer)) {
                    throw new IllegalArgumentException(
                            String.format("Input serialization/deserialization class %s does not exist",
                                    inputSerializer));
                } else if (Reflections.classExists(inputSerializer)) {
                    if (!Reflections.classImplementsIface(inputSerializer, SerDe.class)) {
                        throw new IllegalArgumentException(String.format("Input serialization/deserialization class %s does not not implement %s",
                                inputSerializer, SerDe.class.getCanonicalName()));
                    }
                } else if (Reflections.classExistsInJar(new File(jarFile), inputSerializer)) {
                    if (!Reflections.classInJarImplementsIface(new File(jarFile), inputSerializer, SerDe.class)) {
                        throw new IllegalArgumentException(String.format("Input serialization/deserialization class %s does not not implement %s",
                                inputSerializer, SerDe.class.getCanonicalName()));
                    }
                }
            });
        }
    }

    @Parameters(commandDescription = "Run function locally")
    class LocalRunner extends FunctionConfigCommand {

        @Override
        void runCmd() throws Exception {
            if (!FunctionConfigUtils.areAllRequiredFieldsPresent(functionConfig)) {
                throw new RuntimeException("Missing arguments");
            }
            LimitsConfig limitsConfig = new LimitsConfig(
                -1,   // No timelimit
                1024,       // 1GB
                2,          // 2 cpus
                1024   // 1024 outstanding tuples
            );

            try (ThreadFunctionContainerFactory containerFactory = new ThreadFunctionContainerFactory(
                "LocalRunnerThreadGroup",
                limitsConfig.getMaxBufferedTuples(),
                admin.getServiceUrl().toString())) {

                Spawner spawner = Spawner.createSpawner(
                    functionConfig,
                    limitsConfig,
                    userCodeFile,
                    containerFactory,
                    null,
                    0);
                Runtime.getRuntime().addShutdownHook(new Thread() {
                    public void run() {
                        spawner.close();
                    }
                });
                spawner.start();
                spawner.join();
            }
        }
    }

    @Parameters(commandDescription = "Create function")
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

    @Parameters(commandDescription = "Get function")
    class GetFunction extends FunctionCommand {
        @Override
        void runCmd() throws Exception {
            if (tenant == null || namespace == null || functionName == null) {
                throw new RuntimeException("Missing arguments");
            }

            String json = JsonFormat.printer().print(fnAdmin.functions().getFunction(tenant, namespace, functionName));
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            System.out.println(gson.toJson(new JsonParser().parse(json)));
        }
    }

    @Parameters(commandDescription = "GetStatus function")
    class GetFunctionStatus extends FunctionCommand {
        @Override
        void runCmd() throws Exception {
            if (tenant == null || namespace == null || functionName == null) {
                throw new RuntimeException("Missing arguments");
            }
            String json = JsonFormat.printer().print(fnAdmin.functions().getFunctionStatus(tenant, namespace, functionName));
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            System.out.println(gson.toJson(new JsonParser().parse(json)));
        }
    }

    @Parameters(commandDescription = "Delete function")
    class DeleteFunction extends FunctionCommand {
        @Override
        void runCmd() throws Exception {
            if (tenant == null || namespace == null || functionName == null) {
                throw new RuntimeException("Missing arguments");
            }
            fnAdmin.functions().deleteFunction(tenant, namespace, functionName);
            print("Deleted successfully");
        }
    }

    @Parameters(commandDescription = "Update function")
    class UpdateFunction extends FunctionConfigCommand {
        @Override
        void runCmd() throws Exception {
            fnAdmin.functions().updateFunction(functionConfig, userCodeFile);
            print("Updated successfully");
        }
    }

    @Parameters(commandDescription = "List function")
    class ListFunctions extends NamespaceCommand {
        @Override
        void runCmd() throws Exception {
            if (tenant == null || namespace == null) {
                throw new RuntimeException("Missing arguments");
            }
            print(fnAdmin.functions().getFunctions(tenant, namespace));
        }
    }

    public CmdFunctions(PulsarAdmin admin) {
        super("functions", admin);
        this.fnAdmin = (PulsarFunctionsAdmin) admin;
        localRunner = new LocalRunner();
        creater = new CreateFunction();
        deleter = new DeleteFunction();
        updater = new UpdateFunction();
        getter = new GetFunction();
        statuser = new GetFunctionStatus();
        lister = new ListFunctions();
        jcommander.addCommand("localrun", getLocalRunner());
        jcommander.addCommand("create", getCreater());
        jcommander.addCommand("delete", getDeleter());
        jcommander.addCommand("update", getUpdater());
        jcommander.addCommand("get", getGetter());
        jcommander.addCommand("getstatus", getStatuser());
        jcommander.addCommand("list", getLister());
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
}
