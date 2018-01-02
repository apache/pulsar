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
import lombok.Getter;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarFunctionsAdmin;
import org.apache.pulsar.client.api.ClientConfiguration;
import org.apache.pulsar.functions.fs.FunctionConfig;
import org.apache.pulsar.functions.runtime.container.ThreadFunctionContainerFactory;
import org.apache.pulsar.functions.runtime.spawner.LimitsConfig;
import org.apache.pulsar.functions.runtime.spawner.Spawner;

@Parameters(commandDescription = "Operations about functions")
public class CmdFunctions extends CmdBase {

    private final PulsarFunctionsAdmin fnAdmin;
    private final LocalRunner localRunner;
    private final CreateFunction creater;
    private final DeleteFunction deleter;
    private final UpdateFunction updater;
    private final GetFunction getter;
    private final ListFunctions lister;

    /**
     * Base command
     */
    @Getter
    abstract class BaseCommand extends CliCommand {
        @Parameter(names = "--tenant", description = "Tenant Name")
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
        @Parameter(names = "--namespace", description = "Namespace Name")
        protected String namespace;
    }

    /**
     * Function level command
     */
    @Getter
    abstract class FunctionCommand extends NamespaceCommand {
        @Parameter(names = "--function-name", description = "Function Name")
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
                names = "--function-classpath",
                description = "Function Classpath\n",
                listConverter = StringConverter.class)
        protected String jarFile;
        @Parameter(names = "--source-topic", description = "Input Topic Name\n")
        protected String sourceTopicName;
        @Parameter(names = "--sink-topic", description = "Output Topic Name\n")
        protected String sinkTopicName;

        @Parameter(names = "--input-serde-classname", description = "Input SerDe\n")
        protected String inputSerdeClassName;

        @Parameter(names = "--output-serde-classname", description = "Output SerDe\n")
        protected String outputSerdeClassName;

        @Parameter(names = "--function-config", description = "Function Config\n")
        protected String fnConfigFile;
        protected FunctionConfig functionConfig;

        @Override
        void processArguments() throws Exception {
            if (null != fnConfigFile) {
                functionConfig = FunctionConfig.load(fnConfigFile);
            } else {
                functionConfig = new FunctionConfig();
            }
            if (null != sourceTopicName) {
                functionConfig.setSourceTopic(sourceTopicName);
            }
            if (null != sinkTopicName) {
                functionConfig.setSinkTopic(sinkTopicName);
            }
            if (null != tenant) {
                functionConfig.setTenant(tenant);
            }
            if (null != namespace) {
                functionConfig.setNamespace(namespace);
            }
            if (null != functionName) {
                functionConfig.setName(functionName);
            }
            if (null != className) {
                functionConfig.setClassName(className);
            }
            if (null != inputSerdeClassName) {
                functionConfig.setInputSerdeClassName(inputSerdeClassName);
            }
            if (null != outputSerdeClassName) {
                functionConfig.setOutputSerdeClassName(outputSerdeClassName);
            }
        }
    }

    @Parameters(commandDescription = "Run function locally")
    class LocalRunner extends FunctionConfigCommand {

        @Override
        void runCmd() throws Exception {
            LimitsConfig limitsConfig = new LimitsConfig(
                -1,   // No timelimit
                1024,       // 1GB
                2,          // 2 cpus
                1024   // 1024 outstanding tuples
            );

            ClientConfiguration clientConf;
            if (admin instanceof PulsarFunctionsAdmin) {
                clientConf = ((PulsarFunctionsAdmin) admin).getClientConf();
            } else {
                clientConf = new ClientConfiguration();
            }

            try (ThreadFunctionContainerFactory containerFactory = new ThreadFunctionContainerFactory(
                limitsConfig.getMaxBufferedTuples(),
                admin.getServiceUrl().toString(),
                clientConf)) {

                Spawner spawner = Spawner.createSpawner(
                    functionConfig,
                    limitsConfig,
                    jarFile,
                    containerFactory);

                spawner.start();
                spawner.join();
            }
        }
    }

    @Parameters(commandDescription = "Create function")
    class CreateFunction extends FunctionConfigCommand {
        @Override
        void runCmd() throws Exception {
            fnAdmin.functions().createFunction(functionConfig, jarFile);
            print("Created successfully");
        }
    }

    @Parameters(commandDescription = "Get function")
    class GetFunction extends FunctionCommand {
        @Override
        void runCmd() throws Exception {
            print(fnAdmin.functions().getFunction(tenant, namespace, functionName));
        }
    }

    @Parameters(commandDescription = "Delete function")
    class DeleteFunction extends FunctionCommand {
        @Override
        void runCmd() throws Exception {
            fnAdmin.functions().deleteFunction(tenant, namespace, functionName);
            print("Deleted successfully");
        }
    }

    @Parameters(commandDescription = "Update function")
    class UpdateFunction extends FunctionConfigCommand {
        @Override
        void runCmd() throws Exception {
            fnAdmin.functions().updateFunction(functionConfig, jarFile);
            print("Updated successfully");
        }
    }

    @Parameters(commandDescription = "List function")
    class ListFunctions extends NamespaceCommand {
        @Override
        void runCmd() throws Exception {
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
        lister = new ListFunctions();
        jcommander.addCommand("localrun", getLocalRunner());
        jcommander.addCommand("create", getCreater());
        jcommander.addCommand("delete", getDeleter());
        jcommander.addCommand("update", getUpdater());
        jcommander.addCommand("get", getGetter());
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
    ListFunctions getLister() {
        return lister;
    }
}
