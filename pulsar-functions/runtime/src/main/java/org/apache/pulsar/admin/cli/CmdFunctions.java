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
import com.google.common.collect.Lists;
import java.util.List;
import lombok.Getter;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.functions.fs.FunctionConfig;
import org.apache.pulsar.functions.runtime.spawner.LimitsConfig;
import org.apache.pulsar.functions.runtime.spawner.Spawner;

@Parameters(commandDescription = "Operations about functions")
public class CmdFunctions extends CmdBase {

    private final LocalRunner cmdRunner;

    @Getter
    abstract class FunctionsCommand extends CliCommand {
        @Parameter(names = "--name", description = "Function Name\n")
        protected String name;
        @Parameter(names = "--function-classname", description = "Function Class Name\n")
        protected String className;
        @Parameter(
                names = "--function-classpath",
                description = "Function Classpath\n",
                listConverter = StringConverter.class)
        protected List<String> jarFiles;
        @Parameter(names = "--source-topic", description = "Input Topic Name\n")
        protected String sourceTopicName;
        @Parameter(names = "--sink-topic", description = "Output Topic Name\n")
        protected String sinkTopicName;

        @Parameter(names = "--serde-classname", description = "SerDe\n")
        protected String serDeClassName;

        @Parameter(names = "--function-config", description = "Function Config\n")
        protected String fnConfigFile;
        protected FunctionConfig functionConfig;

        @Override
        void run() throws Exception {
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
            if (null != name) {
                functionConfig.setName(name);
            }
            if (null != className) {
                functionConfig.setClassName(className);
            }
            if (null != serDeClassName) {
                functionConfig.setSerdeClassName(serDeClassName);
            }
            if (null != jarFiles) {
                functionConfig.setJarFiles(jarFiles);
            } else {
                functionConfig.setJarFiles(Lists.newArrayList());
            }

            run_functions_cmd();
        }

        abstract void run_functions_cmd() throws Exception;
    }

    @Getter
    @Parameters(commandDescription = "Run function locally")
    class LocalRunner extends FunctionsCommand {

        @Override
        void run_functions_cmd() throws Exception {
            LimitsConfig limitsConfig = new LimitsConfig(
                60000,   // 60 seconds
                1024,       // 1GB
                1024   // 1024 outstanding tuples
            );

            Spawner spawner = Spawner.createSpawner(
                functionConfig,
                limitsConfig,
                admin.getServiceUrl().toString());

            spawner.start();
            spawner.join();
        }

    }

    public CmdFunctions(PulsarAdmin admin) {
        super("functions", admin);
        cmdRunner = new LocalRunner();
        jcommander.addCommand("run", cmdRunner);
    }

    @VisibleForTesting
    LocalRunner getCmdRunner() {
        return cmdRunner;
    }
}
