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
import java.util.concurrent.CountDownLatch;
import lombok.Getter;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.functions.fs.FunctionConfig;
import org.apache.pulsar.functions.instance.JavaInstanceConfig;
import org.apache.pulsar.functions.runtime.container.SerDe;
import org.apache.pulsar.functions.spawner.LimitsConfig;
import org.apache.pulsar.functions.spawner.Spawner;

@Parameters(commandDescription = "Operations about functions")
public class CmdFunctions extends CmdBase {

    private final LocalRunner cmdRunner;

    @Getter
    @Parameters(commandDescription = "Run function locally")
    class LocalRunner extends CliCommand {

        @Parameter(names = "--name", description = "Function Name\n")
        private String name;
        @Parameter(names = "--function-classname", description = "Function Class Name\n")
        private String className;
        @Parameter(
            names = "--function-classpath",
            description = "Function Classpath\n",
            listConverter = StringConverter.class)
        private List<String> jarFiles;
        @Parameter(names = "--source-topic", description = "Input Topic Name\n")
        private String sourceTopicName;
        @Parameter(names = "--sink-topic", description = "Output Topic Name\n")
        private String sinkTopicName;

        @Parameter(names = "--serde-classname", description = "SerDe\n")
        private String serDeClassName;

        @Parameter(names = "--function-config", description = "Function Config\n")
        private String fnConfigFile;

        @Override
        void run() throws Exception {
            FunctionConfig fc;
            SerDe serDe = null;
            if (null != fnConfigFile) {
                fc = FunctionConfig.load(fnConfigFile);
            } else {
                fc = new FunctionConfig();
            }
            if (null != sourceTopicName) {
                fc.setSourceTopic(sourceTopicName);
            }
            if (null != sinkTopicName) {
                fc.setSinkTopic(sinkTopicName);
            }
            if (null != name) {
                fc.setName(name);
            }
            if (null != className) {
                fc.setClassName(className);
            }
            if (null != serDeClassName) {
                serDe = createSerDe(serDeClassName);
            }
            if (null != jarFiles) {
                fc.setJarFiles(jarFiles);
            } else {
                fc.setJarFiles(Lists.newArrayList());
            }

            // Construct the spawner

            LimitsConfig limitsConfig = new LimitsConfig(
                60000,   // 60 seconds
                1024        // 1GB
            );

            Spawner spawner = Spawner.createSpawner(
                fc,
                limitsConfig,
                serDe,
                admin.getServiceUrl().toString());

            spawner.start();
            spawner.join();
        }

    }

    SerDe createSerDe(String className) {
        SerDe retval;
        try {
            Class<?> clazz = Class.forName(className);
            retval = (SerDe) clazz.newInstance();
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException(ex + " User class must be in class path.");
        } catch (InstantiationException ex) {
            throw new RuntimeException(ex + " User class must be concrete.");
        } catch (IllegalAccessException ex) {
            throw new RuntimeException(ex + " User class must have a no-arg constructor.");
        }
        return retval;
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
