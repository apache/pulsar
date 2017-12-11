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
import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.functions.fs.FunctionConfig;

@Parameters(commandDescription = "Operations about functions")
public class CmdFunctions extends CmdBase {

    private final LocalRunner cmdRunner;

    @Getter
    @Parameters(commandDescription = "Run function locally")
    class LocalRunner extends CliCommand {

        @Parameter(names = "--name", description = "Function Name\n")
        private String name;
        @Parameter(names = "--source-topic", description = "Input Topic Name\n")
        private String sourceTopicName;
        @Parameter(names = "--sink-topic", description = "Output Topic Name\n")
        private String sinkTopicName;

        @Parameter(names = "--function-config", description = "Function Config\n")
        private String fnConfigFile;

        @Override
        void run() throws Exception {
            FunctionConfig fc;
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
            // TODO: execute the runner here

            System.out.println(ReflectionToStringBuilder.toString(fc, ToStringStyle.MULTI_LINE_STYLE));
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
