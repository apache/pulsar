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
package org.apache.pulsar;

import com.beust.jcommander.JCommander;
import java.util.Arrays;
import org.apache.pulsar.broker.tools.LoadReportCommand;
import org.apache.pulsar.common.util.CmdGenerateDocumentation;

public class PulsarTool {
    private static void genBroker(String[] args) {
        CmdGenerateDocumentation cmd = new CmdGenerateDocumentation("pulsar");
        cmd.jcommander.addCommand("broker", new PulsarBrokerStarter.StarterArguments());
        cmd.run(args);
    }

    private static void genBrokerTool(String[] args) {
        CmdGenerateDocumentation cmd = new CmdGenerateDocumentation("pulsar");
        JCommander commander = new JCommander();
        commander.addCommand("load-report", new LoadReportCommand.Flags());
        cmd.jcommander.addCommand("broker-tool", commander);
        cmd.run(args);
    }

    public static void main(String[] args) throws Exception {
        String subCmdName = args[0];
        String[] subArgs = Arrays.copyOfRange(args, 1, args.length);
        if ("broker".equals(subCmdName)) {
            genBroker(subArgs);
        } else {
            genBrokerTool(subArgs);
        }
    }
}
