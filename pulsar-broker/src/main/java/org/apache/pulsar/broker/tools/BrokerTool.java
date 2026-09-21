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
package org.apache.pulsar.broker.tools;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ScopeType;

/**
 * <b>broker-tool</b> is used for operations on a specific broker.
 */
@Command(name = "broker-tool", description = "broker-tool is used for operations on a specific broker",
        showDefaultValues = true, scope = ScopeType.INHERIT)
public class BrokerTool {

    @Option(
            names = {"-h", "--help"},
            description = "Display help information",
            usageHelp = true
    )
    public boolean help = false;

    public static int run(String[] args) {
        BrokerTool brokerTool = new BrokerTool();
        CommandLine commander = new CommandLine(brokerTool);
        GenerateDocsCommand generateDocsCommand = new GenerateDocsCommand(commander);
        commander.addSubcommand(LoadReportCommand.class)
                .addSubcommand(generateDocsCommand);
        return commander.execute(args);
    }

    public static void main(String[] args) {
        int retCode = run(args);
        Runtime.getRuntime().exit(retCode);
    }
}
