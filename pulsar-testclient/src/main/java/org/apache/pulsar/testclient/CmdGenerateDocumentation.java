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
package org.apache.pulsar.testclient;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.ScopeType;

@Slf4j
public class CmdGenerateDocumentation {

    @Command(description = "Generate documentation automatically.", showDefaultValues = true, scope = ScopeType.INHERIT)
    static class Arguments {

        @Option(names = {"-h", "--help"}, description = "Help message", help = true)
        boolean help;

        @Option(names = {"-n", "--command-names"}, description = "List of command names")
        private List<String> commandNames = new ArrayList<>();

    }

    public static void main(String[] args) throws Exception {
        final Arguments arguments = new Arguments();
        CommandLine commander = new CommandLine(arguments);
        commander.setCommandName("pulsar-perf gen-doc");
        try {
            commander.parseArgs(args);
        } catch (ParameterException e) {
            System.out.println(e.getMessage());
            commander.usage(commander.getOut());
            PerfClientUtils.exit(1);
        }


        if (arguments.help) {
            commander.usage(commander.getOut());
            PerfClientUtils.exit(1);
        }

        Map<String, Class<?>> cmdClassMap = new LinkedHashMap<>();
        cmdClassMap.put("produce", Class.forName("org.apache.pulsar.testclient.PerformanceProducer$Arguments"));
        cmdClassMap.put("consume", Class.forName("org.apache.pulsar.testclient.PerformanceConsumer$Arguments"));
        cmdClassMap.put("transaction", Class.forName("org.apache.pulsar.testclient.PerformanceTransaction$Arguments"));
        cmdClassMap.put("read", Class.forName("org.apache.pulsar.testclient.PerformanceReader$Arguments"));
        cmdClassMap.put("monitor-brokers", Class.forName("org.apache.pulsar.testclient.BrokerMonitor$Arguments"));
        cmdClassMap.put("simulation-client",
                Class.forName("org.apache.pulsar.testclient.LoadSimulationClient$MainArguments"));
        cmdClassMap.put("simulation-controller",
                Class.forName("org.apache.pulsar.testclient.LoadSimulationController$MainArguments"));
        cmdClassMap.put("websocket-producer",
                Class.forName("org.apache.pulsar.proxy.socket.client.PerformanceClient$Arguments"));
        cmdClassMap.put("managed-ledger", Class.forName("org.apache.pulsar.testclient.ManagedLedgerWriter$Arguments"));

        for (Map.Entry<String, Class<?>> entry : cmdClassMap.entrySet()) {
            String cmd = entry.getKey();
            Class<?> clazz = entry.getValue();
            Constructor<?> constructor = clazz.getDeclaredConstructor();
            constructor.setAccessible(true);
            commander.addSubcommand(cmd, constructor.newInstance());
        }

        if (arguments.commandNames.size() == 0) {
            for (Map.Entry<String, CommandLine> cmd : commander.getSubcommands().entrySet()) {
                generateDocument(cmd.getKey(), commander);
            }
        } else {
            for (String commandName : arguments.commandNames) {
                generateDocument(commandName, commander);
            }
        }
    }

    private static String generateDocument(String module, CommandLine parentCmd) {
        StringBuilder sb = new StringBuilder();
        CommandLine cmd = parentCmd.getSubcommands().get(module);
        sb.append("## ").append(module).append("\n\n");
        sb.append(cmd.getCommandName()).append("\n");
        sb.append("\n\n```shell\n")
                .append("$ pulsar-perf ").append(module).append(" [options]")
                .append("\n```");
        sb.append("\n\n");
        sb.append("|Flag|Description|Default|\n");
        sb.append("|---|---|---|\n");
        List<CommandLine.Model.OptionSpec> options = cmd.getCommandSpec().options();
        options.stream().filter(ele -> !ele.hidden()).forEach((option) ->
                sb.append("| `").append(String.join(", ", option.names()))
                        .append("` | ").append(option.description()[0].replace("\n", " "))
                        .append("|").append(option.defaultValueString()).append("|\n")
        );
        System.out.println(sb.toString());
        return sb.toString();
    }
}
