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
import org.apache.pulsar.proxy.socket.client.PerformanceClient;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

@Slf4j
@Command(name = "gen-doc", description = "Generate documentation automatically.")
public class CmdGenerateDocumentation extends CmdBase{

    @Option(names = {"-n", "--command-names"}, description = "List of command names")
    private List<String> commandNames = new ArrayList<>();

    public CmdGenerateDocumentation() {
        super("gen-doc");
    }

    @Spec
    CommandSpec spec;

    @Override
    public void run() throws Exception {
        CommandLine commander = spec.commandLine();

        Map<String, Class<?>> cmdClassMap = new LinkedHashMap<>();
        cmdClassMap.put("produce", PerformanceProducer.class);
        cmdClassMap.put("consume", PerformanceConsumer.class);
        cmdClassMap.put("transaction", PerformanceTransaction.class);
        cmdClassMap.put("read", PerformanceReader.class);
        cmdClassMap.put("monitor-brokers", BrokerMonitor.class);
        cmdClassMap.put("simulation-client", LoadSimulationClient.class);
        cmdClassMap.put("simulation-controller", LoadSimulationController.class);
        cmdClassMap.put("websocket-producer", PerformanceClient.class);
        cmdClassMap.put("managed-ledger", ManagedLedgerWriter.class);

        for (Map.Entry<String, Class<?>> entry : cmdClassMap.entrySet()) {
            String cmd = entry.getKey();
            Class<?> clazz = entry.getValue();
            Constructor<?> constructor = clazz.getDeclaredConstructor();
            constructor.setAccessible(true);
            commander.addSubcommand(cmd, constructor.newInstance());
        }

        if (this.commandNames.size() == 0) {
            for (Map.Entry<String, CommandLine> cmd : commander.getSubcommands().entrySet()) {
                generateDocument(cmd.getKey(), commander);
            }
        } else {
            for (String commandName : this.commandNames) {
                generateDocument(commandName, commander);
            }
        }
    }

    private static String generateDocument(String module, CommandLine parentCmd) {
        StringBuilder sb = new StringBuilder();
        CommandLine cmd = parentCmd.getSubcommands().get(module);
        sb.append("## ").append(module).append("\n\n");
        sb.append(getCommandDescription(cmd)).append("\n");
        sb.append("\n\n```shell\n")
                .append("$ pulsar-perf ").append(module).append(" [options]")
                .append("\n```");
        sb.append("\n\n");
        sb.append("|Flag|Description|Default|\n");
        sb.append("|---|---|---|\n");
        List<CommandLine.Model.OptionSpec> options = cmd.getCommandSpec().options();
        options.stream().filter(ele -> !ele.hidden()).forEach((option) ->
                sb.append("| `").append(String.join(", ", option.names()))
                        .append("` | ").append(getOptionDescription(option).replace("\n", " "))
                        .append("|").append(option.defaultValueString()).append("|\n")
        );
        System.out.println(sb.toString());
        return sb.toString();
    }

    public static String getCommandDescription(CommandLine commandLine) {
        String[] description = commandLine.getCommandSpec().usageMessage().description();
        if (description != null && description.length != 0) {
            return description[0];
        }
        return "";
    }

    public static String getOptionDescription(CommandLine.Model.OptionSpec optionSpec) {
        String[] description = optionSpec.description();
        if (description != null && description.length != 0) {
            return description[0];
        }
        return "";
    }
}
