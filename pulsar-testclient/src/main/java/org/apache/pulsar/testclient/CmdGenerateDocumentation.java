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

import com.google.common.annotations.VisibleForTesting;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.CustomLog;
import org.apache.pulsar.cli.ClientApiOptionGroups;
import org.apache.pulsar.proxy.socket.client.PerformanceClient;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.OptionSpec;
import picocli.CommandLine.Option;

@CustomLog
@Command(name = "gen-doc", description = "Generate documentation automatically.")
public class CmdGenerateDocumentation extends CmdBase{

    @Option(names = {"-n", "--command-names"}, description = "List of command names")
    private List<String> commandNames = new ArrayList<>();

    public CmdGenerateDocumentation() {
        super("gen-doc");
    }

    @Override
    public void run() throws Exception {
        CommandLine commander = getCommander();

        Map<String, Class<?>> cmdClassMap = new LinkedHashMap<>();
        cmdClassMap.put("produce", PerformanceProducer.class);
        cmdClassMap.put("consume", PerformanceConsumer.class);
        cmdClassMap.put("transaction", PerformanceTransaction.class);
        cmdClassMap.put("read", PerformanceReader.class);
        cmdClassMap.put("monitor-brokers", BrokerMonitor.class);
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

    @VisibleForTesting
    static String generateDocument(String module, CommandLine parentCmd) {
        StringBuilder sb = new StringBuilder();
        CommandLine cmd = parentCmd.getSubcommands().get(module);
        sb.append("## ").append(module).append("\n\n");
        sb.append(getCommandDescription(cmd)).append("\n");
        String[] description = cmd.getCommandSpec().usageMessage().description();
        for (int i = 1; description != null && i < description.length; i++) {
            sb.append("\n").append(String.format(description[i]).trim()).append("\n");
        }
        sb.append("\n\n```shell\n")
                .append("$ pulsar-perf ").append(module).append(" [options]")
                .append("\n```");
        sb.append("\n\n");
        // Options in an @ArgGroup with a heading (such as the client-specific options of produce/consume)
        // get their own table, so the generated docs have the same sections as --help.
        Map<String, List<OptionSpec>> sections = new LinkedHashMap<>();
        for (OptionSpec option : cmd.getCommandSpec().options()) {
            if (!option.hidden()) {
                sections.computeIfAbsent(ClientApiOptionGroups.sectionHeading(option), k -> new ArrayList<>())
                        .add(option);
            }
        }
        boolean singleSection = sections.size() == 1 && sections.containsKey(null);
        for (Map.Entry<String, List<OptionSpec>> section : sections.entrySet()) {
            if (!singleSection) {
                String heading = section.getKey() != null ? section.getKey()
                        : String.format(cmd.getCommandSpec().usageMessage().optionListHeading()).trim();
                if (heading.isEmpty()) {
                    heading = "Options";
                }
                sb.append("### ").append(heading.endsWith(":") ? heading.substring(0, heading.length() - 1)
                        : heading).append("\n\n");
            }
            sb.append("|Flag|Description|Default|\n");
            sb.append("|---|---|---|\n");
            section.getValue().forEach((option) ->
                    sb.append("| `").append(String.join(", ", option.names()))
                            .append("` | ").append(getOptionDescription(option).replace("\n", " "))
                            .append("|").append(option.defaultValueString()).append("|\n")
            );
            sb.append("\n");
        }
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
