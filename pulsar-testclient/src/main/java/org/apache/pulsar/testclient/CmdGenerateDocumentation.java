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
package org.apache.pulsar.testclient;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterDescription;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CmdGenerateDocumentation {

    @Parameters(commandDescription = "Generate documentation automatically.")
    static class Arguments {

        @Parameter(names = {"-h", "--help"}, description = "Help message", help = true)
        boolean help;

        @Parameter(names = {"-n", "--command-names"}, description = "List of command names")
        private List<String> commandNames = new ArrayList<>();

    }

    public static void main(String[] args) throws Exception {
        final Arguments arguments = new Arguments();
        final JCommander jc = new JCommander(arguments);
        jc.setProgramName("pulsar-perf gen-doc");
        try {
            jc.parse(args);
        } catch (ParameterException e) {
            System.out.println(e.getMessage());
            jc.usage();
            PerfClientUtils.exit(-1);
        }

        if (arguments.help) {
            jc.usage();
            PerfClientUtils.exit(-1);
        }

        Map<String, Class<?>> cmdClassMap = new LinkedHashMap<>();
        cmdClassMap.put("produce", Class.forName("org.apache.pulsar.testclient.PerformanceProducer$Arguments"));
        cmdClassMap.put("consume", Class.forName("org.apache.pulsar.testclient.PerformanceConsumer$Arguments"));
        cmdClassMap.put("read", Class.forName("org.apache.pulsar.testclient.PerformanceReader$Arguments"));
        cmdClassMap.put("monitor-brokers", Class.forName("org.apache.pulsar.testclient.BrokerMonitor$Arguments"));
        cmdClassMap.put("simulation-client", Class.forName("org.apache.pulsar.testclient.LoadSimulationClient$MainArguments"));
        cmdClassMap.put("simulation-controller", Class.forName("org.apache.pulsar.testclient.LoadSimulationController$MainArguments"));
        cmdClassMap.put("websocket-producer", Class.forName("org.apache.pulsar.proxy.socket.client.PerformanceClient$Arguments"));
        cmdClassMap.put("managed-ledger", Class.forName("org.apache.pulsar.testclient.ManagedLedgerWriter$Arguments"));

        for (Map.Entry<String, Class<?>> entry : cmdClassMap.entrySet()) {
            String cmd = entry.getKey();
            Class<?> clazz = entry.getValue();
            Constructor<?> constructor = clazz.getDeclaredConstructor();
            constructor.setAccessible(true);
            jc.addCommand(cmd, constructor.newInstance());
        }

        if (arguments.commandNames.size() == 0) {
            for (Map.Entry<String, JCommander> cmd : jc.getCommands().entrySet()) {
                generateDocument(cmd.getKey(), jc);
            }
        } else {
            for (String commandName : arguments.commandNames) {
                generateDocument(commandName, jc);
            }
        }
    }

    private static String generateDocument(String module, JCommander parentCmd) {
        StringBuilder sb = new StringBuilder();
        JCommander cmd = parentCmd.getCommands().get(module);
        sb.append("------------\n\n");
        sb.append("# ").append(module).append("\n\n");
        sb.append("### Usage\n\n");
        sb.append("`$").append(module).append("`\n\n");
        sb.append("------------\n\n");
        sb.append(parentCmd.getUsageFormatter().getCommandDescription(module)).append("\n");
        sb.append("\n\n```bdocs-tab:example_shell\n")
                .append("$ pulsar-perf ").append(module).append(" [options]")
                .append("\n```");
        sb.append("\n\n");
        for (String s : cmd.getCommands().keySet()) {
            sb.append("* `").append(s).append("`\n");
        }
        sb.append("|Flag|Description|Default|\n");
        sb.append("|---|---|---|\n");
        List<ParameterDescription> options = cmd.getParameters();
        options.forEach((option) ->
                sb.append("| `").append(option.getNames())
                        .append("` | ").append(option.getDescription().replace("\n", " "))
                        .append("|").append(option.getDefault()).append("|\n")
        );
        System.out.println(sb.toString());
        return sb.toString();
    }
}
