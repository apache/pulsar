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

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import java.io.FileInputStream;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.pulsar.proxy.socket.client.PerformanceClient;
import picocli.CommandLine;

@CommandLine.Command(name = "pulsar-perf",
        scope = CommandLine.ScopeType.INHERIT,
        mixinStandardHelpOptions = true,
        showDefaultValues = true
)
public class PulsarPerfTestTool {

    protected Map<String, Class<?>> commandMap;
    protected final CommandLine commander;

    public PulsarPerfTestTool() {
        this.commander = new CommandLine(this);
        commandMap = new HashMap<>();
    }

    private String[] initCommander(String[] args) throws Exception {
        commandMap.put("produce", PerformanceProducer.class);
        commandMap.put("consume", PerformanceConsumer.class);
        commandMap.put("transaction", PerformanceTransaction.class);
        commandMap.put("read", PerformanceReader.class);
        commandMap.put("monitor-brokers", BrokerMonitor.class);
        commandMap.put("simulation-client", LoadSimulationClient.class);
        commandMap.put("simulation-controller", LoadSimulationController.class);
        commandMap.put("websocket-producer", PerformanceClient.class);
        commandMap.put("managed-ledger", ManagedLedgerWriter.class);
        commandMap.put("gen-doc", CmdGenerateDocumentation.class);
        if (args.length == 0) {
            System.out.println("Usage: pulsar-perf CONF_FILE_PATH [options] [command] [command options]");
            PerfClientUtils.exit(0);
        }
        String configFile = args[0];
        Properties prop = new Properties(System.getProperties());
        if (configFile != null) {
            try (FileInputStream fis = new FileInputStream(configFile)) {
                prop.load(fis);
            }
        }
        commander.setDefaultValueProvider(PulsarPerfTestPropertiesProvider.create(prop));

        for (Map.Entry<String, Class<?>> c : commandMap.entrySet()) {
            Constructor<?> constructor = c.getValue().getDeclaredConstructor();
            constructor.setAccessible(true);
            addCommand(c.getKey(), constructor.newInstance());
        }

        // Remove the first argument, it's the config file path
        return Arrays.copyOfRange(args, 1, args.length);
    }

    private void addCommand(String name, Object o) {
        if (o instanceof CmdBase) {
            commander.addSubcommand(name, ((CmdBase) o).getCommander());
        } else {
            commander.addSubcommand(o);
        }
    }

    public static void main(String[] args) throws Exception {
        PulsarPerfTestTool tool = new PulsarPerfTestTool();
        args = tool.initCommander(args);

        if (tool.run(args)) {
            PerfClientUtils.exit(0);
        } else {
            PerfClientUtils.exit(1);
        }
    }

    protected boolean run(String[] args) {
        return commander.execute(args) == 0;
    }

}

class PulsarPerfTestPropertiesProvider extends CommandLine.PropertiesDefaultProvider{
    private static final String brokerServiceUrlKey = "brokerServiceUrl";
    private static final String webServiceUrlKey = "webServiceUrl";
    private final Properties properties;

    public PulsarPerfTestPropertiesProvider(Properties properties) {
        super(properties);
        this.properties = properties;
    }

    static PulsarPerfTestPropertiesProvider create(Properties properties) {
        if (isBlank(properties.getProperty(brokerServiceUrlKey))) {
            String webServiceUrl = properties.getProperty("webServiceUrl");
            if (isNotBlank(webServiceUrl)) {
                properties.put(brokerServiceUrlKey, webServiceUrl);
            } else if (isNotBlank(properties.getProperty("serviceUrl"))) {
                properties.put(brokerServiceUrlKey, properties.getProperty("serviceUrl", "http://localhost:8080/"));
            }
        }

        // Used for produce and transaction to fill parameters.
        if (isBlank(properties.getProperty(webServiceUrlKey))) {
            properties.put(webServiceUrlKey, properties.getProperty("adminURL", "http://localhost:8080/"));
        }

        return new PulsarPerfTestPropertiesProvider(properties);
    }
}
