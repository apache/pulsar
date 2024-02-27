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
package org.apache.pulsar.admin.cli;

import java.util.function.Supplier;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.naming.TopicVersion;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(description = "Operations about brokers")
public class CmdBrokers extends CmdBase {

    @Command(description = "List active brokers of the cluster")
    private class List extends CliCommand {
        @Parameters(description = "cluster-name", arity = "1")
        private String cluster;

        @Override
        void run() throws Exception {
            print(getAdmin().brokers().getActiveBrokers(cluster));
        }
    }

    @Command(description = "Get the information of the leader broker")
    private class LeaderBroker extends CliCommand {

        @Override
        void run() throws Exception {
            print(getAdmin().brokers().getLeaderBroker());
        }
    }

    @Command(description = "List namespaces owned by the broker")
    private class Namespaces extends CliCommand {
        @Parameters(description = "cluster-name", arity = "1")
        private String cluster;

        @Option(names = {"-u", "--url"}, description = "broker-url", required = true)
        private String brokerUrl;

        @Override
        void run() throws Exception {
            print(getAdmin().brokers().getOwnedNamespaces(cluster, brokerUrl));
        }
    }

    @Command(description = "Update dynamic-serviceConfiguration of broker")
    private class UpdateConfigurationCmd extends CliCommand {
        @Option(names = {"-c", "--config"}, description = "service-configuration name", required = true)
        private String configName;
        @Option(names = {"-v", "--value"}, description = "service-configuration value", required = true)
        private String configValue;

        @Override
        void run() throws Exception {
            getAdmin().brokers().updateDynamicConfiguration(configName, configValue);
        }
    }

    @Command(description = "Delete dynamic-serviceConfiguration of broker")
    private class DeleteConfigurationCmd extends CliCommand {
        @Option(names = {"-c", "--config"}, description = "service-configuration name", required = true)
        private String configName;

        @Override
        void run() throws Exception {
            getAdmin().brokers().deleteDynamicConfiguration(configName);
        }
    }

    @Command(description = "Get all overridden dynamic-configuration values")
    private class GetAllConfigurationsCmd extends CliCommand {

        @Override
        void run() throws Exception {
            print(getAdmin().brokers().getAllDynamicConfigurations());
        }
    }

    @Command(description = "Get list of updatable configuration name")
    private class GetUpdatableConfigCmd extends CliCommand {

        @Override
        void run() throws Exception {
            print(getAdmin().brokers().getDynamicConfigurationNames());
        }
    }

    @Command(description = "Get runtime configuration values")
    private class GetRuntimeConfigCmd extends CliCommand {

        @Override
        void run() throws Exception {
            print(getAdmin().brokers().getRuntimeConfigurations());
        }
    }

    @Command(description = "Get internal configuration information")
    private class GetInternalConfigurationCmd extends CliCommand {

        @Override
        void run() throws Exception {
            print(getAdmin().brokers().getInternalConfigurationData());
        }

    }

    @Command(description = "Run a health check against the broker")
    private class HealthcheckCmd extends CliCommand {

        @Option(names = {"-tv", "--topic-version"}, description = "topic version V1 is default")
        private TopicVersion topicVersion;

        @Override
        void run() throws Exception {
            getAdmin().brokers().healthcheck(topicVersion);
            System.out.println("ok");
        }

    }

    @Command(description = "Shutdown broker gracefully.")
    private class ShutDownBrokerGracefully extends CliCommand {

        @Option(names = {"--max-concurrent-unload-per-sec", "-m"},
                description = "Max concurrent unload per second, "
                        + "if the value absent(value=0) means no concurrent limitation")
        private int maxConcurrentUnloadPerSec;

        @Option(names = {"--forced-terminate-topic", "-f"}, description = "Force terminate all topics on Broker")
        private boolean forcedTerminateTopic;

        @Override
        void run() throws Exception {
            sync(() -> getAdmin().brokers().shutDownBrokerGracefully(maxConcurrentUnloadPerSec, forcedTerminateTopic));
            System.out.println("Successfully shutdown broker gracefully");
        }

    }

    @Command(description = "Manually trigger backlogQuotaCheck")
    private class BacklogQuotaCheckCmd extends CliCommand {

        @Override
        void run() throws Exception {
            getAdmin().brokers().backlogQuotaCheckAsync();
            System.out.println("ok");
        }

    }

    @Command(description = "Get the version of the currently connected broker")
    private class PulsarVersion extends CliCommand {

        @Override
        void run() throws Exception {
            System.out.println(getAdmin().brokers().getVersion());
        }
    }

    public CmdBrokers(Supplier<PulsarAdmin> admin) {
        super("brokers", admin);
        addCommand("list", new List());
        addCommand("leader-broker", new LeaderBroker());
        addCommand("namespaces", new Namespaces());
        addCommand("update-dynamic-config", new UpdateConfigurationCmd());
        addCommand("delete-dynamic-config", new DeleteConfigurationCmd());
        addCommand("list-dynamic-config", new GetUpdatableConfigCmd());
        addCommand("get-all-dynamic-config", new GetAllConfigurationsCmd());
        addCommand("get-internal-config", new GetInternalConfigurationCmd());
        addCommand("get-runtime-config", new GetRuntimeConfigCmd());
        addCommand("healthcheck", new HealthcheckCmd());
        addCommand("backlog-quota-check", new BacklogQuotaCheckCmd());
        addCommand("version", new PulsarVersion());
        addCommand("shutdown", new ShutDownBrokerGracefully());
    }
}
