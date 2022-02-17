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
import java.util.function.Supplier;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.naming.TopicVersion;

@Parameters(commandDescription = "Operations about brokers")
public class CmdBrokers extends CmdBase {

    @Parameters(commandDescription = "List active brokers of the cluster")
    private class List extends CliCommand {
        @Parameter(description = "cluster-name", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws Exception {
            String cluster = getOneArgument(params);
            print(getAdmin().brokers().getActiveBrokers(cluster));
        }
    }

    @Parameters(commandDescription = "Get the information of the leader broker")
    private class LeaderBroker extends CliCommand {

        @Override
        void run() throws Exception {
            print(getAdmin().brokers().getLeaderBroker());
        }
    }

    @Parameters(commandDescription = "List namespaces owned by the broker")
    private class Namespaces extends CliCommand {
        @Parameter(description = "cluster-name", required = true)
        private java.util.List<String> params;
        @Parameter(names = "--url", description = "broker-url", required = true)
        private String brokerUrl;

        @Override
        void run() throws Exception {
            String cluster = getOneArgument(params);
            print(getAdmin().brokers().getOwnedNamespaces(cluster, brokerUrl));
        }
    }

    @Parameters(commandDescription = "Update dynamic-serviceConfiguration of broker")
    private class UpdateConfigurationCmd extends CliCommand {
        @Parameter(names = "--config", description = "service-configuration name", required = true)
        private String configName;
        @Parameter(names = "--value", description = "service-configuration value", required = true)
        private String configValue;

        @Override
        void run() throws Exception {
            getAdmin().brokers().updateDynamicConfiguration(configName, configValue);
        }
    }

    @Parameters(commandDescription = "Delete dynamic-serviceConfiguration of broker")
    private class DeleteConfigurationCmd extends CliCommand {
        @Parameter(names = "--config", description = "service-configuration name", required = true)
        private String configName;

        @Override
        void run() throws Exception {
            getAdmin().brokers().deleteDynamicConfiguration(configName);
        }
    }

    @Parameters(commandDescription = "Get all overridden dynamic-configuration values")
    private class GetAllConfigurationsCmd extends CliCommand {

        @Override
        void run() throws Exception {
            print(getAdmin().brokers().getAllDynamicConfigurations());
        }
    }

    @Parameters(commandDescription = "Get list of updatable configuration name")
    private class GetUpdatableConfigCmd extends CliCommand {

        @Override
        void run() throws Exception {
            print(getAdmin().brokers().getDynamicConfigurationNames());
        }
    }

    @Parameters(commandDescription = "Get runtime configuration values")
    private class GetRuntimeConfigCmd extends CliCommand {

        @Override
        void run() throws Exception {
            print(getAdmin().brokers().getRuntimeConfigurations());
        }
    }

    @Parameters(commandDescription = "Get internal configuration information")
    private class GetInternalConfigurationCmd extends CliCommand {

        @Override
        void run() throws Exception {
            print(getAdmin().brokers().getInternalConfigurationData());
        }

    }

    @Parameters(commandDescription = "Run a health check against the broker")
    private class HealthcheckCmd extends CliCommand {

        @Parameter(names = "--topic-version", description = "topic version V1 is default")
        private TopicVersion topicVersion;

        @Override
        void run() throws Exception {
            getAdmin().brokers().healthcheck(topicVersion);
            System.out.println("ok");
        }

    }

    @Parameters(commandDescription = "Shutdown broker gracefully.")
    private class ShutDownBrokerGracefully extends CliCommand {

        @Parameter(names = {"--max-concurrent-unload-per-sec", "-m"},
                description = "Max concurrent unload per second, "
                        + "if the value absent(value=0) means no concurrent limitation")
        private int maxConcurrentUnloadPerSec;

        @Parameter(names = {"--forced-terminate-topic", "-f"}, description = "Force terminate all topics on Broker")
        private boolean forcedTerminateTopic;

        @Override
        void run() throws Exception {
            getAdmin().brokers().shutDownBrokerGracefully(maxConcurrentUnloadPerSec, forcedTerminateTopic);
            System.out.println("Successfully trigger broker shutdown gracefully");
        }

    }

    @Parameters(commandDescription = "Manually trigger backlogQuotaCheck")
    private class BacklogQuotaCheckCmd extends CliCommand {

        @Override
        void run() throws Exception {
            getAdmin().brokers().backlogQuotaCheckAsync();
            System.out.println("ok");
        }

    }

    @Parameters(commandDescription = "Get the version of the currently connected broker")
    private class PulsarVersion extends CliCommand {

        @Override
        void run() throws Exception {
            System.out.println(getAdmin().brokers().getVersion());
        }
    }

    public CmdBrokers(Supplier<PulsarAdmin> admin) {
        super("brokers", admin);
        jcommander.addCommand("list", new List());
        jcommander.addCommand("leader-broker", new LeaderBroker());
        jcommander.addCommand("namespaces", new Namespaces());
        jcommander.addCommand("update-dynamic-config", new UpdateConfigurationCmd());
        jcommander.addCommand("delete-dynamic-config", new DeleteConfigurationCmd());
        jcommander.addCommand("list-dynamic-config", new GetUpdatableConfigCmd());
        jcommander.addCommand("get-all-dynamic-config", new GetAllConfigurationsCmd());
        jcommander.addCommand("get-internal-config", new GetInternalConfigurationCmd());
        jcommander.addCommand("get-runtime-config", new GetRuntimeConfigCmd());
        jcommander.addCommand("healthcheck", new HealthcheckCmd());
        jcommander.addCommand("backlog-quota-check", new BacklogQuotaCheckCmd());
        jcommander.addCommand("version", new PulsarVersion());
        jcommander.addCommand("shutdown", new ShutDownBrokerGracefully());
    }
}
