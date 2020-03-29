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

import org.apache.pulsar.client.admin.PulsarAdmin;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

@Parameters(commandDescription = "Operations about brokers")
public class CmdBrokers extends CmdBase {

    @Parameters(commandDescription = "List active brokers of the cluster")
    private class List extends CliCommand {
        @Parameter(description = "cluster-name\n", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws Exception {
            String cluster = getOneArgument(params);
            print(admin.brokers().getActiveBrokers(cluster));
        }
    }

    @Parameters(commandDescription = "List namespaces owned by the broker")
    private class Namespaces extends CliCommand {
        @Parameter(description = "cluster-name\n", required = true)
        private java.util.List<String> params;
        @Parameter(names = "--url", description = "broker-url\n", required = true)
        private String brokerUrl;

        @Override
        void run() throws Exception {
            String cluster = getOneArgument(params);
            print(admin.brokers().getOwnedNamespaces(cluster, brokerUrl));
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
            admin.brokers().updateDynamicConfiguration(configName, configValue);
        }
    }

    @Parameters(commandDescription = "Delete dynamic-serviceConfiguration of broker")
    private class DeleteConfigurationCmd extends CliCommand {
        @Parameter(names = "--config", description = "service-configuration name", required = true)
        private String configName;

        @Override
        void run() throws Exception {
            admin.brokers().deleteDynamicConfiguration(configName);
        }
    }
    
    @Parameters(commandDescription = "Get all overridden dynamic-configuration values")
    private class GetAllConfigurationsCmd extends CliCommand {

        @Override
        void run() throws Exception {
            print(admin.brokers().getAllDynamicConfigurations());
        }
    }
    
    @Parameters(commandDescription = "Get list of updatable configuration name")
    private class GetUpdatableConfigCmd extends CliCommand {

        @Override
        void run() throws Exception {
            print(admin.brokers().getDynamicConfigurationNames());
        }
    }

    @Parameters(commandDescription = "Get runtime configuration values")
    private class GetRuntimeConfigCmd extends CliCommand {

        @Override
        void run() throws Exception {
            print(admin.brokers().getRuntimeConfigurations());
        }
    }

    @Parameters(commandDescription = "Get internal configuration information")
    private class GetInternalConfigurationCmd extends CliCommand {

        @Override
        void run() throws Exception {
            print(admin.brokers().getInternalConfigurationData());
        }

    }

    @Parameters(commandDescription = "Run a health check against the broker")
    private class HealthcheckCmd extends CliCommand {

        @Override
        void run() throws Exception {
            admin.brokers().healthcheck();
            System.out.println("ok");
        }

    }

    public CmdBrokers(PulsarAdmin admin) {
        super("brokers", admin);
        jcommander.addCommand("list", new List());
        jcommander.addCommand("namespaces", new Namespaces());
        jcommander.addCommand("update-dynamic-config", new UpdateConfigurationCmd());
        jcommander.addCommand("delete-dynamic-config", new DeleteConfigurationCmd());
        jcommander.addCommand("list-dynamic-config", new GetUpdatableConfigCmd());
        jcommander.addCommand("get-all-dynamic-config", new GetAllConfigurationsCmd());
        jcommander.addCommand("get-internal-config", new GetInternalConfigurationCmd());
        jcommander.addCommand("get-runtime-config", new GetRuntimeConfigCmd());
        jcommander.addCommand("healthcheck", new HealthcheckCmd());
    }
}
