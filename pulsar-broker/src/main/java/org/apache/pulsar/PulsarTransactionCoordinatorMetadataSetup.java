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
package org.apache.pulsar;

import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.docs.tools.CmdGenerateDocs;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ScopeType;

/**
 * Set up the transaction coordinator metadata for a cluster, the setup will create pulsar/system namespace and create
 * partitioned topic for transaction coordinator assign.
 */
public class PulsarTransactionCoordinatorMetadataSetup {

    @Command(name = "initialize-transaction-coordinator-metadata", showDefaultValues = true, scope = ScopeType.INHERIT)
    private static class Arguments {

        @Option(names = { "-c", "--cluster" }, description = "Cluster name", required = true)
        private String cluster;

        @Option(names = { "-cs",
                "--configuration-store" }, description = "Configuration Store connection string", required = true)
        private String configurationStore;

        @Option(names = {"-cmscp",
                "--configuration-metadata-store-config-path"}, description = "Configuration Metadata Store config path",
                hidden = false)
        private String configurationStoreConfigPath;

        @Option(names = {
                "--zookeeper-session-timeout-ms"
        }, description = "Local zookeeper session timeout ms")
        private int zkSessionTimeoutMillis = 30000;

        @Option(names = {
                "--initial-num-transaction-coordinators"
        }, description = "Num transaction coordinators will assigned in cluster")
        private int numTransactionCoordinators = 16;

        @Option(names = { "-h", "--help" }, description = "Show this help message")
        private boolean help = false;

        @Option(names = {"-g", "--generate-docs"}, description = "Generate docs")
        private boolean generateDocs = false;
    }

    public static void main(String[] args) throws Exception {
        Arguments arguments = new Arguments();
        CommandLine commander = new CommandLine(arguments);
        try {
            commander.parseArgs(args);
            if (arguments.help) {
                commander.usage(commander.getOut());
                return;
            }
            if (arguments.generateDocs) {
                CmdGenerateDocs cmd = new CmdGenerateDocs("pulsar");
                cmd.addCommand("initialize-transaction-coordinator-metadata", commander);
                cmd.run(null);
                return;
            }
        } catch (Exception e) {
            commander.usage(commander.getOut());
            throw e;
        }

        if (arguments.configurationStore == null) {
            System.err.println("Configuration store address argument is required (--configuration-store)");
            commander.usage(commander.getOut());
            System.exit(1);
        }

        if (arguments.numTransactionCoordinators <= 0) {
            System.err.println("Number of transaction coordinators must greater than 0");
            System.exit(1);
        }

        try (MetadataStoreExtended configStore = PulsarClusterMetadataSetup.initConfigMetadataStore(
                arguments.configurationStore,
                arguments.configurationStoreConfigPath,
                arguments.zkSessionTimeoutMillis)) {
            PulsarResources pulsarResources = new PulsarResources(null, configStore);
            // Create system tenant
            PulsarClusterMetadataSetup
                    .createTenantIfAbsent(pulsarResources, NamespaceName.SYSTEM_NAMESPACE.getTenant(),
                            arguments.cluster);

            // Create system namespace
            PulsarClusterMetadataSetup.createNamespaceIfAbsent(pulsarResources, NamespaceName.SYSTEM_NAMESPACE,
                    arguments.cluster);

            // Create transaction coordinator assign partitioned topic
            PulsarClusterMetadataSetup.createPartitionedTopic(configStore,
                    SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN,
                    arguments.numTransactionCoordinators);
        }

        System.out.println("Transaction coordinator metadata setup success");
    }
}
