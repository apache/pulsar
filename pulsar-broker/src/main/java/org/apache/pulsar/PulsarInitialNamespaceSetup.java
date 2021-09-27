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
package org.apache.pulsar;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import java.util.List;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.util.CmdGenerateDocs;
import org.apache.pulsar.metadata.api.MetadataStore;

/**
 * Setup the initial namespace of the cluster without startup the Pulsar broker.
 */
public class PulsarInitialNamespaceSetup {

    private static class Arguments {

        @Parameter(names = { "-c", "--cluster" }, description = "Cluster name", required = true)
        private String cluster;

        @Parameter(names = { "-cs",
                "--configuration-store" }, description = "Configuration Store connection string", required = true)
        private String configurationStore;

        @Parameter(names = {
                "--zookeeper-session-timeout-ms"
        }, description = "Local zookeeper session timeout ms")
        private int zkSessionTimeoutMillis = 30000;

        @Parameter(description = "tenant/namespace", required = true)
        private List<String> namespaces;

        @Parameter(names = { "-h", "--help" }, description = "Show this help message")
        private boolean help = false;

        @Parameter(names = {"-g", "--generate-docs"}, description = "Generate docs")
        private boolean generateDocs = false;
    }

    public static int doMain(String[] args) throws Exception {
        Arguments arguments = new Arguments();
        JCommander jcommander = new JCommander();
        try {
            jcommander.addObject(arguments);
            jcommander.parse(args);
            if (arguments.help) {
                jcommander.usage();
                return 0;
            }
            if (arguments.generateDocs) {
                CmdGenerateDocs cmd = new CmdGenerateDocs("pulsar");
                cmd.addCommand("initialize-namespace", arguments);
                cmd.run(null);
                return 0;
            }
        } catch (Exception e) {
            jcommander.usage();
            return 1;
        }

        if (arguments.configurationStore == null) {
            System.err.println("Configuration store address argument is required (--configuration-store)");
            jcommander.usage();
            return 1;
        }

        try (MetadataStore configStore = PulsarClusterMetadataSetup
                .initMetadataStore(arguments.configurationStore, arguments.zkSessionTimeoutMillis)) {
            PulsarResources pulsarResources = new PulsarResources(null, configStore);
            for (String namespace : arguments.namespaces) {
                NamespaceName namespaceName = null;
                try {
                    namespaceName = NamespaceName.get(namespace);
                } catch (Exception e) {
                    System.out.println("Invalid namespace name.");
                    return 1;
                }

                // Create specified tenant
                PulsarClusterMetadataSetup
                        .createTenantIfAbsent(pulsarResources, namespaceName.getTenant(), arguments.cluster);

                // Create specified namespace
                PulsarClusterMetadataSetup.createNamespaceIfAbsent(pulsarResources, namespaceName,
                        arguments.cluster);
            }
        }

        System.out.println("Initial namespace setup success");
        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(doMain(args));
    }
}
