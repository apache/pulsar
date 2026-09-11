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
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * CLI commands for metadata store migration operations.
 */
@Command(description = "Operations for metadata store migration")
public class CmdMetadataMigration extends CmdBase {

    public CmdMetadataMigration(Supplier<PulsarAdmin> admin) {
        super("metadata-migration", admin);
        addCommand("start", new CmdMetadataMigration.Start());
        addCommand("status", new CmdMetadataMigration.Status());
    }

    @Command(description = "Start metadata store migration to target")
    private class Start extends CliCommand {
        @Option(names = {"--target"}, description = "Target metadata store URL (e.g., oxia://host:port/namespace)",
                required = true)
        private String targetUrl;

        @Override
        void run() throws Exception {
            print("Starting metadata store migration");
            print("Target: " + targetUrl);
            print("");

            // Extract store type from URL
            getAdmin().metadataMigration().start(targetUrl).get();

            print("");
            print("✓ Migration started");
            print("");
            print("Monitor progress: pulsar-admin metadata-migration status");
        }
    }

    @Command(description = "Check migration status")
    private class Status extends CliCommand {
        @Override
        void run() throws Exception {
            print(getAdmin().metadataMigration().status().get());
        }
    }
}
