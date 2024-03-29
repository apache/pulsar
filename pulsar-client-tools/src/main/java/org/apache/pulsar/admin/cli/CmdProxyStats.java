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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.util.function.Supplier;
import org.apache.pulsar.client.admin.PulsarAdmin;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(description = "Operations to collect Proxy statistics")
public class CmdProxyStats extends CmdBase {

    @Command(description = "dump connections metrics for Monitoring")
    private class CmdConnectionMetrics extends CliCommand {
        @Option(names = {"-i", "--indent"}, description = "Indent JSON output", required = false)
        private boolean indent = false;

        @Override
        void run() throws Exception {
            String json = getAdmin().proxyStats().getConnections();
            JsonArray stats = new Gson().fromJson(json, JsonArray.class);
            printStats(stats, indent);
        }
    }

    @Command(description = "dump topics metrics for Monitoring")
    private class CmdTopicsMetrics extends CliCommand {
        @Option(names = {"-i", "--indent"}, description = "Indent JSON output", required = false)
        private boolean indent = false;

        @Override
        void run() throws Exception {
            String json = getAdmin().proxyStats().getTopics();
            JsonObject stats = new Gson().fromJson(json, JsonObject.class);
            printStats(stats, indent);
        }
    }

    public void printStats(JsonElement json, boolean indent) throws IOException {
        GsonBuilder builder = new GsonBuilder();
        Gson gson = indent ? builder.setPrettyPrinting().create() : builder.create();
        System.out.println(gson.toJson(json));
    }

    public CmdProxyStats(Supplier<PulsarAdmin> admin) {
        super("proxy-stats", admin);
        addCommand("connections", new CmdConnectionMetrics());
        addCommand("topics", new CmdTopicsMetrics());
    }
}