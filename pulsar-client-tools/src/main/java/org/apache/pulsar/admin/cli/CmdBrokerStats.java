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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonWriter;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.stats.AllocatorStats;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(description = "Operations to collect broker statistics")
public class CmdBrokerStats extends CmdBase {
    private static final String DEFAULT_INDENTATION = "    ";

    @Command(description = "dump metrics for Monitoring")
    private class CmdMonitoringMetrics extends CliCommand {
        @Option(names = {"-i", "--indent"}, description = "Indent JSON output", required = false)
        private boolean indent = false;

        @Override
        void run() throws Exception {
            String s = getAdmin().brokerStats().getMetrics();
            JsonArray metrics = new Gson().fromJson(s, JsonArray.class);

            try (Writer out = new OutputStreamWriter(System.out, StandardCharsets.UTF_8);
                 JsonWriter jsonWriter = new JsonWriter(out)) {
                for (int i = 0; i < metrics.size(); i++) {
                    JsonObject m = (JsonObject) metrics.get(i);
                    if (indent) {
                        jsonWriter.setIndent(DEFAULT_INDENTATION);
                        new Gson().toJson(m, jsonWriter);
                        out.write('\n');
                        out.write('\n');
                    } else {
                        new Gson().toJson(m, jsonWriter);
                    }
                    out.flush();
                }
            }
        }
    }

    @Command(description = "dump mbean stats")
    private class CmdDumpMBeans extends CliCommand {
        @Option(names = {"-i", "--indent"}, description = "Indent JSON output", required = false)
        private boolean indent = false;

        @Override
        void run() throws Exception {
            String s = getAdmin().brokerStats().getMBeans();
            JsonArray result = new Gson().fromJson(s, JsonArray.class);
            try (Writer out = new OutputStreamWriter(System.out, StandardCharsets.UTF_8);
                 JsonWriter jsonWriter = new JsonWriter(out)) {
                if (indent) {
                    jsonWriter.setIndent(DEFAULT_INDENTATION);
                }
                new Gson().toJson(result, jsonWriter);
                out.flush();
            }
        }

    }

    @Command(description = "dump broker load-report")
    private class CmdLoadReport extends CliCommand {

        @Override
        void run() throws Exception {
            print(getAdmin().brokerStats().getLoadReport());
        }
    }

    @Command(description = "dump topics stats")
    private class CmdTopics extends CliCommand {
        @Option(names = {"-i", "--indent"}, description = "Indent JSON output", required = false)
        private boolean indent = false;

        @Override
        void run() throws Exception {
            String s = getAdmin().brokerStats().getTopics();
            JsonObject result = new Gson().fromJson(s, JsonObject.class);
            try (Writer out = new OutputStreamWriter(System.out, StandardCharsets.UTF_8);
                 JsonWriter jsonWriter = new JsonWriter(out)) {
                if (indent) {
                    jsonWriter.setIndent(DEFAULT_INDENTATION);
                }
                new Gson().toJson(result, jsonWriter);
                out.flush();
            }
        }

    }

    @Command(description = "dump allocator stats")
    private class CmdAllocatorStats extends CliCommand {
        @Parameters(description = "allocator-name", arity = "1")
        private String allocatorName;

        @Override
        void run() throws Exception {
            AllocatorStats stats = getAdmin().brokerStats().getAllocatorStats(allocatorName);
            ObjectMapper mapper = ObjectMapperFactory.create();
            ObjectWriter writer = mapper.writerWithDefaultPrettyPrinter();
            try (Writer out = new OutputStreamWriter(System.out, StandardCharsets.UTF_8)) {
                out.write(writer.writeValueAsString(stats));
                out.flush();
            }
        }

    }

    public CmdBrokerStats(Supplier<PulsarAdmin> admin) {
        super("broker-stats", admin);
        addCommand("monitoring-metrics", new CmdMonitoringMetrics());
        addCommand("mbeans", new CmdDumpMBeans());
        addCommand("topics", new CmdTopics(), "destinations");
        addCommand("allocator-stats", new CmdAllocatorStats());
        addCommand("load-report", new CmdLoadReport());
    }

}
