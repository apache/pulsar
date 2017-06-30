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

import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.stats.AllocatorStats;
import org.apache.pulsar.common.util.ObjectMapperFactory;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonWriter;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

@Parameters(commandDescription = "Operations to collect broker statistics")
public class CmdBrokerStats extends CmdBase {
    public static final String DEFAULT_INDENTATION = "    ";
    public static final Charset UTF_8 = Charset.forName("UTF-8");
    public static Writer out = new OutputStreamWriter(System.out, UTF_8);

    @Parameters(commandDescription = "dump metrics for Monitoring")
    class CmdMonitoringMetrics extends CliCommand {
        @Parameter(names = { "-i", "--indent" }, description = "Indent JSON output", required = false)
        boolean indent = false;

        @Override
        void run() throws Exception {
            JsonArray metrics = admin.brokerStats().getMetrics();

            for (int i = 0; i < metrics.size(); i++) {
                JsonObject m = (JsonObject) metrics.get(i);
                JsonWriter jsonWriter = new JsonWriter(out);
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

    @Parameters(commandDescription = "dump mbean stats")
    public class CmdDumpMBeans extends CliCommand {
        @Parameter(names = { "-i", "--indent" }, description = "Indent JSON output", required = false)
        boolean indent = false;

        @Override
        void run() throws Exception {
            JsonArray result = admin.brokerStats().getMBeans();
            JsonWriter jsonWriter = new JsonWriter(out);
            if (indent) {
                jsonWriter.setIndent(DEFAULT_INDENTATION);
            }
            new Gson().toJson(result, jsonWriter);
            out.flush();
        }

    }

    @Parameters(commandDescription = "dump destination stats")
    public class CmdDestinations extends CliCommand {
        @Parameter(names = { "-i", "--indent" }, description = "Indent JSON output", required = false)
        boolean indent = false;

        @Override
        void run() throws Exception {
            JsonObject result = admin.brokerStats().getDestinations();
            JsonWriter jsonWriter = new JsonWriter(out);
            if (indent) {
                jsonWriter.setIndent(DEFAULT_INDENTATION);
            }
            new Gson().toJson(result, jsonWriter);
            out.flush();
        }

    }

    @Parameters(commandDescription = "dump allocator stats")
    public class CmdAllocatorStats extends CliCommand {
        @Parameter(description = "allocator-name\n", required = true)
        List<String> params;

        @Override
        void run() throws Exception {
            AllocatorStats stats = admin.brokerStats().getAllocatorStats(params.get(0));
            ObjectMapper mapper = ObjectMapperFactory.create();
            ObjectWriter writer = mapper.writerWithDefaultPrettyPrinter();
            out.write(writer.writeValueAsString(stats));
            out.flush();
        }

    }

    public CmdBrokerStats(PulsarAdmin admin) {
        super("broker-stats", admin);
        jcommander.addCommand("monitoring-metrics", new CmdMonitoringMetrics());
        jcommander.addCommand("mbeans", new CmdDumpMBeans());
        jcommander.addCommand("destinations", new CmdDestinations());
        jcommander.addCommand("allocator-stats", new CmdAllocatorStats());
    }

    public void addCommands(String commandName, CliCommand cliCommand) {
        jcommander.addCommand(commandName, cliCommand);
    }

}
