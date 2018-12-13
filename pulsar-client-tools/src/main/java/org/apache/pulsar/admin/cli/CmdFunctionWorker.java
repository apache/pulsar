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
import org.apache.pulsar.client.api.PulsarClientException;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.functions.WorkerInfo;

import java.util.Collection;
import java.util.List;
import java.util.Map;

@Slf4j
@Parameters(commandDescription = "Operations to collect function-worker statistics")
public class CmdFunctionWorker extends CmdBase {

    /**
     * Base command
     */
    @Getter
    abstract class BaseCommand extends CliCommand {
        @Override
        void run() throws Exception {
            processArguments();
            runCmd();
        }

        void processArguments() throws Exception {
        }

        abstract void runCmd() throws Exception;
    }

//    @Parameters(commandDescription = "dump all functions stats")
//    class FunctionsStats extends BaseCommand {
//
//        @Parameter(names = { "-i", "--indent" }, description = "Indent JSON output", required = false)
//        boolean indent = false;
//
//        @Override
//        void runCmd() throws Exception {
//            String json = JsonFormat.printer().print(admin.worker().getFunctionsStats());
//            GsonBuilder gsonBuilder = new GsonBuilder();
//            if (indent) {
//                gsonBuilder.setPrettyPrinting();
//            }
//            System.out.println(gsonBuilder.create().toJson(new JsonParser().parse(json)));
//        }
//    }

    @Parameters(commandDescription = "dump metrics for Monitoring")
    class CmdMonitoringMetrics extends BaseCommand {

        @Parameter(names = { "-i", "--indent" }, description = "Indent JSON output", required = false)
        boolean indent = false;

        @Override
        void runCmd() throws Exception {
            String json = new Gson().toJson(admin.worker().getMetrics());
            GsonBuilder gsonBuilder = new GsonBuilder();
            if (indent) {
                gsonBuilder.setPrettyPrinting();
            }
            System.out.println(gsonBuilder.create().toJson(new JsonParser().parse(json)));
        }
    }

    @Parameters(commandDescription = "Get all workers belonging to this cluster")
    class GetCluster extends BaseCommand {

        @Parameter(names = { "-i", "--indent" }, description = "Indent JSON output", required = false)
        boolean indent = false;

        @Override
        void runCmd() throws Exception {
            List<WorkerInfo> workers = admin.worker().getCluster();
            GsonBuilder gsonBuilder = new GsonBuilder();
            if (indent) {
                gsonBuilder.setPrettyPrinting();
            }
            System.out.println(gsonBuilder.create().toJson(workers));
        }
    }

    @Parameters(commandDescription = "Get the leader of the worker cluster")
    class GetClusterLeader extends BaseCommand {

        @Parameter(names = { "-i", "--indent" }, description = "Indent JSON output", required = false)
        boolean indent = false;

        @Override
        void runCmd() throws Exception {
            WorkerInfo leader = admin.worker().getClusterLeader();
            GsonBuilder gsonBuilder = new GsonBuilder();
            if (indent) {
                gsonBuilder.setPrettyPrinting();
            }
            System.out.println(gsonBuilder.create().toJson(leader));
        }
    }

    @Parameters(commandDescription = "Get the assignments of the functions accross the worker cluster")
    class GetFunctionAssignments extends BaseCommand {

        @Parameter(names = { "-i", "--indent" }, description = "Indent JSON output", required = false)
        boolean indent = false;

        @Override
        void runCmd() throws Exception {
            Map<String, Collection<String>> assignments = admin.worker().getAssignments();
            GsonBuilder gsonBuilder = new GsonBuilder();
            if (indent) {
                gsonBuilder.setPrettyPrinting();
            }
            System.out.println(gsonBuilder.create().toJson(assignments));
        }
    }

    public CmdFunctionWorker(PulsarAdmin admin) throws PulsarClientException {
        super("functions-worker", admin);
//        jcommander.addCommand("function-stats", new FunctionsStats());
        jcommander.addCommand("monitoring-metrics", new CmdMonitoringMetrics());
        jcommander.addCommand("get-cluster", new GetCluster());
        jcommander.addCommand("get-cluster-leader", new GetClusterLeader());
        jcommander.addCommand("get-function-assignments", new GetFunctionAssignments());
    }

}
