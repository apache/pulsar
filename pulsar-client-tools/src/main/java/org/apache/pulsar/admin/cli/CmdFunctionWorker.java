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

import com.beust.jcommander.Parameters;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClientException;

@Slf4j
@Parameters(commandDescription = "Operations to collect function-worker statistics")
public class CmdFunctionWorker extends CmdBase {

    /**
     * Base command.
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

    @Parameters(commandDescription = "Dump all functions stats running on this broker")
    class FunctionsStats extends BaseCommand {

        @Override
        void runCmd() throws Exception {
            print(getAdmin().worker().getFunctionsStats());
        }
    }

    @Parameters(commandDescription = "Dump metrics for Monitoring")
    class CmdMonitoringMetrics extends BaseCommand {

        @Override
        void runCmd() throws Exception {
            print(getAdmin().worker().getMetrics());
        }
    }

    @Parameters(commandDescription = "Get all workers belonging to this cluster")
    class GetCluster extends BaseCommand {

        @Override
        void runCmd() throws Exception {
            print(getAdmin().worker().getCluster());
        }
    }

    @Parameters(commandDescription = "Get the leader of the worker cluster")
    class GetClusterLeader extends BaseCommand {

        @Override
        void runCmd() throws Exception {
            print(getAdmin().worker().getClusterLeader());
        }
    }

    @Parameters(commandDescription = "Get the assignments of the functions across the worker cluster")
    class GetFunctionAssignments extends BaseCommand {


        @Override
        void runCmd() throws Exception {
            print(getAdmin().worker().getAssignments());
        }
    }

    @Parameters(commandDescription = "Triggers a rebalance of functions to workers")
    class Rebalance extends BaseCommand {

        @Override
        void runCmd() throws Exception {
            getAdmin().worker().rebalance();
            print("Rebalance command sent successfully");
        }
    }

    public CmdFunctionWorker(Supplier<PulsarAdmin> admin) throws PulsarClientException {
        super("functions-worker", admin);
        jcommander.addCommand("function-stats", new FunctionsStats());
        jcommander.addCommand("monitoring-metrics", new CmdMonitoringMetrics());
        jcommander.addCommand("get-cluster", new GetCluster());
        jcommander.addCommand("get-cluster-leader", new GetClusterLeader());
        jcommander.addCommand("get-function-assignments", new GetFunctionAssignments());
        jcommander.addCommand("rebalance", new Rebalance());
    }

}
