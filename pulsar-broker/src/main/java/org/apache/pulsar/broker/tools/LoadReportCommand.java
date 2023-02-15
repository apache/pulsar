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
package org.apache.pulsar.broker.tools;

import com.beust.jcommander.Parameter;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.tools.framework.Cli;
import org.apache.bookkeeper.tools.framework.CliCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.commons.lang3.SystemUtils;
import org.apache.pulsar.broker.loadbalance.BrokerHostUsage;
import org.apache.pulsar.broker.loadbalance.impl.GenericBrokerHostUsageImpl;
import org.apache.pulsar.broker.loadbalance.impl.LinuxBrokerHostUsageImpl;
import org.apache.pulsar.broker.tools.LoadReportCommand.Flags;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.apache.pulsar.policies.data.loadbalancer.SystemResourceUsage;

/**
 * The command to collect the load report of a specific broker.
 */
public class LoadReportCommand extends CliCommand<CliFlags, Flags> {

    private static final String NAME = "load-report";
    private static final String DESC = "Collect the load report of a specific broker";

    /**
     * The CLI flags of load report command.
     */
    public static class Flags extends CliFlags {

        @Parameter(
            names = {
                "-i", "--interval-ms"
            },
            description = "Interval to collect load report, in milliseconds"
        )
        public int intervalMilliseconds = 100;

    }

    public LoadReportCommand() {
        super(CliSpec.<Flags>newBuilder()
            .withName(NAME)
            .withDescription(DESC)
            .withFlags(new Flags())
            .build());
    }

    @Override
    public Boolean apply(CliFlags globalFlags, String[] args) {
        CliSpec<Flags> newSpec = CliSpec.newBuilder(spec)
            .withRunFunc(cmdFlags -> apply(cmdFlags))
            .build();
        return 0 == Cli.runCli(newSpec, args);
    }

    private boolean apply(Flags flags) {

        boolean isLinux = SystemUtils.IS_OS_LINUX;
        spec.console().println("OS ARCH: " + SystemUtils.OS_ARCH);
        spec.console().println("OS NAME: " + SystemUtils.OS_NAME);
        spec.console().println("OS VERSION: " + SystemUtils.OS_VERSION);
        spec.console().println("Linux: " + isLinux);
        spec.console().println("--------------------------------------");
        spec.console().println();
        spec.console().println("Load Report Interval : " + flags.intervalMilliseconds + " ms");
        spec.console().println();
        spec.console().println("--------------------------------------");
        spec.console().println();

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
                new ExecutorProvider.ExtendedThreadFactory("load-report"));
        BrokerHostUsage hostUsage;
        try {
            if (isLinux) {
                hostUsage = new LinuxBrokerHostUsageImpl(
                    Integer.MAX_VALUE, Optional.empty(), scheduler
                );
            } else {
                hostUsage = new GenericBrokerHostUsageImpl(
                    Integer.MAX_VALUE, scheduler
                );
            }

            hostUsage.calculateBrokerHostUsage();
            try {
                TimeUnit.MILLISECONDS.sleep(flags.intervalMilliseconds);
            } catch (InterruptedException e) {
            }
            hostUsage.calculateBrokerHostUsage();
            SystemResourceUsage usage = hostUsage.getBrokerHostUsage();

            printResourceUsage("CPU", usage.cpu);
            printResourceUsage("Memory", usage.memory);
            printResourceUsage("Direct Memory", usage.directMemory);
            printResourceUsage("Bandwidth In", usage.bandwidthIn);
            printResourceUsage("Bandwidth Out", usage.bandwidthOut);

            return true;
        } finally {
            scheduler.shutdown();
        }
    }

    private void printResourceUsage(String name, ResourceUsage usage) {
        spec.console().println(name + " : usage = " + usage.usage + ", limit = " + usage.limit);
    }
}
