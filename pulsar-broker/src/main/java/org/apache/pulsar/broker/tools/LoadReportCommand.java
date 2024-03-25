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
package org.apache.pulsar.broker.tools;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.SystemUtils;
import org.apache.pulsar.broker.loadbalance.BrokerHostUsage;
import org.apache.pulsar.broker.loadbalance.impl.GenericBrokerHostUsageImpl;
import org.apache.pulsar.broker.loadbalance.impl.LinuxBrokerHostUsageImpl;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.apache.pulsar.policies.data.loadbalancer.SystemResourceUsage;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

/**
 * The command to collect the load report of a specific broker.
 */
@Command(name = "load-report", description = "Collect the load report of a specific broker")
public class LoadReportCommand implements Callable<Integer> {

    @Option(names = {"-i", "--interval-ms"}, description = "Interval to collect load report, in milliseconds")
    public int intervalMilliseconds = 100;

    @Spec
    CommandSpec spec;

    @Override
    public Integer call() throws Exception {
        boolean isLinux = SystemUtils.IS_OS_LINUX;
        spec.commandLine().getOut().println("OS ARCH: " + SystemUtils.OS_ARCH);
        spec.commandLine().getOut().println("OS NAME: " + SystemUtils.OS_NAME);
        spec.commandLine().getOut().println("OS VERSION: " + SystemUtils.OS_VERSION);
        spec.commandLine().getOut().println("Linux: " + isLinux);
        spec.commandLine().getOut().println("--------------------------------------");
        spec.commandLine().getOut().println();
        spec.commandLine().getOut().println("Load Report Interval : " + intervalMilliseconds + " ms");
        spec.commandLine().getOut().println();
        spec.commandLine().getOut().println("--------------------------------------");
        spec.commandLine().getOut().println();

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
                TimeUnit.MILLISECONDS.sleep(intervalMilliseconds);
            } catch (InterruptedException e) {
            }
            hostUsage.calculateBrokerHostUsage();
            SystemResourceUsage usage = hostUsage.getBrokerHostUsage();

            printResourceUsage("CPU", usage.cpu);
            printResourceUsage("Memory", usage.memory);
            printResourceUsage("Direct Memory", usage.directMemory);
            printResourceUsage("Bandwidth In", usage.bandwidthIn);
            printResourceUsage("Bandwidth Out", usage.bandwidthOut);

            return 0;
        } finally {
            scheduler.shutdown();
        }
    }

    private void printResourceUsage(String name, ResourceUsage usage) {
        spec.commandLine().getOut().println(name + " : usage = " + usage.usage + ", limit = " + usage.limit);
    }
}
