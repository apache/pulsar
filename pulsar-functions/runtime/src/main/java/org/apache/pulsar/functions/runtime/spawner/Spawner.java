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
/**
 * Spawner is the module responsible for running one particular instance servicing one
 * function. It is responsible for starting/stopping the instance and passing data to the
 * instance and getting the results back.
 */
package org.apache.pulsar.functions.runtime.spawner;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.proto.InstanceCommunication.FunctionStatus;
import org.apache.pulsar.functions.runtime.container.FunctionContainerFactory;
import org.apache.pulsar.functions.runtime.container.FunctionContainer;
import org.apache.pulsar.functions.metrics.MetricsSink;
import org.apache.pulsar.functions.utils.FunctionConfigUtils;

@Slf4j
public class Spawner implements AutoCloseable {

    private final InstanceConfig instanceConfig;
    private final FunctionContainerFactory functionContainerFactory;
    private final String codeFile;

    private FunctionContainer functionContainer;
    private MetricsSink metricsSink;
    private int metricsCollectionInterval;
    private Timer metricsCollectionTimer;
    private int numRestarts;

    public Spawner(InstanceConfig instanceConfig,
                    String codeFile,
                    FunctionContainerFactory containerFactory,
                    MetricsSink metricsSink,
                    int metricsCollectionInterval) {
        this.instanceConfig = instanceConfig;
        this.functionContainerFactory = containerFactory;
        this.codeFile = codeFile;
        this.metricsSink = metricsSink;
        this.metricsCollectionInterval = metricsCollectionInterval;
        this.numRestarts = 0;
    }

    public void start() throws Exception {
        log.info("Spawner starting function {} - {}", this.instanceConfig.getFunctionConfig().getName(),
                this.instanceConfig.getInstanceId());
        functionContainer = functionContainerFactory.createContainer(this.instanceConfig, codeFile);
        functionContainer.start();
        if (metricsSink != null) {
            log.info("Scheduling Metrics Collection every {} secs for {} - {}",
                    metricsCollectionInterval,
                    FunctionConfigUtils.getFullyQualifiedName(instanceConfig.getFunctionConfig()),
                    instanceConfig.getInstanceId());
            metricsCollectionTimer = new Timer();
            metricsCollectionTimer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    if (functionContainer.isAlive()) {

                        log.info("Collecting metrics for function {} - {}",
                                FunctionConfigUtils.getFullyQualifiedName(instanceConfig.getFunctionConfig()),
                                instanceConfig.getInstanceId());
                        functionContainer.getAndResetMetrics().thenAccept(t -> {
                            if (t != null) {
                                log.debug("Collected metrics {}", t);
                                metricsSink.processRecord(t, instanceConfig.getFunctionConfig());
                            }
                        });
                    } else {
                        log.error("Function Container is dead with exception", functionContainer.getDeathException());
                        log.error("Restarting...");
                        functionContainer.start();
                        numRestarts++;
                    }
                }
            }, metricsCollectionInterval * 1000, metricsCollectionInterval * 1000);
        }
    }

    public void join() throws Exception {
        if (null != functionContainer) {
            functionContainer.join();
        }
    }

    public CompletableFuture<FunctionStatus> getFunctionStatus() {
        return functionContainer.getFunctionStatus().thenApply(f -> {
           FunctionStatus.Builder builder = FunctionStatus.newBuilder();
           builder.mergeFrom(f).setNumRestarts(numRestarts).setInstanceId(instanceConfig.getInstanceId());
           if (functionContainer.getDeathException() != null) {
               builder.setFailureException(functionContainer.getDeathException().getMessage());
           }
           return builder.build();
        });
    }

    @Override
    public void close() {
        if (null != functionContainer) {
            functionContainer.stop();
            functionContainer = null;
        }
        if (metricsCollectionTimer != null) {
            metricsCollectionTimer.cancel();
            metricsCollectionTimer = null;
        }
    }
}
