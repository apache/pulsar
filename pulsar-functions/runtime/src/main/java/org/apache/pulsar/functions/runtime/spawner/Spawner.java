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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.fs.LimitsConfig;
import org.apache.pulsar.functions.proto.Function.FunctionConfig;
import org.apache.pulsar.functions.proto.InstanceCommunication.FunctionStatus;
import org.apache.pulsar.functions.runtime.container.FunctionContainerFactory;
import org.apache.pulsar.functions.runtime.container.InstanceConfig;
import org.apache.pulsar.functions.runtime.container.FunctionContainer;
import org.apache.pulsar.functions.runtime.metrics.MetricsSink;
import org.apache.pulsar.functions.utils.FunctionConfigUtils;

@Slf4j
public class Spawner implements AutoCloseable {

    public static Spawner createSpawner(FunctionConfig fnConfig,
                                        LimitsConfig limitsConfig,
                                        String codeFile,
                                        FunctionContainerFactory containerFactory,
                                        MetricsSink metricsSink, int metricsCollectionInterval) {
        AssignmentInfo assignmentInfo = new AssignmentInfo(
            fnConfig,
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        );
        return new Spawner(
            limitsConfig,
            assignmentInfo,
            codeFile,
            containerFactory,
            metricsSink,
            metricsCollectionInterval);
    }

    private final LimitsConfig limitsConfig;
    private final AssignmentInfo assignmentInfo;
    private final FunctionContainerFactory functionContainerFactory;
    private final String codeFile;

    private FunctionContainer functionContainer;
    private MetricsSink metricsSink;
    private int metricsCollectionInterval;
    private Timer metricsCollectionTimer;

    private Spawner(LimitsConfig limitsConfig,
                    AssignmentInfo assignmentInfo,
                    String codeFile,
                    FunctionContainerFactory containerFactory,
                    MetricsSink metricsSink,
                    int metricsCollectionInterval) {
        this.limitsConfig = limitsConfig;
        this.assignmentInfo = assignmentInfo;
        this.functionContainerFactory = containerFactory;
        this.codeFile = codeFile;
        this.metricsSink = metricsSink;
        this.metricsCollectionInterval = metricsCollectionInterval;
    }

    public void start() throws Exception {
        log.info("Spawner starting function {}", this.assignmentInfo.getFunctionConfig().getName());
        functionContainer = functionContainerFactory.createContainer(createJavaInstanceConfig(), codeFile);
        functionContainer.start();
        if (metricsSink != null) {
            log.info("Scheduling Metrics Collection every " + metricsCollectionInterval + " secs for " + FunctionConfigUtils.getFullyQualifiedName(assignmentInfo.getFunctionConfig()));
            metricsCollectionTimer = new Timer();
            metricsCollectionTimer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    log.info("Collecting metrics for function" + FunctionConfigUtils.getFullyQualifiedName(assignmentInfo.getFunctionConfig()));
                    functionContainer.getAndResetMetrics().thenAccept(t -> {
                        if (t != null) {
                            log.debug("Collected metrics {}", t);
                            metricsSink.processRecord(t, assignmentInfo.getFunctionConfig());
                        }
                    });
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
        return functionContainer.getFunctionStatus();
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

    private InstanceConfig createJavaInstanceConfig() {
        InstanceConfig instanceConfig = new InstanceConfig();
        instanceConfig.setFunctionConfig(assignmentInfo.getFunctionConfig());
        instanceConfig.setFunctionId(assignmentInfo.getFunctionId());
        instanceConfig.setFunctionVersion(assignmentInfo.getFunctionVersion());
        instanceConfig.setInstanceId(assignmentInfo.getInstanceId());
        instanceConfig.setLimitsConfig(limitsConfig);
        return instanceConfig;
    }
}
