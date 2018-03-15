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
 * RuntimeSpawner is the module responsible for running one particular instance servicing one
 * function. It is responsible for starting/stopping the instance and passing data to the
 * instance and getting the results back.
 */
package org.apache.pulsar.functions.runtime;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.proto.InstanceCommunication.FunctionStatus;

@Slf4j
public class RuntimeSpawner implements AutoCloseable {

    private final InstanceConfig instanceConfig;
    private final RuntimeFactory runtimeFactory;
    private final String codeFile;

    @Getter
    private Runtime runtime;
    private Timer processLivenessCheckTimer;
    private int numRestarts;
    private Long instanceLivenessCheckFreqMs;


    public RuntimeSpawner(InstanceConfig instanceConfig,
                          String codeFile,
                          RuntimeFactory containerFactory, Long instanceLivenessCheckFreqMs) {
        this.instanceConfig = instanceConfig;
        this.runtimeFactory = containerFactory;
        this.codeFile = codeFile;
        this.numRestarts = 0;
        this.instanceLivenessCheckFreqMs = instanceLivenessCheckFreqMs;
    }

    public void start() throws Exception {
        log.info("RuntimeSpawner starting function {} - {}", this.instanceConfig.getFunctionConfig().getName(),
                this.instanceConfig.getInstanceId());
        runtime = runtimeFactory.createContainer(this.instanceConfig, codeFile);
        runtime.start();

        // monitor function runtime to make sure it is running.  If not, restart the function runtime
        if (instanceLivenessCheckFreqMs != null) {
            processLivenessCheckTimer = new Timer();
            processLivenessCheckTimer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    if (!runtime.isAlive()) {
                        log.error("Function Container is dead with exception", runtime.getDeathException());
                        log.error("Restarting...");
                        runtime.start();
                        numRestarts++;
                    }
                }
            }, instanceLivenessCheckFreqMs, instanceLivenessCheckFreqMs);
        }
    }

    public void join() throws Exception {
        if (null != runtime) {
            runtime.join();
        }
    }

    public CompletableFuture<FunctionStatus> getFunctionStatus() {
        return runtime.getFunctionStatus().thenApply(f -> {
           FunctionStatus.Builder builder = FunctionStatus.newBuilder();
           builder.mergeFrom(f).setNumRestarts(numRestarts).setInstanceId(instanceConfig.getInstanceId());
           if (runtime.getDeathException() != null) {
               builder.setFailureException(runtime.getDeathException().getMessage());
           }
           return builder.build();
        });
    }

    @Override
    public void close() {
        if (null != runtime) {
            runtime.stop();
            runtime = null;
        }
        if (processLivenessCheckTimer != null) {
            processLivenessCheckTimer.cancel();
            processLivenessCheckTimer = null;
        }
    }
}
